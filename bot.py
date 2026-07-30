import os
import re
import json
import asyncio
import tempfile
import struct
import base64
from pathlib import Path
from datetime import datetime

import aiohttp
from telethon import TelegramClient, events, Button
from telethon.tl.functions.messages import StartBotRequest
from telethon.sessions import StringSession, SQLiteSession
from telethon.crypto import AuthKey
from telethon.errors import (
    AuthKeyUnregisteredError,
    UserDeactivatedError,
    FloodWaitError,
    SessionPasswordNeededError,
)

# ====================== ENV ======================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
LOLZ_TOKEN = os.environ["LOLZ_TOKEN"]

MAMBA_BOT = os.environ.get("MAMBA_BOT", "mambarubot")

def _normalize_chat_id(raw: str) -> int:
    """
    4331188948     → -1004331188948
    -1004331188948 → как есть
    """
    n = int(str(raw).strip())
    if n > 0:
        return int(f"-100{n}")
    return n


LOG_SUPERGROUP_ID = _normalize_chat_id(os.environ["LOG_SUPERGROUP_ID"])
TOPIC_LOGS = int(os.environ["TOPIC_LOGS"])
TOPIC_VALID = int(os.environ["TOPIC_VALID"])
TOPIC_NOVALID = int(os.environ["TOPIC_NOVALID"])
TOPIC_DEAD = int(os.environ["TOPIC_DEAD"])
TOPIC_ERROR = int(os.environ["TOPIC_ERROR"])

MAX_PRICE = float(os.environ.get("MAX_PRICE", "7"))
MAX_TRIES = int(os.environ.get("MAX_TRIES", "40"))
RETRY_MAX = int(os.environ.get("RETRY_MAX", "30"))
CURRENCY = os.environ.get("CURRENCY", "rub")
AUTO_CLAIM_ON_DEAD = os.environ.get("AUTO_CLAIM_ON_DEAD", "1") == "1"
# После сплита балансов на маркете — ID кошелька для покупок (0 = авто)
LOLZ_BALANCE_ID = int(os.environ.get("LOLZ_BALANCE_ID", "0") or "0")

LOLZ_BASE = os.environ.get("LOLZ_BASE", "https://prod-api.lzt.market")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "300"))

_cached_balance_id: int | None = None

DC_IP_MAP = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}
DC_PORT = 443

TEMP_DIR = Path(tempfile.gettempdir()) / "mamba_lolz_sessions"
TEMP_DIR.mkdir(parents=True, exist_ok=True)

CLAIM_TEXT_DEAD = (
    "Аккаунт невалидный: не удаётся войти через Telethon по выданным "
    "Auth Key + DC ID. Сессия мёртвая / не авторизована (DEAD). "
    "Прошу замену аккаунта или возврат средств."
)


# ====================== LOLZ API ======================

class LolzAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    async def _request(self, method: str, path: str, *, params=None, json_body=None, form=None, retry=True):
        url = f"{LOLZ_BASE.rstrip('/')}/{path.lstrip('/')}"
        attempts = RETRY_MAX if retry else 1
        last_err = None

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for attempt in range(1, attempts + 1):
                try:
                    kwargs = {
                        "headers": self.headers,
                        "params": params,
                        "ssl": False,
                    }
                    if form is not None:
                        kwargs["data"] = form
                    elif json_body is not None:
                        kwargs["json"] = json_body

                    async with session.request(method, url, **kwargs) as resp:
                        text = await resp.text()
                        try:
                            data = json.loads(text) if text else {}
                        except json.JSONDecodeError:
                            data = {"raw": text}

                        err_msg = str(
                            data.get("error")
                            or data.get("errors")
                            or data.get("message")
                            or ""
                        )
                        if (
                            resp.status == 429
                            or "retry_request" in err_msg.lower()
                            or resp.status in (502, 503, 504)
                        ):
                            wait = min(2 + attempt * 0.5, 8)
                            print(
                                f"[LOLZ] retry {attempt}/{attempts} "
                                f"{method} {path} status={resp.status} wait={wait}s"
                            )
                            await asyncio.sleep(wait)
                            last_err = data
                            continue

                        if resp.status >= 400:
                            return {
                                "_http": resp.status,
                                "_error": True,
                                **(data if isinstance(data, dict) else {"data": data}),
                            }
                        return data
                except asyncio.TimeoutError:
                    print(f"[LOLZ] timeout {attempt}/{attempts} {method} {path}")
                    last_err = {"error": "timeout"}
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"[LOLZ] exception {attempt}/{attempts} {method} {path}: {e}")
                    last_err = {"error": str(e)}
                    await asyncio.sleep(2)

        return {"_error": True, "error": "retry_exhausted", "last": last_err}

    async def search_telegram(self, page: int = 1):
        params = {
            "page": page,
            "pmax": MAX_PRICE,
            "password": "no",
            "order_by": "pdate_to_down_upload",
            "currency": CURRENCY,
        }
        return await self._request("GET", "/telegram", params=params)

    async def check_account(self, item_id: int):
        return await self._request("POST", f"/{item_id}/check-account")

    async def fast_buy(
        self,
        item_id: int,
        price: float | None = None,
        balance_id: int | None = None,
    ):
        """
        POST /{item_id}/fast-buy
        После разделения балансов на маркете ОБЯЗАТЕЛЕН balance_id,
        иначе API отвечает «недостаточно средств» при живых деньгах.
        """
        body: dict = {}
        if price is not None:
            body["price"] = float(price)
        if balance_id is not None:
            body["balance_id"] = int(balance_id)

        # form-urlencoded как веб
        form = {k: str(v) for k, v in body.items()} if body else None
        result = await self._request("POST", f"/{item_id}/fast-buy", form=form)
        if not result.get("_error"):
            return result

        # fallback json
        if body:
            result2 = await self._request(
                "POST", f"/{item_id}/fast-buy", json_body=body
            )
            if not result2.get("_error"):
                return result2
            result = result2

        # ещё попытка: только balance_id без price
        if balance_id is not None and price is not None:
            only_bal = {"balance_id": int(balance_id)}
            result3 = await self._request(
                "POST",
                f"/{item_id}/fast-buy",
                form={k: str(v) for k, v in only_bal.items()},
            )
            if not result3.get("_error"):
                return result3
            result3 = await self._request(
                "POST", f"/{item_id}/fast-buy", json_body=only_bal
            )
            if not result3.get("_error"):
                return result3

        return result

    async def get_item(self, item_id: int):
        return await self._request("GET", f"/{item_id}")

    async def get_profile(self):
        return await self._request("GET", "/me")

    async def get_balances(self):
        """Список балансов (после сплита кошельков)."""
        # в доке: GET /balance/exchange — Returns list of balances
        r = await self._request("GET", "/balance/exchange")
        if not r.get("_error"):
            return r
        # запасные пути
        for path in ("/balances", "/balance", "/user/balances"):
            r2 = await self._request("GET", path)
            if not r2.get("_error"):
                return r2
        return r

    async def create_claim(self, item_id: int, description: str):
        """
        POST /claims — претензия (замена / возврат).
        Док: body item_id + описание ситуации.
        """
        # API принимает form или json — пробуем form как у веб-запросов
        form = {
            "item_id": str(item_id),
            "claim_body": description,
            "body": description,
        }
        result = await self._request("POST", "/claims", form=form)
        if result.get("_error"):
            # fallback json
            result = await self._request(
                "POST",
                "/claims",
                json_body={"item_id": item_id, "claim_body": description, "body": description},
            )
        return result


lolz = LolzAPI(LOLZ_TOKEN)


# ====================== SESSION FROM AUTH KEY ======================

def authkey_to_string_session(auth_key_hex: str, dc_id: int) -> str:
    auth_key_hex = auth_key_hex.strip().replace(" ", "").replace("\n", "")
    if auth_key_hex.lower().startswith("0x"):
        auth_key_hex = auth_key_hex[2:]
    key = bytes.fromhex(auth_key_hex)
    if len(key) != 256:
        raise ValueError(f"auth_key must be 256 bytes, got {len(key)}")

    ip = DC_IP_MAP.get(int(dc_id))
    if not ip:
        raise ValueError(f"unknown dc_id: {dc_id}")

    ip_bytes = ip.encode("ascii")
    pack = struct.pack(
        f">B{len(ip_bytes)}sH256s",
        int(dc_id),
        ip_bytes,
        DC_PORT,
        key,
    )
    return "1" + base64.urlsafe_b64encode(pack).decode("ascii")


def extract_session_data(item: dict) -> dict:
    if not item:
        return {}

    login = item.get("loginData") or item.get("login_data") or item.get("telegramData") or {}
    if isinstance(login, str):
        try:
            login = json.loads(login)
        except Exception:
            login = {"raw": login}

    def dig(*keys, default=None):
        for src in (item, login):
            if not isinstance(src, dict):
                continue
            for k in keys:
                if k in src and src[k] not in (None, "", []):
                    return src[k]
        return default

    phone = dig("telegram_phone", "phone", "login", "accountPhone", "tel", default="")
    phone = re.sub(r"[^\d+]", "", str(phone)) if phone else ""

    user_id = dig("telegram_id", "user_id", "telegramId", "userId")
    dc_id = dig("dc_id", "dcId", "telegram_dc_id", "data_center")
    auth_key = dig(
        "auth_key", "authKey", "auth_key_hex", "authKeyHex",
        "telegram_auth_key", "session", "session_key",
    )

    if isinstance(auth_key, str) and ":" in auth_key and not dc_id:
        parts = auth_key.split(":")
        if len(parts) == 2:
            a, b = parts[0].strip(), parts[1].strip()
            if a.isdigit() and len(b) > 32:
                dc_id, auth_key = a, b
            elif b.isdigit() and len(a) > 32:
                auth_key, dc_id = a, b

    return {
        "phone": str(phone or ""),
        "user_id": str(user_id or ""),
        "dc_id": int(dc_id) if dc_id not in (None, "") else None,
        "auth_key": str(auth_key).strip() if auth_key else None,
        "raw_item": item,
    }


def write_sqlite_session(path: Path, auth_key_hex: str, dc_id: int) -> Path:
    path = Path(path)
    if path.suffix != ".session":
        path = path.with_suffix(".session")

    key = bytes.fromhex(auth_key_hex.strip().replace(" ", ""))
    ip = DC_IP_MAP[int(dc_id)]

    session = SQLiteSession(str(path.with_suffix("")))
    session.set_dc(int(dc_id), ip, DC_PORT)
    session.auth_key = AuthKey(data=key)
    session.save()
    return path


def cleanup_session_file(session_path: Path | None):
    if not session_path:
        return
    try:
        Path(session_path).unlink(missing_ok=True)
        Path(str(session_path) + "-journal").unlink(missing_ok=True)
    except Exception:
        pass


# ====================== LOGGING / ARCHIVE ======================

def now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


_log_entity = None


async def resolve_log_chat(client: TelegramClient):
    """Резолвит супергруппу, чтобы Telethon знал entity."""
    global _log_entity
    if _log_entity is not None:
        return _log_entity
    try:
        _log_entity = await client.get_entity(LOG_SUPERGROUP_ID)
        print(
            f"[LOG CHAT] resolved id={getattr(_log_entity, 'id', LOG_SUPERGROUP_ID)} "
            f"title={getattr(_log_entity, 'title', '?')}"
        )
        return _log_entity
    except Exception as e:
        print(f"[LOG CHAT] get_entity({LOG_SUPERGROUP_ID}) failed: {e}")
        _log_entity = LOG_SUPERGROUP_ID
        return _log_entity


async def send_to_topic(client: TelegramClient, topic_id: int, text: str, file=None):
    entity = await resolve_log_chat(client)
    kwargs = {
        "entity": entity,
        "message": text,
        "link_preview": False,
        "reply_to": topic_id,
    }
    if file is not None:
        kwargs["file"] = file
    try:
        await client.send_message(**kwargs)
    except Exception as e:
        print(f"[TOPIC {topic_id}] send error: {e}")
        try:
            kwargs.pop("reply_to", None)
            await client.send_message(**kwargs)
        except Exception as e2:
            print(f"[TOPIC FALLBACK] {e2}")


async def log_event(
    bot_client: TelegramClient,
    status: str,
    phone: str,
    user_id: int,
    session_name: str = "",
    extra: str = "",
):
    text = (
        f"**{status}**\n"
        f"📞 `{phone or '—'}`\n"
        f"👤 User ID: `{user_id}`\n"
        f"📄 Session: `{session_name or '—'}`\n"
        f"🕒 `{now_str()}`\n"
        f"#{status}"
    )
    if extra:
        text += f"\nℹ️ {extra}"
    await send_to_topic(bot_client, TOPIC_LOGS, text)


async def archive_account(
    bot_client: TelegramClient,
    status: str,
    phone: str,
    auth_key: str | None,
    dc_id: int | None,
    tg_user_id: str,
    session_path: Path | None,
    buyer_id: int = 0,
    extra: str = "",
):
    topic_map = {
        "VALID": TOPIC_VALID,
        "NOVALID": TOPIC_NOVALID,
        "DEAD": TOPIC_DEAD,
        "ERROR": TOPIC_ERROR,
    }
    topic = topic_map.get(status, TOPIC_ERROR)

    text = (
        f"**ARCHIVE · {status}**\n\n"
        f"📞 Phone: `{phone or '—'}`\n"
        f"🆔 TG User ID: `{tg_user_id or '—'}`\n"
        f"🔑 Auth Key (HEX):\n`{auth_key or '—'}`\n"
        f"📡 DC ID: `{dc_id or '—'}`\n"
        f"👤 Buyer: `{buyer_id}`\n"
        f"🕒 `{now_str()}`\n"
        f"#{status}"
    )
    if extra:
        text += f"\nℹ️ {extra}"

    file_arg = None
    if session_path and Path(session_path).exists():
        file_arg = str(session_path)

    await send_to_topic(bot_client, topic, text, file=file_arg)


# ====================== MAMBA CHECK ======================

async def check_mamba_with_session(
    session_path: Path | None = None,
    string_session: str | None = None,
    start_param: str = "",
) -> str:
    client = None
    try:
        if string_session:
            client = TelegramClient(
                StringSession(string_session),
                API_ID,
                API_HASH,
                device_model="PC",
                system_version="Windows 10",
                app_version="4.0",
                lang_code="ru",
            )
        elif session_path:
            session_name = str(Path(session_path).with_suffix(""))
            client = TelegramClient(
                session_name,
                API_ID,
                API_HASH,
                device_model="PC",
                system_version="Windows 10",
                app_version="4.0",
                lang_code="ru",
            )
        else:
            return "ERROR"

        async with asyncio.timeout(20):
            await client.connect()
            if not await client.is_user_authorized():
                print("[-] session not authorized → DEAD")
                return "DEAD"

            await client(
                StartBotRequest(bot=MAMBA_BOT, peer=MAMBA_BOT, start_param=start_param)
            )
            await asyncio.sleep(2.5)
            messages = await client.get_messages(MAMBA_BOT, limit=15)

            for msg in messages:
                text = (msg.message or "").lower()
                if "поздравляем" in text and "анкета подтверждена" in text:
                    return "VALID"
                if (
                    "что-то пошло не так" in text
                    or "something is wrong" in text
                    or "не может использоваться для подтверждения" in text
                    or "был использован ранее" in text
                ):
                    return "NOVALID"

            return "NOVALID"

    except asyncio.TimeoutError:
        return "ERROR"
    except (AuthKeyUnregisteredError, UserDeactivatedError):
        return "DEAD"
    except SessionPasswordNeededError:
        return "DEAD"
    except FloodWaitError:
        return "ERROR"
    except Exception as e:
        print(f"[MAMBA CHECK] {type(e).__name__}: {e}")
        return "ERROR"
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass


def extract_errors(data: dict) -> str:
    if not data:
        return ""
    errs = data.get("errors") or data.get("error") or data.get("message") or ""
    if isinstance(errs, list):
        parts = []
        for e in errs:
            if isinstance(e, dict):
                parts.append(str(e.get("message") or e.get("error") or e))
            else:
                parts.append(str(e))
        text = " | ".join(parts)
    else:
        text = str(errs)
    # убрать html из ответа маркета
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def is_insufficient_funds(data: dict) -> bool:
    text = extract_errors(data).lower()
    keys = (
        "недостаточно средств",
        "not enough",
        "insufficient",
        "не хватает",
        "not enough balance",
    )
    return any(k in text for k in keys)


def _pick_balance_id(balances_payload: dict) -> int | None:
    """
    Выбирает balance_id для покупок в CURRENCY (по умолчанию rub).
    Структура ответа API может отличаться — перебираем типичные поля.
    """
    if not balances_payload or balances_payload.get("_error"):
        return None

    candidates = []
    for key in ("balances", "items", "list", "data", "exchange"):
        val = balances_payload.get(key)
        if isinstance(val, list):
            candidates = val
            break
        if isinstance(val, dict):
            # иногда dict id -> info
            candidates = list(val.values())
            break

    if not candidates and isinstance(balances_payload, list):
        candidates = balances_payload

    # если весь payload — один объект баланса
    if not candidates and any(k in balances_payload for k in ("balance_id", "id", "currency")):
        candidates = [balances_payload]

    currency_want = CURRENCY.lower()
    best = None
    best_amount = -1.0

    for b in candidates:
        if not isinstance(b, dict):
            continue
        bid = b.get("balance_id") or b.get("id") or b.get("balanceId")
        if bid is None:
            continue
        cur = str(
            b.get("currency") or b.get("currency_code") or b.get("code") or ""
        ).lower()
        amount = b.get("amount") or b.get("balance") or b.get("value") or b.get("money") or 0
        try:
            amount = float(amount)
        except Exception:
            amount = 0.0

        # предпочитаем нужную валюту с максимальным остатком
        if cur == currency_want or currency_want in cur or cur in currency_want:
            if amount > best_amount:
                best_amount = amount
                best = int(bid)
        elif best is None and amount > 0:
            # fallback: любой положительный
            best = int(bid)
            best_amount = amount

    return best


async def resolve_balance_id() -> int | None:
    """LOLZ_BALANCE_ID из env или авто из API."""
    global _cached_balance_id
    if LOLZ_BALANCE_ID > 0:
        return LOLZ_BALANCE_ID
    if _cached_balance_id is not None:
        return _cached_balance_id

    data = await lolz.get_balances()
    print(f"[BALANCE] raw keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
    raw = json.dumps(data, ensure_ascii=False, default=str)
    print(f"[BALANCE] snippet: {raw[:1000]}")

    bid = _pick_balance_id(data)
    if bid is None:
        # пробуем вытащить из /me
        me = await lolz.get_profile()
        print(f"[BALANCE /me] snippet: {json.dumps(me, ensure_ascii=False, default=str)[:1000]}")
        bid = _pick_balance_id(me)
        if bid is None and isinstance(me.get("user"), dict):
            bid = _pick_balance_id(me["user"])
        if bid is None:
            bid = _pick_balance_id({"balances": me.get("balances") or me.get("balance")})

    _cached_balance_id = bid
    print(f"[BALANCE] selected balance_id={bid}")
    return bid


def item_price_rub(item: dict) -> float | None:
    """Достаёт цену лота в рублях (или текущей валюте поиска)."""
    if not item:
        return None
    for key in (
        "rub_price",
        "price_rub",
        "price",
        "converted_price",
        "price_with_fee",
    ):
        val = item.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def is_check_ok(data: dict) -> bool:
    if not data or data.get("_error"):
        return False
    if is_insufficient_funds(data):
        return False
    if data.get("status") in ("ok", "success", "valid", True):
        return True
    if data.get("valid") is True:
        return True
    item = data.get("item") or data
    if isinstance(item, dict):
        st = str(item.get("status") or item.get("item_state") or "").lower()
        if st in ("active", "validated", "ok", "checked"):
            return True
    err = extract_errors(data)
    if err and "retry" not in err.lower():
        bad = ("sold", "deleted", "invalid", "blacklist", "limit")
        if any(b in err.lower() for b in bad):
            return False
    return not data.get("_error")


def is_buy_ok(data: dict) -> bool:
    if not data or data.get("_error"):
        return False
    if data.get("item") or data.get("loginData") or data.get("login_data"):
        return True
    if data.get("status") in ("ok", "success"):
        return True
    if extract_errors(data):
        return False
    return True


# ====================== BUY ONE ======================

async def buy_one_account(status_msg, buyer_id: int, seen_ids: set[int], page_state: dict):
    """
    Одна попытка: найти лот → check → buy → session data.
    Возвращает (sess_dict | None, status_msg, reason).
    reason: ok | no_items | check_fail | buy_fail | no_auth | search_error
    """
    page = page_state.get("page", 1)

    for _round in range(5):  # до 5 страниц за один вызов
        search = await lolz.search_telegram(page=page)
        if search.get("_error"):
            page_state["page"] = 1
            return None, status_msg, "search_error"

        items = search.get("items") or search.get("list") or []
        if not items:
            if page == 1:
                return None, status_msg, "no_items"
            page = 1
            page_state["page"] = 1
            await asyncio.sleep(1)
            continue

        for item in items:
            item_id = item.get("item_id") or item.get("id")
            if not item_id or int(item_id) in seen_ids:
                continue
            seen_ids.add(int(item_id))

            price = item_price_rub(item)
            if price is not None and price > MAX_PRICE:
                print(f"[SKIP] {item_id} price={price} > MAX_PRICE={MAX_PRICE}")
                continue

            phone_hint = item.get("telegram_phone") or item.get("title") or item_id
            print(
                f"[LOT] id={item_id} price={price} "
                f"keys_price={[k for k in item.keys() if 'price' in k.lower()]}"
            )

            status_msg = await update_status(
                status_msg,
                f"🔄 Проверяю лот `{item_id}` (цена {price or '?'} ₽)...",
            )

            check = await lolz.check_account(int(item_id))
            if is_insufficient_funds(check):
                err = extract_errors(check)
                print(f"[CHECK NO FUNDS] {item_id}: {err}")
                return None, status_msg, "no_funds"

            if not is_check_ok(check):
                err = extract_errors(check) or check.get("_http")
                print(f"[CHECK FAIL] {item_id}: {err}")
                continue

            # актуальная цена после check (может быть в ответе)
            check_item = check.get("item") if isinstance(check.get("item"), dict) else {}
            price = item_price_rub(check_item) or price

            status_msg = await update_status(
                status_msg,
                f"✅ Лот `{item_id}` валиден, покупаю за {price or '?'} ₽...",
            )

            balance_id = await resolve_balance_id()
            buy = await lolz.fast_buy(
                int(item_id),
                price=float(price) if price is not None else None,
                balance_id=balance_id,
            )
            if is_insufficient_funds(buy):
                err = extract_errors(buy)
                print(f"[BUY NO FUNDS] {item_id}: {err} balance_id={balance_id}")
                # сброс кэша и одна повторная попытка с перечитанным balance_id
                global _cached_balance_id
                _cached_balance_id = None
                balance_id = await resolve_balance_id()
                buy = await lolz.fast_buy(
                    int(item_id),
                    price=float(price) if price is not None else None,
                    balance_id=balance_id,
                )
                if is_insufficient_funds(buy):
                    print(f"[BUY NO FUNDS2] {item_id}: {extract_errors(buy)} balance_id={balance_id}")
                    return None, status_msg, "no_funds"

            if not is_buy_ok(buy):
                err = extract_errors(buy) or buy.get("_http")
                print(f"[BUY FAIL] {item_id}: {err} balance_id={balance_id}")
                fresh = await lolz.get_item(int(item_id))
                fresh_item = fresh.get("item") if isinstance(fresh.get("item"), dict) else fresh
                fresh_price = item_price_rub(fresh_item) if isinstance(fresh_item, dict) else None
                if fresh_price and fresh_price != price:
                    print(f"[BUY RETRY] {item_id} new_price={fresh_price}")
                    buy = await lolz.fast_buy(
                        int(item_id),
                        price=float(fresh_price),
                        balance_id=balance_id,
                    )
                    if is_insufficient_funds(buy):
                        return None, status_msg, "no_funds"
                    if not is_buy_ok(buy):
                        print(f"[BUY FAIL2] {item_id}: {extract_errors(buy) or buy.get('_http')}")
                        continue
                    price = fresh_price
                else:
                    continue

            bought_item = buy.get("item") or buy
            if not extract_session_data(bought_item).get("auth_key"):
                full = await lolz.get_item(int(item_id))
                if full.get("item"):
                    bought_item = full["item"]
                elif not full.get("_error"):
                    bought_item = {**bought_item, **full}

            sess = extract_session_data(bought_item)
            sess["item_id"] = int(item_id)
            sess["price"] = price

            if not sess.get("auth_key") or not sess.get("dc_id"):
                print(f"[NO AUTH KEY] item={item_id}")
                await log_event(
                    bot,
                    "ERROR",
                    sess.get("phone") or str(phone_hint),
                    buyer_id,
                    extra=f"нет auth_key/dc_id item={item_id}",
                )
                continue

            page_state["page"] = page
            return sess, status_msg, "ok"

        page += 1
        if page > 15:
            page = 1
        page_state["page"] = page
        await asyncio.sleep(1)

    return None, status_msg, "no_items"


# ====================== MAIN FLOW ======================

# pending confirmations: key = f"{user_id}:{msg_id}" -> Future[bool]
_pending_continue: dict[str, asyncio.Future] = {}
CONTINUE_TIMEOUT = int(os.environ.get("CONTINUE_TIMEOUT", "300"))  # сек


async def update_status(status_msg, text: str, buttons=None):
    try:
        await status_msg.edit(text, buttons=buttons)
        return status_msg
    except Exception:
        try:
            return await status_msg.respond(text, buttons=buttons)
        except Exception:
            return status_msg


async def ask_continue(
    event,
    status_msg,
    text: str,
    *,
    buyer_id: int,
) -> tuple[bool, object]:
    """
    Показать кнопки «Купить следующий» / «Стоп».
    True  — юзер нажал продолжить
    False — стоп / таймаут
    """
    buttons = [
        [
            Button.inline("✅ Купить следующий", data=b"cont:yes"),
            Button.inline("🛑 Стоп", data=b"cont:no"),
        ]
    ]
    status_msg = await update_status(status_msg, text, buttons=buttons)

    key = f"{buyer_id}:{status_msg.id}"
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    _pending_continue[key] = fut

    try:
        ok = await asyncio.wait_for(fut, timeout=CONTINUE_TIMEOUT)
        return bool(ok), status_msg
    except asyncio.TimeoutError:
        try:
            await status_msg.edit(
                text + f"\n\n⏱ Таймаут {CONTINUE_TIMEOUT}с — остановлено.",
                buttons=None,
            )
        except Exception:
            pass
        return False, status_msg
    finally:
        _pending_continue.pop(key, None)


async def process_verification(event, start_param: str):
    buyer_id = event.sender_id
    status_msg = await event.reply(
        "🔄 Запрос принят. Ищу и покупаю Telegram-аккаунт на Lolz..."
    )
    await log_event(bot, "START", "—", buyer_id, extra=f"start=`{start_param[:40]}`")

    seen_ids: set[int] = set()
    page_state = {"page": 1}
    tried = 0

    while tried < MAX_TRIES:
        tried += 1
        status_msg = await update_status(
            status_msg,
            f"🔄 Попытка [{tried}/{MAX_TRIES}]: поиск и покупка...",
        )

        sess, status_msg, reason = await buy_one_account(
            status_msg, buyer_id, seen_ids, page_state
        )

        if reason == "search_error":
            status_msg = await update_status(
                status_msg, "⚠️ Ошибка поиска Lolz, жду 5 сек..."
            )
            await asyncio.sleep(5)
            continue

        if reason == "no_funds":
            status_msg = await update_status(
                status_msg,
                "💸 API: недостаточно средств.\n\n"
                "После сплита балансов нужен `balance_id`.\n"
                "Смотри в логах Railway строки `[BALANCE]`.\n"
                "Можно задать вручную env `LOLZ_BALANCE_ID=...`",
            )
            await log_event(bot, "ERROR", "—", buyer_id, extra="insufficient funds / balance_id")
            return

        if reason == "no_items" or sess is None:
            status_msg = await update_status(
                status_msg,
                "📭 Нет подходящих аккаунтов (цена ≤ 7 ₽, без пароля) или все лоты исчерпаны.",
            )
            await log_event(bot, "EMPTY", "—", buyer_id, extra="нет лотов")
            return

        phone = sess.get("phone") or "unknown"
        auth_key = sess["auth_key"]
        dc_id = sess["dc_id"]
        tg_uid = sess.get("user_id") or ""
        item_id = sess.get("item_id")

        status_msg = await update_status(
            status_msg,
            f"🛒 Куплен `{phone}` (item `{item_id}`)\n"
            f"Подключаю session и проверяю Mamba...",
        )

        # build session
        session_path = TEMP_DIR / f"{phone or item_id}_{item_id}.session"
        string_session = None
        session_name = session_path.name
        try:
            write_sqlite_session(session_path, auth_key, dc_id)
        except Exception as e:
            print(f"[SESSION FILE] {e}, fallback StringSession")
            try:
                string_session = authkey_to_string_session(auth_key, dc_id)
                session_name = f"{phone or item_id}.string"
                cleanup_session_file(session_path)
                session_path = None
            except Exception as e2:
                await log_event(bot, "ERROR", phone, buyer_id, extra=str(e2))
                await archive_account(
                    bot, "ERROR", phone, auth_key, dc_id, tg_uid, None, buyer_id, str(e2)
                )
                if AUTO_CLAIM_ON_DEAD:
                    claim = await lolz.create_claim(item_id, CLAIM_TEXT_DEAD + f"\nItem ID: {item_id}")
                    await log_event(
                        bot,
                        "DEAD",
                        phone,
                        buyer_id,
                        session_name=session_name,
                        extra=f"claim={not claim.get('_error')} item={item_id}",
                    )
                go, status_msg = await ask_continue(
                    event,
                    status_msg,
                    f"💀 **DEAD** (не собралась session) — `{phone}`\n"
                    f"Попытка {tried}/{MAX_TRIES}. Купить следующий?",
                    buyer_id=buyer_id,
                )
                if not go:
                    await update_status(status_msg, f"🛑 Остановлено после DEAD `{phone}`.")
                    return
                continue

        result = await check_mamba_with_session(
            session_path=session_path,
            string_session=string_session,
            start_param=start_param,
        )

        # --- VALID: успех, стоп ---
        if result == "VALID":
            await update_status(
                status_msg,
                f"✅ **Mamba верифицирована**\n\nАккаунт: `{phone}`",
            )
            await log_event(bot, "VALID", phone, buyer_id, session_name=session_name)
            await archive_account(
                bot, "VALID", phone, auth_key, dc_id, tg_uid, session_path, buyer_id
            )
            cleanup_session_file(session_path)
            return

        # --- NOVALID: архив + спросить продолжать ли ---
        if result == "NOVALID":
            await log_event(
                bot,
                "NOVALID",
                phone,
                buyer_id,
                session_name=session_name,
                extra=f"item={item_id}",
            )
            await archive_account(
                bot, "NOVALID", phone, auth_key, dc_id, tg_uid, session_path, buyer_id
            )
            cleanup_session_file(session_path)
            go, status_msg = await ask_continue(
                event,
                status_msg,
                f"❌ **NOVALID** — `{phone}` (item `{item_id}`)\n\n"
                f"Попытка {tried}/{MAX_TRIES}. Купить следующий аккаунт?",
                buyer_id=buyer_id,
            )
            if not go:
                await update_status(status_msg, f"🛑 Остановлено после NOVALID `{phone}`.")
                await log_event(bot, "STOP", phone, buyer_id, extra="user stop after NOVALID")
                return
            continue

        # --- DEAD: claim + спросить ---
        if result == "DEAD":
            claim_ok = False
            if AUTO_CLAIM_ON_DEAD:
                claim = await lolz.create_claim(
                    item_id,
                    CLAIM_TEXT_DEAD + f"\nPhone: {phone}\nItem ID: {item_id}",
                )
                claim_ok = not claim.get("_error")
                print(f"[CLAIM] item={item_id} ok={claim_ok} resp={claim}")

            await log_event(
                bot,
                "DEAD",
                phone,
                buyer_id,
                session_name=session_name,
                extra=f"item={item_id} claim={'ok' if claim_ok else 'fail'}",
            )
            await archive_account(
                bot,
                "DEAD",
                phone,
                auth_key,
                dc_id,
                tg_uid,
                session_path,
                buyer_id,
                extra=f"claim={'ok' if claim_ok else 'fail'}",
            )
            cleanup_session_file(session_path)
            claim_line = "Претензия отправлена." if claim_ok else "Претензия не создалась."
            go, status_msg = await ask_continue(
                event,
                status_msg,
                f"💀 **DEAD** — `{phone}` (item `{item_id}`)\n{claim_line}\n\n"
                f"Попытка {tried}/{MAX_TRIES}. Купить следующий?",
                buyer_id=buyer_id,
            )
            if not go:
                await update_status(status_msg, f"🛑 Остановлено после DEAD `{phone}`.")
                await log_event(bot, "STOP", phone, buyer_id, extra="user stop after DEAD")
                return
            continue

        # --- ERROR: архив + спросить ---
        await log_event(
            bot,
            "ERROR",
            phone,
            buyer_id,
            session_name=session_name,
            extra=f"item={item_id}",
        )
        await archive_account(
            bot, "ERROR", phone, auth_key, dc_id, tg_uid, session_path, buyer_id
        )
        cleanup_session_file(session_path)
        go, status_msg = await ask_continue(
            event,
            status_msg,
            f"⚠️ **ERROR** — `{phone}` (item `{item_id}`)\n\n"
            f"Попытка {tried}/{MAX_TRIES}. Купить следующий?",
            buyer_id=buyer_id,
        )
        if not go:
            await update_status(status_msg, f"🛑 Остановлено после ERROR `{phone}`.")
            await log_event(bot, "STOP", phone, buyer_id, extra="user stop after ERROR")
            return

    # исчерпали попытки
    await update_status(
        status_msg,
        "⚠️ Не удалось подтвердить после перебора.\n"
        "Возможные причины: все аккаунты NOVALID/DEAD, закончился баланс, нет лотов.",
    )
    await log_event(bot, "END", "—", buyer_id, extra="лимит попыток")


# ====================== BOT HANDLERS ======================

bot = TelegramClient("bot_session", API_ID, API_HASH).start(bot_token=BOT_TOKEN)


@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start_cmd(event):
    await event.reply(
        "Пришли ссылку для верификации Мамбы.\n\n"
        "Бот купит Telegram-аккаунт на Lolz (до 7 ₽, без пароля/2FA, свежий),\n"
        "соберёт session и проверит анкету.\n\n"
        "• **VALID** — готово\n"
        "• **NOVALID / DEAD / ERROR** — кнопки «Купить следующий» / «Стоп»\n"
        "  (деньги не тратятся без твоего подтверждения)"
    )


@bot.on(events.CallbackQuery(pattern=rb"^cont:(yes|no)$"))
async def on_continue_callback(event):
    """Обработка кнопок продолжения покупки."""
    data = event.data.decode()
    buyer_id = event.sender_id
    msg_id = event.message_id
    key = f"{buyer_id}:{msg_id}"
    fut = _pending_continue.get(key)

    # также ищем по любому pending этого юзера (на случай edit id)
    if fut is None:
        for k, f in list(_pending_continue.items()):
            if k.startswith(f"{buyer_id}:") and not f.done():
                fut = f
                key = k
                break

    ok = data.endswith("yes")
    if fut is not None and not fut.done():
        fut.set_result(ok)

    try:
        if ok:
            await event.answer("Ищем следующий аккаунт...")
            try:
                await event.edit(
                    (event.message.message or "") + "\n\n▶️ Продолжаем...",
                    buttons=None,
                )
            except Exception:
                pass
        else:
            await event.answer("Остановлено")
            try:
                await event.edit(
                    (event.message.message or "") + "\n\n🛑 Остановлено.",
                    buttons=None,
                )
            except Exception:
                pass
    except Exception as e:
        print(f"[CALLBACK] {e}")


@bot.on(events.NewMessage(pattern=r"(?i).*(tg://|start=|mambarubot)"))
async def handle_link(event):
    if not event.is_private:
        return
    text = event.raw_text.strip()
    start_param = None
    match = re.search(r"start=([a-zA-Z0-9_\-]+)", text)
    if match:
        start_param = match.group(1)
    elif re.fullmatch(r"[a-zA-Z0-9_\-]{10,}", text):
        start_param = text
    if not start_param:
        await event.reply("❌ Некорректная ссылка.")
        return
    asyncio.create_task(process_verification(event, start_param))


print("Бот запущен (Lolz Market mode)...")
print(
    f"MAX_PRICE={MAX_PRICE} MAX_TRIES={MAX_TRIES} "
    f"CURRENCY={CURRENCY} AUTO_CLAIM_ON_DEAD={AUTO_CLAIM_ON_DEAD}"
)
print(f"LOG_SUPERGROUP_ID={LOG_SUPERGROUP_ID}")


async def _startup():
    try:
        ent = await resolve_log_chat(bot)
        print(f"[STARTUP] log chat ok: {ent}")
    except Exception as e:
        print(f"[STARTUP] log chat resolve failed: {e}")
        print(
            "Проверь:\n"
            "1) Бот добавлен в супергруппу и является админом\n"
            "2) LOG_SUPERGROUP_ID верный (лучше -100XXXXXXXXXX)\n"
            "3) У бота есть право писать в топики"
        )

    try:
        me = await lolz.get_profile()
        if me.get("_error"):
            print(f"[LOLZ /me] error: {me}")
        else:
            user = me.get("user") or me.get("me") or me
            uid = uname = None
            if isinstance(user, dict):
                uid = user.get("user_id") or user.get("userId")
                uname = user.get("username")
            print(f"[LOLZ /me] user_id={uid} username={uname}")
            print(f"[LOLZ /me] snippet: {json.dumps(me, ensure_ascii=False, default=str)[:800]}")
    except Exception as e:
        print(f"[LOLZ /me] failed: {e}")

    try:
        bid = await resolve_balance_id()
        print(f"[STARTUP] balance_id={bid} (env LOLZ_BALANCE_ID={LOLZ_BALANCE_ID})")
    except Exception as e:
        print(f"[STARTUP] balance resolve failed: {e}")


bot.loop.run_until_complete(_startup())
bot.run_until_disconnected()
