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
from telethon import TelegramClient, events
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

LOG_SUPERGROUP_ID = int(os.environ["LOG_SUPERGROUP_ID"])
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

LOLZ_BASE = os.environ.get("LOLZ_BASE", "https://prod-api.lzt.market")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "300"))

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

    async def fast_buy(self, item_id: int, price: float | None = None):
        body = {}
        if price is not None:
            body["price"] = price
        return await self._request("POST", f"/{item_id}/fast-buy", json_body=body or None)

    async def get_item(self, item_id: int):
        return await self._request("GET", f"/{item_id}")

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


async def send_to_topic(client: TelegramClient, topic_id: int, text: str, file=None):
    kwargs = {
        "entity": LOG_SUPERGROUP_ID,
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


def is_check_ok(data: dict) -> bool:
    if not data or data.get("_error"):
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
    err = str(data.get("error") or data.get("errors") or "")
    if err and "retry" not in err.lower():
        bad = ("sold", "deleted", "invalid", "not enough", "blacklist", "limit")
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
    err = str(data.get("error") or "")
    if err:
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

            price = item.get("price") or item.get("rub_price") or MAX_PRICE
            phone_hint = item.get("telegram_phone") or item.get("title") or item_id

            status_msg = await update_status(
                status_msg,
                f"🔄 Проверяю лот `{item_id}` ({phone_hint})...",
            )

            check = await lolz.check_account(int(item_id))
            if not is_check_ok(check):
                print(f"[CHECK FAIL] {item_id}: {check.get('error') or check.get('_http')}")
                continue

            status_msg = await update_status(
                status_msg,
                f"✅ Лот `{item_id}` валиден, покупаю...",
            )

            buy = await lolz.fast_buy(int(item_id), price=float(price) if price else None)
            if not is_buy_ok(buy):
                print(f"[BUY FAIL] {item_id}: {buy.get('error') or buy.get('_http')}")
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

async def update_status(status_msg, text: str):
    try:
        await status_msg.edit(text)
        return status_msg
    except Exception:
        try:
            return await status_msg.respond(text)
        except Exception:
            return status_msg


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
                # считаем как DEAD-подобное → claim + следующий
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

        # --- NOVALID: архив + следующий номер ---
        if result == "NOVALID":
            await log_event(
                bot,
                "NOVALID",
                phone,
                buyer_id,
                session_name=session_name,
                extra=f"item={item_id}, беру следующий",
            )
            await archive_account(
                bot, "NOVALID", phone, auth_key, dc_id, tg_uid, session_path, buyer_id
            )
            cleanup_session_file(session_path)
            status_msg = await update_status(
                status_msg,
                f"❌ NOVALID — `{phone}`\nБеру следующий аккаунт...",
            )
            continue

        # --- DEAD: claim (замена/возврат) + архив + следующий ---
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
            status_msg = await update_status(
                status_msg,
                f"💀 DEAD — `{phone}`\n"
                f"{'Претензия отправлена. ' if claim_ok else 'Претензия не создалась. '}"
                f"Беру следующий...",
            )
            continue

        # --- ERROR: архив + следующий ---
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
        status_msg = await update_status(
            status_msg,
            f"⚠️ ERROR — `{phone}`\nБеру следующий...",
        )

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
        "Пришли ссылку для **автоматической** верификации Мамбы.\n\n"
        "Бот сам купит Telegram-аккаунт на Lolz (до 7 ₽, без пароля/2FA, свежий),\n"
        "соберёт session и подтвердит анкету.\n\n"
        "• NOVALID → следующий аккаунт\n"
        "• DEAD → претензия (замена/возврат) + следующий"
    )


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
bot.run_until_disconnected()
