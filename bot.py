import os
import re
import json
import asyncio
import tempfile
import struct
import base64
import ipaddress
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import ClassVar, Mapping

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

        print(f"[FAST-BUY] item={item_id} body={body}")

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

def _normalize_auth_hex(raw: str) -> str:
    s = str(raw).strip().replace(" ", "").replace("\n", "").replace("\r", "")
    if s.lower().startswith("0x"):
        s = s[2:]
    s = re.sub(r"[^0-9a-fA-F]", "", s)
    return s


@dataclass(frozen=True)
class TelegramSessionEncoder:
    """
    Модуль от поддержки Lolz Market.
    Строит StringSession из auth_key (256 bytes) + dc_id.
    """
    auth_key: bytes
    dc_id: int

    _VERSION: ClassVar[str] = "1"
    _PORT: ClassVar[int] = 443
    _DC_IP_MAP: ClassVar[Mapping[int, str]] = {
        1: "149.154.175.53",
        2: "149.154.167.51",
        3: "149.154.175.100",
        4: "149.154.167.91",
        5: "91.108.56.130",
    }

    def to_string(self) -> str:
        ip_bytes = self._resolve_ip()
        payload = self._build_payload(ip_bytes)
        encoded = base64.urlsafe_b64encode(payload).decode("ascii")
        return f"{self._VERSION}{encoded}"

    def _resolve_ip(self) -> bytes:
        ip = self._DC_IP_MAP.get(self.dc_id)
        if not ip:
            raise ValueError(f"Unknown data center ID: {self.dc_id}")
        return ipaddress.ip_address(ip).packed

    def _build_payload(self, ip_bytes: bytes) -> bytes:
        if len(self.auth_key) != 256:
            raise ValueError("auth_key must be exactly 256 bytes")
        fmt = f">B{len(ip_bytes)}sH256s"
        return struct.pack(fmt, self.dc_id, ip_bytes, self._PORT, self.auth_key)


def authkey_to_string_session(auth_key_hex: str, dc_id: int) -> str:
    """Собираем Telethon StringSession (как веб-маркет / модуль поддержки)."""
    hex_key = _normalize_auth_hex(auth_key_hex)
    key = bytes.fromhex(hex_key)
    if len(key) != 256:
        raise ValueError(f"auth_key must be 256 bytes, got {len(key)}")
    return TelegramSessionEncoder(auth_key=key, dc_id=int(dc_id)).to_string()


def _walk_find_auth(obj, found=None, depth=0):
    """Рекурсивно ищет auth_key (hex ~512 символов) и dc_id в любом JSON."""
    if found is None:
        found = {"auth_key": None, "dc_id": None, "phone": None, "user_id": None}
    if depth > 8 or obj is None:
        return found

    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if v is None or v == "" or v == []:
                continue
            # прямые ключи
            if kl in (
                "auth_key", "authkey", "auth_key_hex", "authkeyhex",
                "telegram_auth_key", "session_key", "authkeyhex",
            ) and isinstance(v, str) and len(re.sub(r"[^0-9a-fA-F]", "", v)) >= 64:
                found["auth_key"] = v
            if kl in ("dc_id", "dcid", "telegram_dc_id", "data_center", "dc") and str(v).isdigit():
                if 1 <= int(v) <= 5:
                    found["dc_id"] = int(v)
            if kl in ("telegram_phone", "phone", "tel", "accountphone") and not found["phone"]:
                found["phone"] = str(v)
            if kl in ("telegram_id", "telegramid", "user_id", "userid") and not found["user_id"]:
                if str(v).isdigit() and len(str(v)) >= 5:
                    found["user_id"] = str(v)
            # login/password паттерн маркета для TG
            if kl == "login" and isinstance(v, str):
                hexpart = re.sub(r"[^0-9a-fA-F]", "", v)
                if len(hexpart) >= 64:
                    found["auth_key"] = found["auth_key"] or v
                elif re.search(r"\d{10,15}", v) and not found["phone"]:
                    found["phone"] = v
            if kl == "password" and isinstance(v, (str, int)):
                s = str(v).strip()
                if s.isdigit() and 1 <= int(s) <= 5:
                    found["dc_id"] = found["dc_id"] or int(s)
                hexpart = re.sub(r"[^0-9a-fA-F]", "", s)
                if len(hexpart) >= 64:
                    found["auth_key"] = found["auth_key"] or s
            # значения вида "5:hex..." или "hex:5"
            if isinstance(v, str) and ":" in v:
                parts = v.split(":")
                if len(parts) == 2:
                    a, b = parts[0].strip(), parts[1].strip()
                    if a.isdigit() and 1 <= int(a) <= 5 and len(re.sub(r"[^0-9a-fA-F]", "", b)) >= 64:
                        found["dc_id"] = found["dc_id"] or int(a)
                        found["auth_key"] = found["auth_key"] or b
                    elif b.isdigit() and 1 <= int(b) <= 5 and len(re.sub(r"[^0-9a-fA-F]", "", a)) >= 64:
                        found["auth_key"] = found["auth_key"] or a
                        found["dc_id"] = found["dc_id"] or int(b)
            # длинная hex-строка без ключа
            if isinstance(v, str):
                hexpart = re.sub(r"[^0-9a-fA-F]", "", v)
                if len(hexpart) in (512, 256) or (len(hexpart) >= 500 and len(hexpart) <= 520):
                    found["auth_key"] = found["auth_key"] or hexpart
            _walk_find_auth(v, found, depth + 1)
    elif isinstance(obj, list):
        for x in obj[:50]:
            _walk_find_auth(x, found, depth + 1)
    return found


def _parse_telegram_json(raw) -> dict:
    """
    Поддержка: telegram_json — строка JSON (как сказал support Lolz).
    Также dict / уже распарсенный объект.
    """
    if raw is None or raw == "" or raw == "?":
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="ignore")
        except Exception:
            return {}
    if not isinstance(raw, str):
        return {}
    s = raw.strip()
    # иногда двойной encode
    for _ in range(2):
        try:
            obj = json.loads(s)
        except Exception:
            return {}
        if isinstance(obj, str):
            s = obj.strip()
            continue
        if isinstance(obj, dict):
            return obj
        return {}
    return {}


def extract_session_data(item: dict) -> dict:
    """
    Support Lolz:
      session через API не скачивается;
      данные в item['telegram_json'] (string JSON);
      session собираем сами из auth_key + dc_id.
    """
    if not item:
        return {}

    login = item.get("loginData") or item.get("login_data") or item.get("telegramData") or {}
    if isinstance(login, str):
        try:
            login = json.loads(login)
        except Exception:
            login = {"raw": login}
    if not isinstance(login, dict):
        login = {}

    # --- 1) ГЛАВНЫЙ ИСТОЧНИК (support): item['telegram_json'] как строка JSON ---
    tg_json_raw = (
        item.get("telegram_json")
        or item.get("telegramJson")
        or item.get("telegram_json_data")
        or login.get("telegram_json")
        or login.get("telegramJson")
        or login.get("json")
    )
    tg = _parse_telegram_json(tg_json_raw)
    if tg:
        print(f"[telegram_json] keys={list(tg.keys())[:30]}")
        print(f"[telegram_json] snippet={json.dumps(tg, ensure_ascii=False, default=str)[:400]}")
    else:
        print(
            f"[telegram_json] EMPTY "
            f"item_has={bool(item.get('telegram_json') or item.get('telegramJson'))} "
            f"login_has={bool(login.get('telegram_json') or login.get('telegramJson'))}"
        )

    sources = [tg, login, item]

    def dig(*keys, default=None):
        for src in sources:
            if not isinstance(src, dict):
                continue
            for k in keys:
                if k in src and src[k] not in (None, "", [], "?"):
                    return src[k]
                # case-insensitive
                for sk, sv in src.items():
                    if str(sk).lower() == k.lower() and sv not in (None, "", [], "?"):
                        return sv
        return default

    phone = dig(
        "phone", "telegram_phone", "tel", "accountPhone", "number", "msisdn",
        default="",
    )
    user_id = dig(
        "user_id", "userId", "telegram_id", "telegramId", "id", "uid",
    )
    dc_id = dig(
        "dc_id", "dcId", "dc", "telegram_dc_id", "data_center", "dataCenter",
    )
    auth_key = dig(
        "auth_key", "authKey", "auth_key_hex", "authKeyHex",
        "telegram_auth_key", "session_key", "key", "auth",
    )

    # иногда auth лежит в login/password
    if not auth_key:
        for cand in (dig("login"), dig("password"), login.get("login"), login.get("password")):
            if isinstance(cand, str) and len(_normalize_auth_hex(cand)) >= 64:
                auth_key = cand
                break

    # формат "dc:hex" / "hex:dc"
    if isinstance(auth_key, str) and ":" in auth_key:
        parts = auth_key.split(":")
        if len(parts) == 2:
            a, b = parts[0].strip(), parts[1].strip()
            ha, hb = _normalize_auth_hex(a), _normalize_auth_hex(b)
            if a.isdigit() and 1 <= int(a) <= 5 and len(hb) >= 64:
                dc_id, auth_key = int(a), b
            elif b.isdigit() and 1 <= int(b) <= 5 and len(ha) >= 64:
                auth_key, dc_id = a, int(b)

    # fallback deep walk
    if not auth_key or not dc_id:
        deep = _walk_find_auth({"telegram_json": tg, "loginData": login, "item": item})
        auth_key = auth_key or deep.get("auth_key")
        dc_id = dc_id or deep.get("dc_id")
        phone = phone or deep.get("phone") or ""
        user_id = user_id or deep.get("user_id")

    if not dc_id and item.get("telegram_dc_id"):
        try:
            dc_id = int(item["telegram_dc_id"])
        except Exception:
            pass

    if phone:
        phone = re.sub(r"[^\d+]", "", str(phone))
    else:
        phone = ""

    auth_clean = _normalize_auth_hex(auth_key) if auth_key else None
    if auth_clean and len(auth_clean) not in (512, 256) and len(auth_clean) < 64:
        auth_clean = None
    # 256 hex chars = 128 bytes — иногда ключ укорочен; 512 = 256 bytes — норма
    if auth_clean and len(auth_clean) == 256:
        # некоторые отдают 128-byte key как hex256 — Telethon ждёт 256 bytes (512 hex)
        print(f"[extract] auth_key len=256 hex (128 bytes) — может быть неполным")

    try:
        dc_id = int(dc_id) if dc_id not in (None, "") else None
    except Exception:
        dc_id = None

    return {
        "phone": str(phone or ""),
        "user_id": str(user_id or "") if user_id not in (None, "") else "",
        "dc_id": dc_id,
        "auth_key": auth_clean,
        "raw_item": item,
        "telegram_json": tg,
    }


def write_sqlite_session(path: Path, auth_key_hex: str, dc_id: int) -> Path:
    path = Path(path)
    if path.suffix != ".session":
        path = path.with_suffix(".session")

    hex_key = _normalize_auth_hex(auth_key_hex)
    key = bytes.fromhex(hex_key)
    if len(key) != 256:
        raise ValueError(f"auth_key must be 256 bytes, got {len(key)}")
    ip = DC_IP_MAP[int(dc_id)]

    # удалить старый файл если есть
    try:
        path.unlink(missing_ok=True)
        Path(str(path) + "-journal").unlink(missing_ok=True)
    except Exception:
        pass

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


async def build_session_artifacts(
    phone: str,
    item_id,
    auth_key: str | None,
    dc_id: int | None,
    tg_user_id: str = "",
    status: str = "",
    buyer_id: int = 0,
    extra: str = "",
) -> tuple[Path | None, Path | None, str | None]:
    """
    Всегда создаёт:
      - .session (Telethon SQLite)
      - .json  (phone, auth_key, dc_id, user_id, string_session, ...)
    Возвращает (session_path, json_path, string_session).
    """
    safe_phone = re.sub(r"[^\d]", "", str(phone or "")) or "unknown"
    base = TEMP_DIR / f"{safe_phone}_{item_id or 'x'}_{status or 'acc'}"
    session_path = base.with_suffix(".session")
    json_path = base.with_suffix(".json")
    string_session = None

    if auth_key and dc_id:
        try:
            string_session = authkey_to_string_session(auth_key, int(dc_id))
        except Exception as e:
            print(f"[ARCHIVE] StringSession fail: {e}")
        try:
            write_sqlite_session(session_path, auth_key, int(dc_id))
            print(f"[ARCHIVE] session file: {session_path}")
        except Exception as e:
            print(f"[ARCHIVE] sqlite fail: {e}")
            session_path = None
    else:
        session_path = None

    meta = {
        "status": status,
        "phone": phone,
        "user_id": tg_user_id,
        "dc_id": dc_id,
        "auth_key": auth_key,
        "string_session": string_session,
        "item_id": item_id,
        "buyer_id": buyer_id,
        "extra": extra,
        "time": now_str(),
    }
    try:
        json_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[ARCHIVE] json file: {json_path}")
    except Exception as e:
        print(f"[ARCHIVE] json fail: {e}")
        json_path = None

    return session_path, json_path, string_session


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
    item_id=None,
):
    """
    Архив в топик VALID/NOVALID/DEAD/ERROR:
      текст + файл .session + файл .json (auth_key, dc, phone, string_session).
    """
    topic_map = {
        "VALID": TOPIC_VALID,
        "NOVALID": TOPIC_NOVALID,
        "DEAD": TOPIC_DEAD,
        "ERROR": TOPIC_ERROR,
    }
    topic = topic_map.get(status, TOPIC_ERROR)

    # гарантированно собираем файлы (даже если session_path не передали)
    built_session, json_path, string_session = await build_session_artifacts(
        phone=phone,
        item_id=item_id or phone,
        auth_key=auth_key,
        dc_id=dc_id,
        tg_user_id=tg_user_id,
        status=status,
        buyer_id=buyer_id,
        extra=extra,
    )
    if built_session and built_session.exists():
        session_path = built_session
    elif session_path and not Path(session_path).exists():
        session_path = None

    # caption короткий (лимит TG ~1024 для media)
    text = (
        f"**ARCHIVE · {status}**\n\n"
        f"📞 Phone: `{phone or '—'}`\n"
        f"🆔 TG User ID: `{tg_user_id or '—'}`\n"
        f"📡 DC ID: `{dc_id or '—'}`\n"
        f"📦 Item: `{item_id or '—'}`\n"
        f"👤 Buyer: `{buyer_id}`\n"
        f"🕒 `{now_str()}`\n"
        f"#{status}"
    )
    if extra:
        text += f"\nℹ️ {extra}"
    if auth_key:
        # не целиком в caption — обрежем, полный в .json
        ak = auth_key if len(auth_key) <= 64 else (auth_key[:24] + "…" + auth_key[-24:])
        text += f"\n🔑 Auth: `{ak}`"

    files = []
    if session_path and Path(session_path).exists():
        files.append(str(session_path))
    if json_path and Path(json_path).exists():
        files.append(str(json_path))

    print(f"[ARCHIVE] status={status} topic={topic} files={files}")

    if files:
        # сначала session, потом json — двумя сообщениями надёжнее
        await send_to_topic(bot_client, topic, text, file=files[0])
        for f in files[1:]:
            await send_to_topic(
                bot_client,
                topic,
                f"📎 meta `{Path(f).name}` · {status} · `{phone}`",
                file=f,
            )
    else:
        # файлов нет — хотя бы текст + полный auth
        long_text = text
        if auth_key:
            long_text += f"\n\n🔑 Auth Key (HEX):\n`{auth_key}`"
        if string_session:
            long_text += f"\n\n🧵 StringSession:\n`{string_session[:200]}…`"
        await send_to_topic(bot_client, topic, long_text)

    # подчистить временные
    cleanup_session_file(session_path)
    if json_path:
        try:
            Path(json_path).unlink(missing_ok=True)
        except Exception:
            pass


# ====================== MAMBA CHECK ======================

def _classify_mamba_text(text: str) -> str | None:
    t = (text or "").lower()
    if not t:
        return None
    if "поздравляем" in t and ("анкета подтверждена" in t or "подтверждена" in t):
        return "VALID"
    if "анкета подтверждена" in t or "успешно подтвержд" in t:
        return "VALID"
    if any(
        x in t
        for x in (
            "что-то пошло не так",
            "something is wrong",
            "не может использоваться для подтверждения",
            "был использован ранее",
            "не подходит для подтверждения",
            "нельзя использовать",
            "already been used",
            "cannot be used",
        )
    ):
        return "NOVALID"
    return None


async def check_mamba_with_session(
    session_path: Path | None = None,
    string_session: str | None = None,
    start_param: str = "",
) -> str:
    """
    VALID / NOVALID / DEAD / ERROR
    DEAD  — сессия не логинится
    NOVALID — залогинились, mamba отказала
    ERROR — сеть/таймаут (не считаем аккаунт мёртвым)
    """
    client = None
    try:
        if string_session:
            client = TelegramClient(
                StringSession(string_session),
                API_ID,
                API_HASH,
                device_model="Desktop",
                system_version="Windows 10",
                app_version="4.16.8 x64",
                lang_code="en",
                system_lang_code="en-US",
            )
        elif session_path:
            session_name = str(Path(session_path).with_suffix(""))
            client = TelegramClient(
                session_name,
                API_ID,
                API_HASH,
                device_model="Desktop",
                system_version="Windows 10",
                app_version="4.16.8 x64",
                lang_code="en",
                system_lang_code="en-US",
            )
        else:
            print("[MAMBA CHECK] no session provided")
            return "ERROR"

        # connect
        try:
            await asyncio.wait_for(client.connect(), timeout=25)
        except asyncio.TimeoutError:
            print("[MAMBA CHECK] connect timeout → ERROR")
            return "ERROR"
        except (AuthKeyUnregisteredError, UserDeactivatedError) as e:
            print(f"[MAMBA CHECK] auth dead: {e}")
            return "DEAD"
        except SessionPasswordNeededError:
            print("[MAMBA CHECK] 2FA needed → DEAD (фильтр password=no)")
            return "DEAD"
        except Exception as e:
            print(f"[MAMBA CHECK] connect fail {type(e).__name__}: {e}")
            # типичные «мёртвые» ключи
            en = type(e).__name__.lower() + " " + str(e).lower()
            if any(x in en for x in ("authkey", "unregistered", "deactivated", "user_deactivated")):
                return "DEAD"
            return "ERROR"

        try:
            authorized = await client.is_user_authorized()
        except Exception as e:
            print(f"[MAMBA CHECK] is_user_authorized fail: {e}")
            return "DEAD"

        if not authorized:
            print("[-] session not authorized → DEAD")
            return "DEAD"

        try:
            me = await client.get_me()
            print(f"[MAMBA CHECK] logged in as id={getattr(me, 'id', '?')} phone={getattr(me, 'phone', '?')}")
        except Exception as e:
            print(f"[MAMBA CHECK] get_me fail: {e}")

        # /start mamba
        try:
            await asyncio.wait_for(
                client(StartBotRequest(bot=MAMBA_BOT, peer=MAMBA_BOT, start_param=start_param)),
                timeout=20,
            )
        except FloodWaitError as e:
            wait = min(int(getattr(e, "seconds", 5)), 30)
            print(f"[MAMBA CHECK] FloodWait {wait}s")
            await asyncio.sleep(wait)
            try:
                await client(StartBotRequest(bot=MAMBA_BOT, peer=MAMBA_BOT, start_param=start_param))
            except Exception as e2:
                print(f"[MAMBA CHECK] StartBot retry fail: {e2}")
                return "ERROR"
        except Exception as e:
            print(f"[MAMBA CHECK] StartBotRequest fail: {e}")
            # если уже в диалоге — попробуем просто /start текстом
            try:
                await client.send_message(MAMBA_BOT, f"/start {start_param}".strip())
            except Exception as e2:
                print(f"[MAMBA CHECK] send /start fail: {e2}")
                return "ERROR"

        # ждём ответ бота (несколько попыток)
        for attempt in range(6):
            await asyncio.sleep(1.5)
            try:
                messages = await client.get_messages(MAMBA_BOT, limit=20)
            except Exception as e:
                print(f"[MAMBA CHECK] get_messages fail: {e}")
                continue
            for msg in messages:
                text = msg.message or ""
                cls = _classify_mamba_text(text)
                if cls:
                    print(f"[MAMBA CHECK] classified {cls}: {text[:120]!r}")
                    return cls
            print(f"[MAMBA CHECK] no decisive reply yet attempt={attempt+1}")

        # залогинены, но mamba не дала понятный ответ → NOVALID (не ERROR)
        print("[MAMBA CHECK] no clear mamba reply → NOVALID")
        return "NOVALID"

    except (AuthKeyUnregisteredError, UserDeactivatedError):
        return "DEAD"
    except SessionPasswordNeededError:
        return "DEAD"
    except FloodWaitError as e:
        print(f"[MAMBA CHECK] FloodWait outer: {e}")
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


def _collect_balance_list(payload) -> list:
    """Достаёт список балансов из любого типичного ответа API."""
    if not payload:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    if payload.get("_error"):
        return []

    # прямое поле balances (как сказал специалист — GET /me)
    for key in ("balances", "items", "list", "data", "exchange"):
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
        if isinstance(val, dict):
            # dict balance_id -> info  ИЛИ  currency -> info
            out = []
            for k, v in val.items():
                if isinstance(v, dict):
                    item = dict(v)
                    if "balance_id" not in item and "id" not in item:
                        try:
                            item["balance_id"] = int(k)
                        except Exception:
                            item.setdefault("currency", k)
                    out.append(item)
                elif isinstance(v, (int, float, str)):
                    # {"rub": 205.0} или {"12345": 205}
                    try:
                        out.append({"balance_id": int(k), "amount": float(v), "currency": str(k)})
                    except Exception:
                        out.append({"currency": str(k), "amount": v})
            if out:
                return out

    # вложенный user.balances
    user = payload.get("user")
    if isinstance(user, dict):
        nested = _collect_balance_list(user)
        if nested:
            return nested

    if any(k in payload for k in ("balance_id", "id", "currency", "type")):
        return [payload]
    return []


def _pick_balance_id(balances_payload) -> int | None:
    """
    Выбирает balance_id кошелька «для покупок».
    Приоритет (по ответу специалиста Lolz):
      1) type/name содержит purchase / buy / покупок
      2) currency == CURRENCY (rub)
      3) максимальный amount
    """
    candidates = _collect_balance_list(balances_payload)
    if not candidates:
        return None

    currency_want = CURRENCY.lower()

    def bid_of(b: dict):
        for k in ("balance_id", "id", "balanceId", "wallet_id"):
            if b.get(k) is not None:
                try:
                    return int(b[k])
                except Exception:
                    pass
        return None

    def amount_of(b: dict) -> float:
        for k in ("amount", "balance", "value", "money", "sum"):
            if b.get(k) is not None:
                try:
                    return float(b[k])
                except Exception:
                    pass
        return 0.0

    def meta_text(b: dict) -> str:
        parts = []
        for k in (
            "type", "name", "title", "label", "description",
            "balance_type", "kind", "slug", "code",
        ):
            if b.get(k) is not None:
                parts.append(str(b[k]))
        return " ".join(parts).lower()

    purchase_keys = (
        "покуп", "purchase", "buy", "buying", "spend",
        "market", "для покупок", "purchases",
    )

    scored = []
    for b in candidates:
        bid = bid_of(b)
        if bid is None:
            continue
        cur = str(
            b.get("currency") or b.get("currency_code") or b.get("code") or ""
        ).lower()
        amount = amount_of(b)
        meta = meta_text(b)
        is_purchase = any(k in meta for k in purchase_keys)
        # иногда type=1 purchase, type=0 withdraw и т.п.
        if str(b.get("type") or "").lower() in ("purchase", "buy", "1"):
            is_purchase = True
        cur_ok = (cur == currency_want) or (currency_want in cur) or (cur in currency_want) or not cur
        scored.append((is_purchase, cur_ok, amount, bid, b))
        print(
            f"[BALANCE item] id={bid} cur={cur or '?'} amount={amount} "
            f"purchase={is_purchase} meta={meta[:80]!r}"
        )

    if not scored:
        return None

    # 1) явно «для покупок»
    purchase = [x for x in scored if x[0]]
    if purchase:
        purchase.sort(key=lambda x: (x[1], x[2]), reverse=True)
        return purchase[0][3]

    # 2) нужная валюта с макс. суммой
    same_cur = [x for x in scored if x[1]]
    if same_cur:
        same_cur.sort(key=lambda x: x[2], reverse=True)
        return same_cur[0][3]

    # 3) любой с макс. суммой
    scored.sort(key=lambda x: x[2], reverse=True)
    return scored[0][3]


async def resolve_balance_id() -> int | None:
    """
    balance_id для fast-buy.
    Специалист Lolz: брать из GET /me → balances (кошелёк «для покупок»).
    """
    global _cached_balance_id
    if LOLZ_BALANCE_ID > 0:
        print(f"[BALANCE] from env LOLZ_BALANCE_ID={LOLZ_BALANCE_ID}")
        return LOLZ_BALANCE_ID
    if _cached_balance_id is not None:
        return _cached_balance_id

    # 1) главный источник — GET /me
    me = await lolz.get_profile()
    print(f"[BALANCE /me] keys={list(me.keys()) if isinstance(me, dict) else type(me)}")
    print(f"[BALANCE /me] snippet: {json.dumps(me, ensure_ascii=False, default=str)[:1500]}")

    bid = _pick_balance_id(me)
    if bid is None and isinstance(me.get("user"), dict):
        bid = _pick_balance_id(me["user"])
    if bid is None and me.get("balances") is not None:
        bid = _pick_balance_id({"balances": me["balances"]})

    # 2) запасной endpoint
    if bid is None:
        data = await lolz.get_balances()
        print(f"[BALANCE exchange] snippet: {json.dumps(data, ensure_ascii=False, default=str)[:1000]}")
        bid = _pick_balance_id(data)

    _cached_balance_id = bid
    print(f"[BALANCE] selected balance_id={bid}")
    if bid is None:
        print(
            "[BALANCE] ⚠️ не нашли balance_id. "
            "Задай вручную LOLZ_BALANCE_ID из /me → balances"
        )
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
            try:
                if isinstance(bought_item, dict):
                    print(f"[BUY OK keys] item={item_id} keys={list(bought_item.keys())[:60]}")
                    tj = bought_item.get("telegram_json") or bought_item.get("telegramJson")
                    if tj is not None:
                        print(f"[BUY OK telegram_json type={type(tj).__name__}] {str(tj)[:500]}")
                    for lk in ("loginData", "login_data", "telegramData"):
                        if bought_item.get(lk) is not None:
                            print(
                                f"[BUY OK {lk}] "
                                f"{json.dumps(bought_item.get(lk), ensure_ascii=False, default=str)[:400]}"
                            )
            except Exception as e:
                print(f"[BUY OK dump fail] {e}")

            sess = extract_session_data(bought_item if isinstance(bought_item, dict) else {})
            # после покупки telegram_json может появиться только в GET
            if not sess.get("auth_key") or not sess.get("dc_id"):
                await asyncio.sleep(1.2)
                full = await lolz.get_item(int(item_id))
                print(
                    f"[GET ITEM after buy] top_keys="
                    f"{list(full.keys()) if isinstance(full, dict) else type(full)}"
                )
                full_item = full.get("item") if isinstance(full.get("item"), dict) else (
                    full if isinstance(full, dict) and not full.get("_error") else {}
                )
                if isinstance(full_item, dict):
                    print(f"[GET ITEM item keys] {list(full_item.keys())[:60]}")
                    tj = full_item.get("telegram_json") or full_item.get("telegramJson")
                    if tj is not None:
                        print(f"[GET ITEM telegram_json] {str(tj)[:600]}")
                    bought_item = {**(bought_item if isinstance(bought_item, dict) else {}), **full_item}
                sess = extract_session_data(bought_item if isinstance(bought_item, dict) else {})

            # dc с исходного лота, если API не отдал после покупки
            if not sess.get("dc_id") and item.get("telegram_dc_id"):
                try:
                    sess["dc_id"] = int(item["telegram_dc_id"])
                except Exception:
                    pass
            if not sess.get("phone") and item.get("telegram_phone"):
                sess["phone"] = re.sub(r"[^\d+]", "", str(item["telegram_phone"]))

            sess["item_id"] = int(item_id)
            sess["price"] = price

            if not sess.get("auth_key") or not sess.get("dc_id"):
                print(
                    f"[NO AUTH KEY] item={item_id} "
                    f"auth={bool(sess.get('auth_key'))} dc={sess.get('dc_id')}"
                )
                await log_event(
                    bot,
                    "ERROR",
                    sess.get("phone") or str(phone_hint),
                    buyer_id,
                    extra=f"нет auth_key/dc_id item={item_id}",
                )
                # не жжём баланс в цикле — отдаём управление наверх
                return sess, status_msg, "no_auth"

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
    ВСЕГДА ждёт кнопку. Без нажатия «Купить следующий» покупки НЕ продолжаются.
    При любой ошибке/таймауте → False (стоп).
    """
    buttons = [
        [
            Button.inline("✅ Купить следующий", data=b"cont:yes"),
            Button.inline("🛑 Стоп", data=b"cont:no"),
        ]
    ]

    # Новое сообщение с кнопками надёжнее, чем edit
    prompt = None
    try:
        prompt = await event.respond(text, buttons=buttons)
    except Exception as e:
        print(f"[ASK_CONTINUE] respond fail: {e}")
        try:
            prompt = await status_msg.respond(text, buttons=buttons)
        except Exception as e2:
            print(f"[ASK_CONTINUE] status respond fail: {e2}")
            try:
                prompt = await update_status(status_msg, text, buttons=buttons)
            except Exception as e3:
                print(f"[ASK_CONTINUE] total fail: {e3}")
                return False, status_msg

    if prompt is None:
        print("[ASK_CONTINUE] no prompt message → STOP")
        return False, status_msg

    key = f"{buyer_id}:{prompt.id}"
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    _pending_continue[key] = fut
    print(f"[ASK_CONTINUE] waiting key={key} timeout={CONTINUE_TIMEOUT}s")

    try:
        ok = await asyncio.wait_for(asyncio.shield(fut), timeout=CONTINUE_TIMEOUT)
        print(f"[ASK_CONTINUE] user chose ok={ok}")
        return bool(ok), prompt
    except asyncio.TimeoutError:
        print("[ASK_CONTINUE] timeout → STOP")
        try:
            await prompt.edit(
                text + f"\n\n⏱ Таймаут {CONTINUE_TIMEOUT}с — остановлено.",
                buttons=None,
            )
        except Exception:
            pass
        return False, prompt
    except Exception as e:
        print(f"[ASK_CONTINUE] wait error: {e} → STOP")
        return False, prompt
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

        if reason == "no_auth":
            phone_x = (sess or {}).get("phone") or "—"
            item_x = (sess or {}).get("item_id") or "?"
            go, status_msg = await ask_continue(
                event,
                status_msg,
                f"⚠️ Куплен item `{item_x}` (`{phone_x}`), но **нет auth_key/dc_id** в ответе API.\n"
                f"Смотри логи `[BUY OK]` / `[GET ITEM]`.\n\n"
                f"Попытка {tried}/{MAX_TRIES}. Купить следующий?",
                buyer_id=buyer_id,
            )
            if not go:
                await update_status(status_msg, "🛑 Остановлено (нет auth_key).")
                return
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

        # build session из auth_key + dc_id (как веб-маркет)
        print(
            f"[SESSION BUILD] phone={phone} dc={dc_id} "
            f"auth_len={len(auth_key) if auth_key else 0}"
        )
        session_path = TEMP_DIR / f"{phone or item_id}_{item_id}.session"
        string_session = None
        session_name = session_path.name
        try:
            string_session = authkey_to_string_session(auth_key, dc_id)
            print(f"[SESSION BUILD] StringSession ok len={len(string_session)}")
        except Exception as e:
            print(f"[SESSION BUILD] StringSession fail: {e}")
            string_session = None

        try:
            write_sqlite_session(session_path, auth_key, dc_id)
            print(f"[SESSION BUILD] SQLite ok path={session_path}")
        except Exception as e:
            print(f"[SESSION BUILD] SQLite fail: {e}")
            cleanup_session_file(session_path)
            session_path = None

        if not string_session and not session_path:
            await log_event(bot, "ERROR", phone, buyer_id, extra="session build failed")
            await archive_account(
                bot,
                "ERROR",
                phone,
                auth_key,
                dc_id,
                tg_uid,
                None,
                buyer_id,
                "session build failed",
                item_id=item_id,
            )
            go, status_msg = await ask_continue(
                event,
                status_msg,
                f"⚠️ Не собралась session для `{phone}` (item `{item_id}`).\n"
                f"Попытка {tried}/{MAX_TRIES}. Купить следующий?",
                buyer_id=buyer_id,
            )
            if not go:
                await update_status(status_msg, "🛑 Остановлено — session build fail.")
                return
            continue

        # приоритет StringSession (не зависит от файловой системы Railway)
        result = await check_mamba_with_session(
            session_path=None if string_session else session_path,
            string_session=string_session,
            start_param=start_param,
        )
        print(f"[RESULT] phone={phone} item={item_id} → {result}")

        # --- VALID: успех, стоп ---
        if result == "VALID":
            await update_status(
                status_msg,
                f"✅ **Mamba верифицирована**\n\nАккаунт: `{phone}`",
            )
            await log_event(bot, "VALID", phone, buyer_id, session_name=session_name)
            await archive_account(
                bot,
                "VALID",
                phone,
                auth_key,
                dc_id,
                tg_uid,
                session_path,
                buyer_id,
                item_id=item_id,
            )
            return

        # --- NOVALID: архив + спросить ---
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
                bot,
                "NOVALID",
                phone,
                auth_key,
                dc_id,
                tg_uid,
                session_path,
                buyer_id,
                item_id=item_id,
            )
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
                item_id=item_id,
            )
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
            bot,
            "ERROR",
            phone,
            auth_key,
            dc_id,
            tg_uid,
            session_path,
            buyer_id,
            item_id=item_id,
        )
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


@bot.on(events.CallbackQuery)
async def on_continue_callback(event):
    """Обработка кнопок продолжения покупки."""
    raw = event.data or b""
    try:
        data = raw.decode()
    except Exception:
        data = ""
    print(f"[CALLBACK] from={event.sender_id} data={data!r} msg={event.message_id}")

    if not data.startswith("cont:"):
        return

    buyer_id = event.sender_id
    msg_id = event.message_id
    key = f"{buyer_id}:{msg_id}"
    fut = _pending_continue.get(key)

    if fut is None:
        for k, f in list(_pending_continue.items()):
            if k.startswith(f"{buyer_id}:") and not f.done():
                fut = f
                key = k
                break

    ok = data == "cont:yes"
    if fut is not None and not fut.done():
        fut.set_result(ok)
        print(f"[CALLBACK] resolved key={key} ok={ok}")
    else:
        print(f"[CALLBACK] no pending future for {key}")

    try:
        await event.answer("Продолжаем..." if ok else "Стоп")
    except Exception:
        pass
    try:
        base = ""
        if event.message:
            base = event.message.message or ""
        await event.edit(
            base + ("\n\n▶️ Продолжаем..." if ok else "\n\n🛑 Остановлено."),
            buttons=None,
        )
    except Exception as e:
        print(f"[CALLBACK] edit fail: {e}")


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
