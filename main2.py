
# -*- coding: utf-8 -*-
import asyncio
import html
import logging
import os
import sqlite3
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import a2s
import requests
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from dotenv import load_dotenv

ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH, encoding="utf-8-sig")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")
DB_PATH = os.getenv("BOT_DB_PATH", "bot_data.sqlite3")

if not TELEGRAM_TOKEN or not STEAM_API_KEY:
    raise ValueError("Не найдены TELEGRAM_TOKEN или STEAM_API_KEY в .env")

RUST_APPID = "252490"
COUNTRY_MAP = {
    "RU": "Россия",
    "BY": "Беларусь",
    "UA": "Украина",
    "KZ": "Казахстан",
    "US": "США",
    "DE": "Германия",
    "FR": "Франция",
    "GB": "Великобритания",
}


def to_int(value: str | None, fallback: int) -> int:
    try:
        return int(value) if value is not None else fallback
    except Exception:
        return fallback


def parse_admin_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    result = set()
    for part in raw.split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            result.add(int(part))
    return result


DEFAULTS = {
    "required_channel": os.getenv("REQUIRED_CHANNEL", "").strip(),
    "stars_price": str(max(to_int(os.getenv("STARS_PRICE"), 1), 1)),
    "sub_days": str(max(to_int(os.getenv("SUB_DAYS"), 30), 1)),
    "free_daily_limit": str(max(to_int(os.getenv("FREE_DAILY_LIMIT"), 5), 0)),
    "ref_bonus_days": str(max(to_int(os.getenv("REF_BONUS_DAYS"), 3), 0)),
}
ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS"))

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)
BOT_USERNAME = ""


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                created_at TEXT NOT NULL,
                ref_code TEXT UNIQUE NOT NULL,
                referred_by INTEGER,
                ref_uses INTEGER NOT NULL DEFAULT 0,
                paid_until TEXT,
                paid_purchases_count INTEGER NOT NULL DEFAULT 0,
                free_checks_date TEXT,
                free_checks_used INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS payments(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                payload TEXT,
                stars_amount INTEGER NOT NULL,
                tg_payment_id TEXT UNIQUE,
                created_at TEXT NOT NULL
            );
            """
        )
        for k, v in DEFAULTS.items():
            c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))


def get_setting(key: str, fallback: str = "") -> str:
    with conn() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else fallback


def get_setting_int(key: str, fallback: int) -> int:
    return to_int(get_setting(key, str(fallback)), fallback)


def set_setting(key: str, value: str) -> None:
    with conn() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def ref_code_for(user_id: int) -> str:
    return f"r{user_id:x}"


def ensure_user(u: types.User | None) -> None:
    if not u:
        return
    with conn() as c:
        c.execute(
            """
            INSERT INTO users(user_id,username,created_at,ref_code)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
            """,
            (u.id, u.username or "", now_utc().isoformat(), ref_code_for(u.id)),
        )


def get_user(user_id: int) -> sqlite3.Row | None:
    with conn() as c:
        return c.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


def get_user_by_ref(code: str) -> sqlite3.Row | None:
    with conn() as c:
        return c.execute("SELECT * FROM users WHERE lower(ref_code)=lower(?)", (code,)).fetchone()


def apply_referral(user_id: int, code: str) -> str:
    inviter = get_user_by_ref(code)
    if not inviter:
        return "invalid"
    if int(inviter["user_id"]) == user_id:
        return "self"
    with conn() as c:
        row = c.execute("SELECT referred_by FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return "missing"
        if row["referred_by"]:
            return "exists"
        c.execute("UPDATE users SET referred_by=? WHERE user_id=?", (int(inviter["user_id"]), user_id))
    return "ok"


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def paid_until(user_id: int) -> datetime | None:
    row = get_user(user_id)
    return parse_iso(row["paid_until"]) if row else None


def is_paid(user_id: int) -> bool:
    pu = paid_until(user_id)
    return bool(pu and pu > now_utc())


def extend_paid(user_id: int, days: int) -> datetime:
    days = max(days, 1)
    cur = paid_until(user_id)
    start = cur if cur and cur > now_utc() else now_utc()
    new_dt = start + timedelta(days=days)
    with conn() as c:
        c.execute("UPDATE users SET paid_until=? WHERE user_id=?", (new_dt.isoformat(), user_id))
    return new_dt

def inc_paid_purchase(user_id: int) -> int:
    with conn() as c:
        row = c.execute("SELECT paid_purchases_count FROM users WHERE user_id=?", (user_id,)).fetchone()
        prev = int(row["paid_purchases_count"]) if row else 0
        c.execute("UPDATE users SET paid_purchases_count=? WHERE user_id=?", (prev + 1, user_id))
        return prev


def save_payment(user_id: int, payload: str, amount: int, charge_id: str | None) -> None:
    with conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO payments(user_id,payload,stars_amount,tg_payment_id,created_at) VALUES(?,?,?,?,?)",
            (user_id, payload, amount, charge_id, now_utc().isoformat()),
        )


def referred_count(user_id: int) -> int:
    with conn() as c:
        row = c.execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=?", (user_id,)).fetchone()
        return int(row["c"])


def free_usage(user_id: int, limit: int) -> tuple[int, int]:
    today = date.today().isoformat()
    with conn() as c:
        row = c.execute("SELECT free_checks_date,free_checks_used FROM users WHERE user_id=?", (user_id,)).fetchone()
        used = 0
        if row and row["free_checks_date"] == today:
            used = int(row["free_checks_used"] or 0)
        elif row:
            c.execute("UPDATE users SET free_checks_date=?, free_checks_used=0 WHERE user_id=?", (today, user_id))
        return used, max(limit - used, 0)


def consume_free(user_id: int, limit: int) -> int:
    today = date.today().isoformat()
    with conn() as c:
        row = c.execute("SELECT free_checks_date,free_checks_used FROM users WHERE user_id=?", (user_id,)).fetchone()
        used = int(row["free_checks_used"] or 0) if row and row["free_checks_date"] == today else 0
        used = min(used + 1, limit)
        c.execute("UPDATE users SET free_checks_date=?, free_checks_used=? WHERE user_id=?", (today, used, user_id))
        return max(limit - used, 0)


def admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def cmd_arg(text: str | None) -> str:
    if not text:
        return ""
    parts = text.strip().split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def kb() -> InlineKeyboardMarkup:
    price = max(get_setting_int("stars_price", 1), 1)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Купить PRO ({price} XTR)", callback_data="buy")],
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check")],
            [InlineKeyboardButton(text="👥 Рефералка", callback_data="ref")],
        ]
    )


async def subscribed(user_id: int) -> bool:
    channel = get_setting("required_channel", DEFAULTS["required_channel"]).strip()
    if not channel or channel == "-":
        return True
    try:
        m = await bot.get_chat_member(channel, user_id)
        if m.status in {"creator", "administrator", "member"}:
            return True
        return m.status == "restricted" and bool(getattr(m, "is_member", False))
    except Exception as e:
        logging.warning(f"sub check fail: {e}")
        return False


async def access(user_id: int, consume: bool = True) -> dict:
    if is_paid(user_id):
        return {"ok": True, "tier": "pro", "remaining": None, "text": ""}

    channel = get_setting("required_channel", DEFAULTS["required_channel"]).strip()
    if channel and channel != "-" and not await subscribed(user_id):
        return {
            "ok": False,
            "tier": "blocked",
            "remaining": None,
            "text": f"Подпишитесь на канал для бесплатного доступа: {html.escape(channel)}",
        }

    limit = max(get_setting_int("free_daily_limit", 5), 0)
    if limit <= 0:
        return {"ok": False, "tier": "blocked", "remaining": 0, "text": "Бесплатный доступ отключён."}

    used, rem = free_usage(user_id, limit)
    if rem <= 0:
        return {
            "ok": False,
            "tier": "blocked",
            "remaining": 0,
            "text": f"Лимит free на сегодня исчерпан ({used}/{limit}). Купите PRO.",
        }

    if consume:
        rem = consume_free(user_id, limit)

    return {"ok": True, "tier": "free", "remaining": rem, "text": ""}


async def send_invoice(chat_id: int) -> None:
    price = max(get_setting_int("stars_price", 1), 1)
    days = max(get_setting_int("sub_days", 30), 1)
    await bot.send_invoice(
        chat_id=chat_id,
        title="Rust Tracker PRO",
        description=f"Безлимитные проверки на {days} дн.",
        payload=f"stars:{days}:{price}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=f"PRO {days} дн.", amount=price)],
        start_parameter="rust_tracker_pro",
    )


async def bot_api_json(method: str, data: dict | None = None) -> dict:
    loop = asyncio.get_running_loop()
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

    def _do_request():
        response = requests.post(url, data=data or {}, timeout=15)
        return response.json()

    return await loop.run_in_executor(None, _do_request)


def star_amount_to_text(amount_obj: dict | None) -> str:
    if not isinstance(amount_obj, dict):
        return "неизвестно"
    amount = amount_obj.get("amount")
    nanos = amount_obj.get("nanostar_amount", 0)
    if amount is None:
        return "неизвестно"
    if nanos:
        return f"{amount}.{str(nanos).zfill(9)}"
    return str(amount)


async def get_star_balance_data() -> dict:
    # New aiogram versions expose this helper.
    if hasattr(bot, "get_my_star_balance"):
        result = await bot.get_my_star_balance()
        if hasattr(result, "model_dump"):
            return result.model_dump(exclude_none=True)
        if isinstance(result, dict):
            return result

    # Fallback for older aiogram: raw Bot API call.
    payload = await bot_api_json("getMyStarBalance")
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "unknown error"))
    result = payload.get("result")
    return result if isinstance(result, dict) else {}


async def get_star_transactions_data(limit: int = 5) -> list[dict]:
    safe_limit = max(1, min(limit, 10))

    if hasattr(bot, "get_star_transactions"):
        result = await bot.get_star_transactions(limit=safe_limit)
        if hasattr(result, "model_dump"):
            data = result.model_dump(exclude_none=True)
        elif isinstance(result, dict):
            data = result
        else:
            data = {}
        txs = data.get("transactions", [])
        return [tx.model_dump(exclude_none=True) if hasattr(tx, "model_dump") else tx for tx in txs]

    payload = await bot_api_json("getStarTransactions", {"limit": safe_limit})
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "unknown error"))
    result = payload.get("result", {})
    txs = result.get("transactions", []) if isinstance(result, dict) else []
    return txs if isinstance(txs, list) else []


async def bot_balance_text() -> str:
    try:
        balance = await get_star_balance_data()
    except Exception as e:
        return f"Не удалось получить баланс Stars: {e}"
    return f"Баланс бота: {star_amount_to_text(balance)} XTR"


async def starlog_text() -> str:
    try:
        txs = await get_star_transactions_data(limit=5)
    except Exception as e:
        return f"Не удалось получить логи Stars: {e}"

    if not txs:
        return "Транзакции Stars пока не найдены."

    lines = ["Последние транзакции Stars:"]
    for tx in txs:
        tx_id = tx.get("id", "?") if isinstance(tx, dict) else "?"
        amount_text = star_amount_to_text(tx.get("amount")) if isinstance(tx, dict) else "неизвестно"
        ts = tx.get("date") if isinstance(tx, dict) else None
        if isinstance(ts, int):
            dt_text = datetime.fromtimestamp(ts, timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
        else:
            dt_text = "время неизвестно"
        source = tx.get("source") if isinstance(tx, dict) else None
        source_type = source.get("type", "?") if isinstance(source, dict) else "?"
        lines.append(f"{dt_text} | {amount_text} XTR | source={source_type} | id={tx_id}")

    return "\n".join(lines)


def grant_test_subscription_for_user(user_id: int, days: int, note: str = "manual_test") -> datetime:
    """Marks local DB user as having paid subscription and logs local payment."""
    with conn() as c:
        c.execute(
            """
            INSERT OR IGNORE INTO users(user_id, username, created_at, ref_code)
            VALUES(?, ?, ?, ?)
            """,
            (user_id, "", now_utc().isoformat(), ref_code_for(user_id)),
        )
    new_until = extend_paid(user_id, max(days, 1))
    save_payment(user_id, note, 1, f"local-{user_id}-{int(now_utc().timestamp())}")
    return new_until


def country_name(code: str | None) -> str:
    if not code:
        return "Неизвестно"
    return COUNTRY_MAP.get(code.upper(), code.upper())


def split_endpoint(endpoint: str | None) -> tuple[str, str]:
    if not endpoint or ":" not in endpoint:
        return "", ""
    ip, port = endpoint.rsplit(":", 1)
    return ip.strip(), port.strip()


def parse_input(text: str) -> tuple[str, str]:
    text = text.strip()
    if text.isdigit() and len(text) == 17:
        return text, "steamid"
    if "/profiles/" in text:
        sid = text.split("/profiles/")[-1].split("/")[0].split("?")[0]
        if sid.isdigit() and len(sid) == 17:
            return sid, "steamid"
    if "/id/" in text:
        vanity = text.split("/id/")[-1].split("/")[0].split("?")[0]
        return vanity, "vanity"
    if not text.startswith("http") and len(text) < 100:
        return text, "vanity"
    return text, "unknown"

async def resolve_vanity(vanity: str) -> str | None:
    url = "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/"
    params = {"key": STEAM_API_KEY, "vanityurl": vanity}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=10).json())
        resp = data.get("response", {})
        return resp.get("steamid") if resp.get("success") == 1 else None
    except Exception:
        return None


async def player_info(steamid: str) -> dict | None:
    url = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/"
    params = {"key": STEAM_API_KEY, "steamids": steamid}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=10).json())
        players = data.get("response", {}).get("players", [])
        return players[0] if players else None
    except Exception:
        return None


async def rust_hours(steamid: str) -> float | None:
    url = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/"
    params = {
        "key": STEAM_API_KEY,
        "steamid": steamid,
        "include_played_free_games": 1,
        "format": "json",
        "appids_filter[0]": RUST_APPID,
    }
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=10).json())
        games = data.get("response", {}).get("games", [])
        if not games:
            return None
        mins = games[0].get("playtime_forever")
        return (float(mins) / 60.0) if mins is not None else None
    except Exception:
        return None


async def bm_server_name(ip: str, port: str) -> str:
    loop = asyncio.get_running_loop()
    try:
        url = "https://api.battlemetrics.com/servers"
        params = {"filter[search]": f"{ip}:{port}", "filter[game]": "rust"}
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=8).json())
        for item in data.get("data", []):
            a = item.get("attributes", {})
            if a.get("ip") == ip and str(a.get("port")) == str(port):
                return a.get("name", "")
        return ""
    except Exception:
        return ""


async def a2s_name(endpoint: str) -> str:
    if not endpoint or endpoint == "0.0.0.0:0" or ":" not in endpoint:
        return ""
    try:
        ip, port = endpoint.rsplit(":", 1)
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, lambda: a2s.info((ip, int(port)), timeout=4.0))
        return info.server_name.strip() if info and info.server_name else ""
    except Exception:
        return ""


async def apply_ref_bonus(invited_user_id: int) -> str:
    row = get_user(invited_user_id)
    if not row or not row["referred_by"]:
        return ""
    bonus = max(get_setting_int("ref_bonus_days", 3), 0)
    if bonus <= 0:
        return ""
    inviter = int(row["referred_by"])
    new_until = extend_paid(inviter, bonus)
    with conn() as c:
        c.execute("UPDATE users SET ref_uses = ref_uses + 1 WHERE user_id=?", (inviter,))
    try:
        await bot.send_message(
            inviter,
            f"По вашей рефералке прошла первая оплата. Начислено +{bonus} дн. PRO до {new_until.strftime('%d.%m.%Y %H:%M UTC')}",
        )
    except Exception:
        pass
    return f"\nРеферал-пригласивший получил +{bonus} дн."


async def status_text(user_id: int) -> str:
    pu = paid_until(user_id)
    paid_txt = pu.strftime("%d.%m.%Y %H:%M UTC") if pu and pu > now_utc() else "нет"
    limit = max(get_setting_int("free_daily_limit", 5), 0)
    used, rem = free_usage(user_id, limit)
    channel = get_setting("required_channel", DEFAULTS["required_channel"]).strip()
    if not channel or channel == "-":
        ch_txt = "не требуется"
    else:
        ch_txt = f"{channel} ({'подписан' if await subscribed(user_id) else 'не подписан'})"
    return (
        "Ваш доступ:\n"
        f"PRO до: {paid_txt}\n"
        f"Free: {used}/{limit} (осталось {rem})\n"
        f"Канал: {html.escape(ch_txt)}\n"
        f"Рефералов: {referred_count(user_id)}"
    )


async def ref_text(user_id: int) -> str:
    global BOT_USERNAME
    row = get_user(user_id)
    if not row:
        return "Нет данных рефералки"
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username or ""
    code = row["ref_code"]
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{code}" if BOT_USERNAME else f"/start ref_{code}"
    bonus = max(get_setting_int("ref_bonus_days", 3), 0)
    return (
        "Реферальная программа:\n"
        f"Ссылка: {link}\n"
        f"Код: {code}\n"
        f"Приглашено: {referred_count(user_id)}\n"
        f"Бонус за первую оплату друга: +{bonus} дн."
    )

@dp.message(Command(commands=["start"]))
async def cmd_start(message: types.Message):
    ensure_user(message.from_user)
    arg = cmd_arg(message.text)
    note = ""
    if arg.startswith("ref_"):
        st = apply_referral(message.from_user.id, arg[4:].strip().lower())
        note = {
            "ok": "Реферальный код принят.",
            "self": "Нельзя использовать свой код.",
            "exists": "Реферал уже привязан.",
            "invalid": "Реферальный код не найден.",
        }.get(st, "")

    text = (
        "Бот обновлён: Free за подписку на канал + PRO за Telegram Stars.\n\n"
        "Команды:\n"
        "/buy - купить PRO\n"
        "/status - статус доступа\n"
        "/myid - ваш Telegram ID\n"
        "/ref - рефералка\n"
        "/help - помощь\n\n"
        "Отправь SteamID64 или ссылку на Steam-профиль."
    )
    if note:
        text += f"\n\n{note}"
    await message.answer(text, reply_markup=kb())
    await message.answer(await status_text(message.from_user.id))


@dp.message(Command(commands=["help"]))
async def cmd_help(message: types.Message):
    ensure_user(message.from_user)
    text = (
        "Free режим: нужен канал + лимит в день.\n"
        "PRO режим: оплата в Stars, без лимитов на срок подписки.\n"
        "Рефералка: бонусные дни за первую оплату приглашённого.\n\n"
        "Команды: /buy /status /myid /ref"
    )
    if admin(message.from_user.id):
        text += (
            "\n\nАдмин:\n"
            "/cfg\n/setprice <stars>\n/setdays <days>\n/setfree <limit>\n"
            "/setchannel <@channel или ->\n/setrefbonus <days>\n/adminstats\n"
            "/botbalance\n/starlog\n/grantme\n/withdrawhelp"
        )
    await message.answer(text, reply_markup=kb())


@dp.message(Command(commands=["buy"]))
async def cmd_buy(message: types.Message):
    ensure_user(message.from_user)
    if is_paid(message.from_user.id):
        pu = paid_until(message.from_user.id)
        await message.answer(f"У вас уже активен PRO до {pu.strftime('%d.%m.%Y %H:%M UTC')}")
        return
    await send_invoice(message.chat.id)


@dp.message(Command(commands=["status"]))
async def cmd_status(message: types.Message):
    ensure_user(message.from_user)
    await message.answer(await status_text(message.from_user.id), reply_markup=kb())


@dp.message(Command(commands=["myid"]))
async def cmd_myid(message: types.Message):
    await message.answer(f"Ваш Telegram user_id: {message.from_user.id}")


@dp.message(Command(commands=["ref"]))
async def cmd_ref(message: types.Message):
    ensure_user(message.from_user)
    await message.answer(await ref_text(message.from_user.id))


@dp.message(Command(commands=["botbalance"]))
async def cmd_botbalance(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    await message.answer(await bot_balance_text())


@dp.message(Command(commands=["starlog"]))
async def cmd_starlog(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    await message.answer(await starlog_text())


@dp.message(Command(commands=["grantme"]))
async def cmd_grantme(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    days = max(get_setting_int("sub_days", 30), 1)
    new_until = grant_test_subscription_for_user(message.from_user.id, days, note="manual_test_grant")
    await message.answer(
        "Тестовая локальная подписка проставлена.\n"
        f"До: {new_until.strftime('%d.%m.%Y %H:%M UTC')}\n"
        "В таблицу payments добавлена локальная запись на 1 XTR."
    )


@dp.message(Command(commands=["withdrawhelp"]))
async def cmd_withdrawhelp(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    await message.answer(
        "Как вывести Stars:\n"
        "1) Открой BotFather и карточку бота.\n"
        "2) Перейди в раздел Stars/Monetization и проверь доступность вывода.\n"
        "3) Привяжи кошелек/аккаунт выплат (обычно через Fragment, если доступно в твоем регионе).\n"
        "4) Оформи вывод из панели бота.\n"
        "5) Проверь итоговые комиссии и минимальный порог вывода перед подтверждением."
    )


@dp.message(Command(commands=["cfg"]))
async def cmd_cfg(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    await message.answer(
        "Текущие настройки:\n"
        f"Канал: {get_setting('required_channel', DEFAULTS['required_channel']) or 'не задан'}\n"
        f"Цена: {get_setting_int('stars_price', 1)} XTR\n"
        f"Срок: {get_setting_int('sub_days', 30)} дн.\n"
        f"Free лимит: {get_setting_int('free_daily_limit', 5)}\n"
        f"Реф бонус: {get_setting_int('ref_bonus_days', 3)} дн.\n"
        f"ADMIN_IDS: {', '.join(str(x) for x in sorted(ADMIN_IDS)) or 'не заданы'}"
    )


@dp.message(Command(commands=["setprice"]))
async def cmd_setprice(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    arg = cmd_arg(message.text)
    if not arg.isdigit() or int(arg) <= 0:
        await message.answer("Формат: /setprice <число> > 0")
        return
    set_setting("stars_price", str(int(arg)))
    await message.answer(f"Цена обновлена: {int(arg)} XTR")


@dp.message(Command(commands=["setdays"]))
async def cmd_setdays(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    arg = cmd_arg(message.text)
    if not arg.isdigit() or int(arg) <= 0:
        await message.answer("Формат: /setdays <число> > 0")
        return
    set_setting("sub_days", str(int(arg)))
    await message.answer(f"Срок подписки: {int(arg)} дн.")


@dp.message(Command(commands=["setfree"]))
async def cmd_setfree(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    arg = cmd_arg(message.text)
    if not arg.isdigit() or int(arg) < 0:
        await message.answer("Формат: /setfree <число> >= 0")
        return
    set_setting("free_daily_limit", str(int(arg)))
    await message.answer(f"Free лимит: {int(arg)}")


@dp.message(Command(commands=["setchannel"]))
async def cmd_setchannel(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    arg = cmd_arg(message.text)
    if not arg:
        await message.answer("Формат: /setchannel <@channel или ->")
        return
    set_setting("required_channel", arg.strip())
    await message.answer(f"Канал проверки: {html.escape(arg.strip())}")


@dp.message(Command(commands=["setrefbonus"]))
async def cmd_setrefbonus(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return
    arg = cmd_arg(message.text)
    if not arg.isdigit() or int(arg) < 0:
        await message.answer("Формат: /setrefbonus <число> >= 0")
        return
    set_setting("ref_bonus_days", str(int(arg)))
    await message.answer(f"Реф бонус: +{int(arg)} дн.")

@dp.message(Command(commands=["adminstats"]))
async def cmd_adminstats(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа")
        return

    with conn() as c:
        now_iso = now_utc().isoformat()
        users = int(c.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"])
        paid = int(c.execute("SELECT COUNT(*) AS c FROM users WHERE paid_until > ?", (now_iso,)).fetchone()["c"])
        pay_cnt = int(c.execute("SELECT COUNT(*) AS c FROM payments").fetchone()["c"])
        stars = int(c.execute("SELECT COALESCE(SUM(stars_amount),0) AS s FROM payments").fetchone()["s"])
        refs = int(c.execute("SELECT COUNT(*) AS c FROM users WHERE referred_by IS NOT NULL").fetchone()["c"])
        top = c.execute(
            """
            SELECT i.ref_code AS code, COUNT(u.user_id) AS uses
            FROM users u JOIN users i ON i.user_id=u.referred_by
            GROUP BY i.user_id
            ORDER BY uses DESC
            LIMIT 10
            """
        ).fetchall()

    lines = [
        f"Количество пользователей бота: {users}",
        "---------------------------",
        f"Активных PRO: {paid}",
        f"Оплат Stars: {pay_cnt}",
        f"Всего Stars: {stars}",
        f"Пришли по рефералке: {refs}",
        "---------------------------",
        "Топ реф-кодов:",
    ]
    if top:
        for row in top:
            lines.append(f"Код: {row['code']} | Использований: {row['uses']}")
    else:
        lines.append("Пока нет данных")
    await message.answer("\n".join(lines))


@dp.callback_query(F.data == "buy")
async def cb_buy(callback: types.CallbackQuery):
    ensure_user(callback.from_user)
    if is_paid(callback.from_user.id):
        pu = paid_until(callback.from_user.id)
        await callback.answer(f"PRO уже активен до {pu.strftime('%d.%m.%Y %H:%M UTC')}", show_alert=True)
        return
    await send_invoice(callback.message.chat.id)
    await callback.answer("Счёт отправлен")


@dp.callback_query(F.data == "check")
async def cb_check(callback: types.CallbackQuery):
    ensure_user(callback.from_user)
    channel = get_setting("required_channel", DEFAULTS["required_channel"]).strip()
    if not channel or channel == "-":
        await callback.answer("Проверка канала отключена", show_alert=True)
        return
    await callback.answer("Подписка подтверждена ✅" if await subscribed(callback.from_user.id) else "Подписка не найдена ❌", show_alert=True)


@dp.callback_query(F.data == "ref")
async def cb_ref(callback: types.CallbackQuery):
    ensure_user(callback.from_user)
    await callback.message.answer(await ref_text(callback.from_user.id))
    await callback.answer()


@dp.pre_checkout_query()
async def on_pre_checkout(q: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(q.id, ok=True)


@dp.message(F.successful_payment)
async def on_success(message: types.Message):
    ensure_user(message.from_user)
    pay = message.successful_payment
    uid = message.from_user.id
    days = max(get_setting_int("sub_days", 30), 1)
    until = extend_paid(uid, days)
    prev = inc_paid_purchase(uid)
    save_payment(uid, pay.invoice_payload, pay.total_amount, pay.telegram_payment_charge_id)

    bonus_note = ""
    if prev == 0:
        bonus_note = await apply_ref_bonus(uid)

    await message.answer(
        "Оплата получена.\n"
        f"PRO активен до: {until.strftime('%d.%m.%Y %H:%M UTC')}\n"
        f"Списано: {pay.total_amount} XTR"
        f"{bonus_note}"
    )


@dp.message()
async def handle(message: types.Message):
    if not message.from_user:
        return
    ensure_user(message.from_user)
    text = (message.text or "").strip()
    if not text:
        return
    if text.startswith("/"):
        await message.answer("Неизвестная команда. Используйте /help")
        return

    gate = await access(message.from_user.id, consume=True)
    if not gate["ok"]:
        await message.answer(gate["text"], reply_markup=kb())
        return

    value, inp_type = parse_input(text)
    await bot.send_chat_action(message.chat.id, "typing")

    if inp_type == "vanity":
        await message.answer("Ищу SteamID...")
        steamid = await resolve_vanity(value)
        if not steamid:
            await message.answer("Не удалось найти профиль")
            return
    else:
        steamid = value

    player = await player_info(steamid)
    if not player:
        await message.answer("Не удалось получить данные профиля")
        return

    name = player.get("personaname", "—")
    profile_url = player.get("profileurl", "—")
    avatar = player.get("avatarfull")
    status_code = player.get("personastate", 0)
    timecreated = player.get("timecreated")
    loc_country = player.get("loccountrycode")
    gameid = player.get("gameid")
    game_name = player.get("gameextrainfo", "Не в игре") if gameid else "Не в игре"
    gameserverip = player.get("gameserverip", "0.0.0.0:0")

    country = country_name(loc_country)
    status = "В сети" if status_code != 0 else "Не в сети"
    hours = await rust_hours(steamid)
    hours_text = f"{hours:.2f}" if hours is not None else "скрыто"

    server_line = "Сервер: не найден"
    if str(gameid) == RUST_APPID:
        ip, port = split_endpoint(gameserverip)
        srv_name = ""
        if ip and port:
            srv_name = await bm_server_name(ip, port)
        if not srv_name and gameserverip:
            srv_name = await a2s_name(gameserverip)
        if srv_name and gameserverip and gameserverip != "0.0.0.0:0":
            server_line = f"Сервер: {html.escape(srv_name)} | <code>connect {html.escape(gameserverip)}</code>"
        elif srv_name:
            server_line = f"Сервер: {html.escape(srv_name)}"
        elif gameserverip and gameserverip != "0.0.0.0:0":
            server_line = f"Сервер: <code>connect {html.escape(gameserverip)}</code>"

    created = datetime.fromtimestamp(timecreated).strftime("%d.%m.%Y") if timecreated else "—"
    out = (
        f"Имя: {html.escape(name)}\n"
        f"Steam_id: {html.escape(steamid)} ({html.escape(profile_url)})\n"
        f"Страна: {html.escape(country)}\n"
        f"Статус: {html.escape(status)}\n"
        f"Игра: {html.escape(game_name)}\n"
        f"{server_line}\n"
        f"Аккаунт создан: {html.escape(created)}\n"
        f"Часов в Rust: {html.escape(hours_text)}"
    )

    if avatar:
        await message.answer_photo(photo=avatar, caption=out, parse_mode="HTML")
    else:
        await message.answer(out, parse_mode="HTML")

    if gate["tier"] == "free":
        await message.answer(f"Осталось бесплатных проверок сегодня: {gate['remaining']}")


async def main():
    global BOT_USERNAME
    init_db()
    set_setting("stars_price", "1")

    # For testing: mark first admin as locally having the current paid subscription.
    if ADMIN_IDS:
        test_days = max(get_setting_int("sub_days", 30), 1)
        test_admin_id = sorted(ADMIN_IDS)[0]
        until = extend_paid(test_admin_id, test_days)
        save_payment(test_admin_id, "startup_local_mark", 1, f"local-startup-{test_admin_id}")
        logging.info(f"Startup test mark applied for admin {test_admin_id}, until={until.isoformat()}")

    me = await bot.get_me()
    BOT_USERNAME = me.username or ""
    print("Bot started: free + stars + referrals")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
