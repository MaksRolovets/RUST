# -*- coding: utf-8 -*-
import asyncio
import html
import logging
import os
import re
import sqlite3
from difflib import SequenceMatcher
from datetime import datetime
from collections import defaultdict

import requests
from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    ReplyKeyboardMarkup,
    LabeledPrice,
)
from dotenv import load_dotenv
from pathlib import Path

# ====================== ENV ======================
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH, encoding="utf-8-sig")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BM_TOKEN = os.getenv("BATTLEMETRICS_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

if not TELEGRAM_TOKEN or not BM_TOKEN:
    raise ValueError("❌ Нет токена")

BM_HEADERS = {"Authorization": f"Bearer {BM_TOKEN}"}
SEARCH_PAGE_SIZE = 8
MIN_NICKNAME_SIMILARITY = 0.8
RUST_APPID = "252490"

MENU_NICK = "🔎 Поиск по никнейму"
MENU_STEAM = "🆔 Поиск по Steam ID"
MENU_DONATE = "⭐ Пожертвование"
MENU_BACK = "⬅️ Назад"


def parse_admin_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    result = set()
    for part in raw.split(","):
        p = part.strip()
        if p.lstrip("-").isdigit():
            result.add(int(p))
    return result


ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS"))
user_modes: dict[int, str] = {}

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)

# ====================== TRACKING ======================
DB_PATH = Path(__file__).with_name("bot_data.sqlite3")
trackings = defaultdict(dict)

def init_tracking_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_players (
                user_id INTEGER NOT NULL,
                player_id TEXT NOT NULL,
                name TEXT,
                steam_id TEXT,
                last_stop TEXT,
                last_server_id TEXT,
                PRIMARY KEY (user_id, player_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_access (
                user_id INTEGER PRIMARY KEY,
                total_queries INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS donations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                payload TEXT,
                charge_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                user_id INTEGER PRIMARY KEY,
                ref_code TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stats_counters (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO bot_settings(key, value) VALUES ('required_channel', ?)",
            (os.getenv("REQUIRED_CHANNEL", "").strip(),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO bot_settings(key, value) VALUES ('subscription_enabled', '1')"
        )
        for key in (
            "search_user_clicks",
            "details_clicks",
            "inventory_clicks",
            "tracking_clicks",
            "friends_clicks",
        ):
            conn.execute(
                "INSERT OR IGNORE INTO stats_counters(key, value) VALUES (?, 0)",
                (key,),
            )

        # ---- DB migrations for old schemas ----
        user_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(users)").fetchall()
            if row and len(row) > 1
        }
        if "first_seen" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN first_seen TEXT")
        if "last_seen" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")

        # Backfill timestamps for existing users from legacy created_at when possible.
        user_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(users)").fetchall()
            if row and len(row) > 1
        }
        if "created_at" in user_cols:
            conn.execute(
                """
                UPDATE users
                SET first_seen = COALESCE(first_seen, created_at),
                    last_seen = COALESCE(last_seen, created_at)
                """
            )
        now_ts = now_iso()
        conn.execute(
            """
            UPDATE users
            SET first_seen = COALESCE(first_seen, ?),
                last_seen = COALESCE(last_seen, ?)
            """,
            (now_ts, now_ts),
        )

        conn.commit()


def load_trackings():
    global trackings
    trackings = defaultdict(dict)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT user_id, player_id, name, steam_id, last_stop, last_server_id FROM tracked_players"
            ).fetchall()
        for user_id, player_id, name, steam_id, last_stop, last_server_id in rows:
            trackings[int(user_id)][str(player_id)] = {
                "name": name or "Unknown",
                "steam_id": steam_id or None,
                "last_stop": last_stop,
                "last_server_id": last_server_id,
            }
        print(f"✅ Загружено отслеживаний: {sum(len(p) for p in trackings.values())}")
    except Exception as e:
        logging.error(f"Load tracking error: {e}")


def save_trackings():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM tracked_players")
            for user_id, players in trackings.items():
                for player_id, info in players.items():
                    conn.execute(
                        """
                        INSERT INTO tracked_players (user_id, player_id, name, steam_id, last_stop, last_server_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(user_id),
                            str(player_id),
                            info.get("name"),
                            info.get("steam_id"),
                            info.get("last_stop"),
                            info.get("last_server_id"),
                        ),
                    )
            conn.commit()
    except Exception as e:
        logging.error(f"Save tracking error: {e}")


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def touch_user(user_id: int):
    ts = now_iso()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, first_seen, last_seen)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen
            """,
            (int(user_id), ts, ts),
        )
        conn.commit()


def normalize_ref_code(raw: str) -> str:
    code = (raw or "").strip()
    if not code:
        return ""
    code = code[:64]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", code):
        return ""
    return code


def attach_referral_if_missing(user_id: int, ref_code: str):
    code = normalize_ref_code(ref_code)
    if not code:
        return
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute(
            "SELECT 1 FROM referrals WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO referrals(user_id, ref_code, created_at) VALUES (?, ?, ?)",
                (int(user_id), code, now_iso()),
            )
            conn.commit()


def increment_counter(key: str, delta: int = 1):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO stats_counters(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = value + excluded.value
            """,
            (key, int(delta)),
        )
        conn.commit()


def get_counter(key: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM stats_counters WHERE key=?", (key,)).fetchone()
    return int(row[0]) if row else 0


def get_users_count() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
    return int(row[0]) if row else 0


def get_referral_stats() -> list[tuple[str, int]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT ref_code, COUNT(*) as cnt
            FROM referrals
            GROUP BY ref_code
            ORDER BY cnt DESC, ref_code ASC
            """
        ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def get_referral_users_count() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COUNT(*) FROM referrals").fetchone()
    return int(row[0]) if row else 0


def get_tracking_stats() -> tuple[int, int, int]:
    with sqlite3.connect(DB_PATH) as conn:
        total_row = conn.execute("SELECT COUNT(*) FROM tracked_players").fetchone()
        steam_row = conn.execute(
            "SELECT COUNT(*) FROM tracked_players WHERE steam_id IS NOT NULL AND steam_id != ''"
        ).fetchone()
    total = int(total_row[0]) if total_row else 0
    steam = int(steam_row[0]) if steam_row else 0
    nick = max(0, total - steam)
    return total, steam, nick


def build_settings_report_text() -> str:
    users_count = get_users_count()
    referral_stats = get_referral_stats()
    referral_users = get_referral_users_count()
    self_invited = max(0, users_count - referral_users)
    tracking_total, tracking_steam, tracking_nick = get_tracking_stats()

    lines = [
        f"Количество пользователей бота: {users_count}",
        "---------------------------",
        "Статистика по реферальным кодам:",
    ]
    if referral_stats:
        for code, cnt in referral_stats:
            lines.append(f"Код: {code} | Использований: {cnt}")
    else:
        lines.append("Код: (нет данных) | Использований: 0")
    lines.append(f"Люди сами пригласили: {self_invited}")
    lines.append("---------------------------")
    lines.append(f"Трекингов всего: {tracking_total}")
    lines.append(f"Трекинг steam_id: {tracking_steam}")
    lines.append(f"Трекинг ников: {tracking_nick}")
    lines.append(f"Нажатия поисков пользователей: {get_counter('search_user_clicks')}")
    lines.append(f"Нажатия на подробнее: {get_counter('details_clicks')}")
    lines.append(f"Нажатия поиск инвентаря: {get_counter('inventory_clicks')}")
    lines.append(f"Нажатия на трекинг: {get_counter('tracking_clicks')}")
    lines.append(f"Нажатия на друзей: {get_counter('friends_clicks')}")
    return "\n".join(lines)


def admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_setting(key: str, fallback: str = "") -> str:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM bot_settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else fallback


def set_setting(key: str, value: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO bot_settings(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        conn.commit()


def ensure_user_access(user_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR IGNORE INTO user_access(user_id,total_queries) VALUES (?,0)", (user_id,))
        conn.commit()


def get_total_queries(user_id: int) -> int:
    ensure_user_access(user_id)
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT total_queries FROM user_access WHERE user_id=?", (user_id,)).fetchone()
    return int(row[0]) if row else 0


def increment_total_queries(user_id: int):
    ensure_user_access(user_id)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE user_access SET total_queries = total_queries + 1 WHERE user_id=?", (user_id,))
        conn.commit()


def save_donation(user_id: int, amount: int, payload: str | None, charge_id: str | None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO donations(user_id, amount, payload, charge_id, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, amount, payload or "", charge_id or "", now_iso()),
        )
        conn.commit()


def total_local_donations() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COALESCE(SUM(amount),0) FROM donations").fetchone()
    return int(row[0]) if row else 0

# ====================== API ======================

async def bm_request(url: str, params=None, timeout=20, retries=2):
    for attempt in range(retries + 1):
        try:
            loop = asyncio.get_running_loop()
            def do_request():
                r = requests.get(url, headers=BM_HEADERS, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json()
            return await loop.run_in_executor(None, do_request)
        except requests.exceptions.ReadTimeout:
            if attempt < retries:
                await asyncio.sleep(1.5)
                continue
            logging.error(f"❌ ReadTimeout после {retries} попыток: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            # BattleMetrics may return temporary 5xx, retry with backoff.
            if status_code and status_code >= 500 and attempt < retries:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            logging.error(f"API error {url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            logging.error(f"API request error {url}: {e}")
            raise


def sort_mode_to_token(sort_mode: str | None) -> str:
    if sort_mode == "-lastSeen":
        return "lastSeenDesc"
    return "none"


def sort_token_to_mode(token: str | None) -> str | None:
    if token == "lastSeenDesc":
        return "-lastSeen"
    return None


async def bm_search_players_sorted(nickname: str, page: int = 1, sort_mode: str | None = "-lastSeen"):
    url = "https://api.battlemetrics.com/players"
    offset = (page - 1) * SEARCH_PAGE_SIZE
    params = {
        "filter[search]": nickname,
        "page[size]": SEARCH_PAGE_SIZE,
        "page[offset]": offset,
    }
    if sort_mode:
        params["sort"] = sort_mode

    prepared_url = requests.Request("GET", url, params=params).prepare().url
    print(
        f"[LOG] bm_search_players_sorted -> nickname='{nickname}', page={page}, "
        f"offset={offset}, sort={sort_mode}, url={prepared_url}"
    )
    return await bm_request(url, params=params, timeout=18)


async def bm_search_players_with_variants(
    nickname: str,
    page: int = 1,
    forced_sort: str | None = None,
):
    if forced_sort is not None:
        try:
            data = await bm_search_players_sorted(nickname, page=page, sort_mode=forced_sort)
            return data, forced_sort
        except requests.exceptions.HTTPError as e:
            logging.warning(f"Forced sort failed sort={forced_sort}: {e}. Falling back to default search.")

    nickname_stripped = nickname.strip()
    has_spaces = " " in nickname_stripped
    preferred_sort = None if has_spaces else "-lastSeen"
    print(f"[LOG SEARCH DEBUG] preferred sort for '{nickname}' -> {preferred_sort}")
    try:
        data = await bm_search_players_sorted(nickname, page=page, sort_mode=preferred_sort)
    except requests.exceptions.HTTPError as e:
        fallback_sort = "-lastSeen" if preferred_sort is None else None
        logging.warning(
            f"Preferred sort failed sort={preferred_sort}: {e}. Fallback sort={fallback_sort}"
        )
        data = await bm_search_players_sorted(nickname, page=page, sort_mode=fallback_sort)
        preferred_sort = fallback_sort

    players = data.get("data", []) or []
    names = [(p.get("attributes", {}) or {}).get("name", "") for p in players]
    ids = [str((p or {}).get("id", "")) for p in players]
    exact_count = sum(1 for n in names if n == nickname)
    ci_exact_count = sum(1 for n in names if n.lower() == nickname.lower())
    top_debug = [f"{ids[i]}:{names[i]}" for i in range(min(len(names), 8))]
    links = data.get("links", {}) or {}
    print(
        f"[LOG SEARCH DEBUG] selected sort={preferred_sort} count={len(players)} "
        f"exact={exact_count} ci_exact={ci_exact_count} "
        f"next={bool(links.get('next'))} top={top_debug}"
    )
    return data, preferred_sort


async def bm_get_player(player_id: str):
    url = f"https://api.battlemetrics.com/players/{player_id}"
    return await bm_request(url, timeout=15)


async def bm_get_sessions(player_id: str, include_server: bool = False):
    url = f"https://api.battlemetrics.com/players/{player_id}/relationships/sessions"
    params = {"page[size]": 6}
    if include_server:
        params["include"] = "server"
    try:
        return await bm_request(url, params=params, timeout=25, retries=3)
    except Exception as e:
        logging.warning(f"Sessions unavailable for player {player_id}: {e}")
        return {"data": []}


async def bm_get_server(server_id: str):
    url = f"https://api.battlemetrics.com/servers/{server_id}"
    data = await bm_request(url, timeout=12)
    attr = data.get("data", {}).get("attributes", {})
    return (
        attr.get("name", "Unknown"),
        attr.get("players", 0),
        attr.get("maxPlayers", 0),
        attr.get("ip"),
        attr.get("port")
    )


# ====================== UTILS ======================

def format_time(ts):
    if not ts:
        return "—"
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone().strftime("%d.%m.%Y %H:%M")


def escape_html(value) -> str:
    return html.escape(str(value), quote=False)


def extract_steam_id(player_data: dict):
    attributes = player_data.get("data", {}).get("attributes", {})
    identifiers = attributes.get("identifiers") or []
    for identifier in identifiers:
        value = str(identifier)
        if value.isdigit() and len(value) == 17:
            return value
        match = re.search(r"(?:steamid|steamID|steam):?(\d{17})", value)
        if match:
            return match.group(1)
        any_digits = re.search(r"\b(\d{17})\b", value)
        if any_digits:
            return any_digits.group(1)
    return None


def build_tracking_text(
    name: str,
    steam_id: str | None,
    is_online: bool,
    server_name: str,
):
    status_text = "🟢 В сети 🟢" if is_online else "🔴 Не в сети 🔴"
    safe_name = escape_html(name)
    safe_server = escape_html(server_name)
    if steam_id:
        profile_url = f"https://steamcommunity.com/profiles/{steam_id}/"
        safe_url = escape_html(profile_url)
        steam_line = f"🆔 Steam_id: {steam_id} (<a href=\"{safe_url}\">{safe_url}</a>)"
    else:
        steam_line = "🆔 Steam_id: не найден"

    return (
        f"👤 Имя: {safe_name}\n"
        f"{steam_line}\n"
        f"💬 Статус: {status_text}\n"
        f"🎮 Игра: Rust\n"
        f"🌐 Сервер: {safe_server}"
    )


def tracking_controls_kb(player_id: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить отслеживание", callback_data=f"bm_untrack:{player_id}")],
            [InlineKeyboardButton(text="🔎 Подробнее", callback_data=f"bm_profile:{player_id}")],
        ]
    )


def main_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_NICK), KeyboardButton(text=MENU_STEAM)],
            [KeyboardButton(text=MENU_DONATE), KeyboardButton(text=MENU_BACK)],
        ],
        resize_keyboard=True,
    )


def main_menu_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=MENU_NICK, callback_data="menu_mode:nickname")],
            [InlineKeyboardButton(text=MENU_STEAM, callback_data="menu_mode:steam")],
            [InlineKeyboardButton(text=MENU_DONATE, callback_data="menu_mode:donate")],
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")],
        ]
    )


def normalize_channel_ref(channel: str) -> str:
    ch = (channel or "").strip()
    if not ch:
        return ""
    if ch == "-":
        return "-"
    ch = ch.replace("https://", "").replace("http://", "")
    if ch.startswith("t.me/"):
        ch = ch[len("t.me/"):]
    ch = ch.strip("/")
    if ch.lstrip("-").isdigit():
        return ch
    if not ch.startswith("@"):
        ch = f"@{ch}"
    return ch


def parse_steam_input(text: str) -> tuple[str, str]:
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


def is_probable_steam_input(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if t.isdigit() and len(t) == 17:
        return True
    low = t.lower()
    if "steamcommunity.com/profiles/" in low or "steamcommunity.com/id/" in low:
        return True
    # Vanity in Steam is typically latin/num/_/.- without spaces.
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{2,64}", t))


async def resolve_vanity(vanity: str) -> str | None:
    if not STEAM_API_KEY:
        return None
    url = "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/"
    params = {"key": STEAM_API_KEY, "vanityurl": vanity}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=10).json())
        resp = data.get("response", {})
        return resp.get("steamid") if resp.get("success") == 1 else None
    except Exception:
        return None


async def steam_player_info(steamid: str) -> dict | None:
    if not STEAM_API_KEY:
        return None
    url = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/"
    params = {"key": STEAM_API_KEY, "steamids": steamid}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=12).json())
        players = data.get("response", {}).get("players", [])
        return players[0] if players else None
    except Exception:
        return None


async def rust_hours(steamid: str) -> float | None:
    if not STEAM_API_KEY:
        return None
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
        data = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=12).json())
        games = data.get("response", {}).get("games", [])
        if not games:
            return None
        mins = games[0].get("playtime_forever")
        return (float(mins) / 60.0) if mins is not None else None
    except Exception:
        return None


async def get_star_balance_data() -> dict | None:
    try:
        if hasattr(bot, "get_my_star_balance"):
            result = await bot.get_my_star_balance()
            if hasattr(result, "model_dump"):
                return result.model_dump(exclude_none=True)
            if isinstance(result, dict):
                return result
    except Exception as e:
        logging.warning(f"get_my_star_balance failed: {e}")

    # Fallback for aiogram versions without get_my_star_balance.
    try:
        loop = asyncio.get_running_loop()

        def do_request():
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMyStarBalance"
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            payload = r.json()
            if payload.get("ok"):
                return payload.get("result")
            return None

        return await loop.run_in_executor(None, do_request)
    except Exception as e:
        logging.warning(f"HTTP getMyStarBalance fallback failed: {e}")
        return None
    return None


def extract_star_amount(balance_data: dict | None) -> str:
    if not isinstance(balance_data, dict):
        return "неизвестно"
    amount_data = balance_data.get("amount") or balance_data.get("star_amount") or {}
    if isinstance(amount_data, dict):
        amount = amount_data.get("amount")
        nanos = amount_data.get("nanostar_amount", 0)
        if amount is None:
            return "неизвестно"
        return f"{amount}.{str(nanos).zfill(9)}" if nanos else str(amount)
    if isinstance(amount_data, int):
        return str(amount_data)
    return "неизвестно"


async def subscribed(user_id: int) -> tuple[bool | None, str]:
    channel = normalize_channel_ref(get_setting("required_channel", "").strip())
    if not channel or channel == "-":
        return True, "disabled"
    try:
        m = await bot.get_chat_member(channel, user_id)
        if m.status in {"creator", "administrator", "member"}:
            return True, "ok"
        return (m.status == "restricted" and bool(getattr(m, "is_member", False))), "ok"
    except Exception as e:
        emsg = str(e)
        logging.warning(f"sub check fail: {emsg}")
        if "member list is inaccessible" in emsg.lower():
            return None, "inaccessible"
        return None, "error"


async def can_use_search(user_id: int) -> tuple[bool, str]:
    total = get_total_queries(user_id)
    if total == 0:
        increment_total_queries(user_id)
        return True, ""

    if get_setting("subscription_enabled", "1") != "1":
        increment_total_queries(user_id)
        return True, ""

    channel = normalize_channel_ref(get_setting("required_channel", "").strip())
    if not channel or channel == "-":
        increment_total_queries(user_id)
        return True, ""

    is_subscribed, reason_code = await subscribed(user_id)
    if is_subscribed is True:
        increment_total_queries(user_id)
        return True, ""
    if is_subscribed is None:
        # If Telegram API cannot verify membership (e.g., member list inaccessible),
        # do not hard-block users with false negatives.
        logging.warning(f"subscription check unavailable for user={user_id}, reason={reason_code}")
        increment_total_queries(user_id)
        return True, ""

    return False, f"Для следующих запросов подпишитесь на канал: {channel}"


async def send_donation_invoice(chat_id: int, amount: int):
    await bot.send_invoice(
        chat_id=chat_id,
        title="Пожертвование",
        description=f"Поддержка проекта на {amount} ⭐",
        payload=f"donation:{amount}:{int(datetime.utcnow().timestamp())}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label="Пожертвование", amount=amount)],
        start_parameter="support_rust_tracker",
    )


async def bm_find_player_by_steamid(steamid: str) -> str | None:
    url = "https://api.battlemetrics.com/players"
    params = {
        "filter[search]": steamid,
        "page[size]": 10,
        "page[offset]": 0,
        "sort": "-lastSeen",
    }
    data = await bm_request(url, params=params, timeout=18)
    players = data.get("data", []) or []
    if not players:
        return None

    def identifiers_has_steam(attrs: dict) -> bool:
        for ident in (attrs.get("identifiers") or []):
            if steamid in str(ident):
                return True
        return False

    for p in players:
        attrs = p.get("attributes", {}) or {}
        if identifiers_has_steam(attrs):
            return p.get("id")
    return players[0].get("id")


async def add_tracking_for_user(user_id: int, player_id: str) -> tuple[str, bool]:
    player = await bm_get_player(player_id)
    name = player.get("data", {}).get("attributes", {}).get("name", "Unknown")
    steam_id = extract_steam_id(player)

    sessions_data = await bm_get_sessions(player_id)
    sessions = sessions_data.get("data", [])
    if sessions:
        last = sessions[0]
        attr = last.get("attributes", {})
        current_stop = attr.get("stop")
        server_data = last.get("relationships", {}).get("server", {}).get("data")
        current_server_id = server_data["id"] if server_data else None
    else:
        current_stop = None
        current_server_id = None

    already_tracked = player_id in trackings.get(user_id, {})
    trackings[user_id][player_id] = {
        "name": name,
        "steam_id": steam_id,
        "last_stop": current_stop,
        "last_server_id": current_server_id,
    }
    save_trackings()
    return name, already_tracked


def prioritize_exact_nickname(players, nickname: str):
    def sort_key(player):
        name = (player.get("attributes", {}).get("name") or "")
        return 0 if name == nickname else 1

    return sorted(players, key=sort_key)


def normalize_nickname(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def nickname_similarity(query: str, candidate: str) -> float:
    q = normalize_nickname(query)
    c = normalize_nickname(candidate)
    if not q or not c:
        return 0.0
    return SequenceMatcher(None, q, c).ratio()


def filter_players_by_similarity(players, nickname: str, min_similarity: float = MIN_NICKNAME_SIMILARITY):
    filtered = []
    for p in players:
        name = (p.get("attributes", {}) or {}).get("name", "")
        sim = nickname_similarity(nickname, name)
        if sim >= min_similarity:
            filtered.append(p)
    return filtered


def get_pagination_info(data: dict, page: int, page_size: int):
    players = data.get("data", []) or []
    meta = data.get("meta", {}) or {}
    links = data.get("links", {}) or {}

    total_pages = None
    total = meta.get("total")
    if isinstance(total, int) and total >= 0:
        total_pages = max(1, (total + page_size - 1) // page_size)

    if total_pages is not None:
        has_next = page < total_pages
    else:
        has_next = bool(links.get("next")) or len(players) == page_size

    return total_pages, has_next


async def get_player_preview(player_id: str):
    name = "Unknown"
    try:
        player_data = await bm_get_player(player_id)
        name = player_data.get("data", {}).get("attributes", {}).get("name", "Unknown")

        sessions_data = await bm_get_sessions(player_id, include_server=True)
        sessions = sessions_data.get("data", [])
        included = sessions_data.get("included", [])
        included_servers = {
            item.get("id"): item.get("attributes", {})
            for item in included
            if item.get("type") == "server"
        }

        if not sessions:
            return {
                "name": name,
                "last_time": "—",
                "servers": [],
                "player_id": player_id
            }

        last_attr = sessions[0]["attributes"]
        last_time = format_time(last_attr.get("stop") or last_attr.get("start"))

        recent_servers = []
        seen = set()
        for s in sessions[:6]:
            rel = s.get("relationships", {})
            server_data = rel.get("server", {}).get("data")
            if not server_data:
                continue
            server_id = server_data["id"]
            if server_id in seen:
                continue
            seen.add(server_id)

            server_attr = included_servers.get(server_id, {})
            server_name = server_attr.get("name", "Unknown")

            recent_servers.append({
                "name": server_name
            })
            if len(recent_servers) >= 3:
                break

        return {
            "name": name,
            "last_time": last_time,
            "servers": recent_servers,
            "player_id": player_id
        }
    except Exception as e:
        logging.error(f"Preview error {player_id}: {e}")
        return {
            "name": name or "Unknown",
            "last_time": "—",
            "servers": [],
            "player_id": player_id
        }


async def send_player_profile_details(
    target_message: types.Message,
    player_id: str,
    viewer_user_id: int | None = None,
):
    player = await bm_get_player(player_id)
    sessions_data = await bm_get_sessions(player_id)

    name = player.get("data", {}).get("attributes", {}).get("name", "Unknown")
    sessions_list = sessions_data.get("data", [])
    text = f"👤 {name}\n"

    if not sessions_list:
        text += "\n❌ Нет сессий"
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔗 Открыть профиль", url=f"https://www.battlemetrics.com/players/{player_id}")
        ]])
        await target_message.answer(text, reply_markup=kb)
        return

    def session_sort_key(session: dict):
        attr = session.get("attributes", {})
        return attr.get("start") or attr.get("stop") or ""

    active_sessions = [s for s in sessions_list if not (s.get("attributes", {}) or {}).get("stop")]
    if active_sessions:
        current = max(active_sessions, key=session_sort_key)
        is_online = True
    else:
        current = max(sessions_list, key=session_sort_key)
        is_online = False

    current_attr = current.get("attributes", {})
    current_rel = current.get("relationships", {})
    current_server_id = None
    if current_rel.get("server") and current_rel["server"].get("data"):
        current_server_id = current_rel["server"]["data"]["id"]

    if current_server_id:
        current_server_name, players, max_players, _, _ = await bm_get_server(current_server_id)
    else:
        current_server_name, players, max_players = "Unknown", 0, 0

    status = "🟢 онлайн" if is_online else "🔴 оффлайн"
    text += f"{status}\n"
    text += f"🌐 {'Текущий сервер' if is_online else 'Последний сервер'}: {current_server_name}\n\n"
    text += "История серверов (последние 5):\n"

    sessions_sorted = sorted(sessions_list, key=session_sort_key, reverse=True)
    for s in sessions_sorted[:5]:
        attr = s.get("attributes", {})
        rel = s.get("relationships", {})
        server_id = None
        if rel.get("server") and rel["server"].get("data"):
            server_id = rel["server"]["data"]["id"]

        if server_id:
            server_name, players, max_players, ip, port = await bm_get_server(server_id)
            endpoint = f"{ip}:{port}" if ip and port else "—"
        else:
            server_name, players, max_players, endpoint = "Unknown", 0, 0, "—"

        status_icon = "🟢" if not attr.get("stop") else "🔴"
        if not attr.get("stop"):
            time_label = "🕒 Сессия началась"
            time_value = format_time(attr.get("start"))
        else:
            time_label = "🕒 Последнее посещение"
            time_value = format_time(attr.get("stop") or attr.get("start"))

        text += (
            f"🌐 {server_name} {status_icon}\n"
            f"👥 Игроки: {players}/{max_players}\n"
            f"connect {endpoint}\n"
            f"{time_label}: {time_value}\n\n"
        )

    tracking_button = InlineKeyboardButton(
        text="👁 Отслеживать игрока",
        callback_data=f"bm_track:{player_id}",
    )
    if viewer_user_id is not None and player_id in trackings.get(viewer_user_id, {}):
        tracking_button = InlineKeyboardButton(
            text="❌ Отменить отслеживание",
            callback_data=f"bm_untrack:{player_id}",
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Открыть профиль", url=f"https://www.battlemetrics.com/players/{player_id}")],
            [tracking_button],
        ]
    )
    await target_message.answer(text, reply_markup=kb)


# ====================== TRACKING ======================
async def tracking_checker():
    while True:
        await asyncio.sleep(90)
        for user_id, tracked_players in list(trackings.items()):
            for player_id, info in list(tracked_players.items()):
                try:
                    sessions_data = await bm_get_sessions(player_id)
                    sessions = sessions_data.get("data", [])
                    if not sessions:
                        continue

                    last = sessions[0]
                    attr = last.get("attributes", {})
                    current_stop = attr.get("stop")
                    server_data = last.get("relationships", {}).get("server", {}).get("data")
                    current_server_id = server_data["id"] if server_data else None
                    is_online = current_stop is None
                    if is_online and current_server_id:
                        server_name, _, _, _, _ = await bm_get_server(current_server_id)
                    else:
                        server_name = "В меню"

                    changed = (
                        current_stop != info.get("last_stop")
                        or current_server_id != info.get("last_server_id")
                    )
                    if not changed:
                        continue

                    name = info.get("name") or "Unknown"
                    steam_id = info.get("steam_id")
                    if name == "Unknown" or not steam_id:
                        player_data = await bm_get_player(player_id)
                        name = player_data.get("data", {}).get("attributes", {}).get("name", name)
                        steam_id = steam_id or extract_steam_id(player_data)

                    text = build_tracking_text(
                        name=name,
                        steam_id=steam_id,
                        is_online=is_online,
                        server_name=server_name,
                    )
                    await bot.send_message(
                        user_id,
                        text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=tracking_controls_kb(player_id),
                    )

                    info["name"] = name
                    info["steam_id"] = steam_id
                    info["last_stop"] = current_stop
                    info["last_server_id"] = current_server_id
                    save_trackings()
                except Exception as e:
                    logging.error(f"Tracking error {player_id}: {e}")


# ====================== HANDLERS ======================

@dp.message(Command("start"))
async def start(message: types.Message):
    if not message.from_user:
        return
    touch_user(message.from_user.id)
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        attach_referral_if_missing(message.from_user.id, parts[1].strip())
    user_modes[message.from_user.id] = "nickname"
    await message.answer(
        "Выберите действие:\n"
        f"• {MENU_NICK}\n"
        f"• {MENU_STEAM}\n"
        f"• {MENU_DONATE}",
        reply_markup=main_menu_inline_kb(),
    )


async def do_nickname_search(message: types.Message, nickname: str, actor_user_id: int | None = None):
    increment_counter("search_user_clicks")
    request_user_id = actor_user_id if actor_user_id is not None else (message.from_user.id if message.from_user else None)
    safe_nickname = escape_html(nickname)
    print(f"[LOG SEARCH] Запущен поиск по нику: '{nickname}'")
    await message.answer(f"🔍 Ищем: <b>{safe_nickname}</b>...", parse_mode="HTML")

    data, selected_sort_mode = await bm_search_players_with_variants(nickname, page=1)
    sort_token = sort_mode_to_token(selected_sort_mode)
    print(f"[LOG SEARCH] selected_sort={selected_sort_mode} token={sort_token}")

    raw_players = data.get("data", [])
    players = prioritize_exact_nickname(raw_players, nickname)
    print(f"[LOG SEARCH] players_raw={len(players)}")

    players = filter_players_by_similarity(players, nickname)
    print(
        f"[LOG SEARCH] players_filtered={len(players)} "
        f"threshold={MIN_NICKNAME_SIMILARITY}"
    )

    exact_on_page = sum(
        1 for p in players if ((p.get("attributes", {}) or {}).get("name", "") == nickname)
    )
    print(f"[LOG SEARCH] exact_on_page={exact_on_page}")

    total_pages, has_next = get_pagination_info(data, page=1, page_size=SEARCH_PAGE_SIZE)
    if total_pages is not None:
        print(f"[LOG SEARCH] total_pages={total_pages}")
    else:
        print(f"[LOG SEARCH] total_pages=unknown (has_next={has_next})")

    if not players:
        if has_next:
            await message.answer("❌ На этой странице нет игроков с похожестью 80%+. Попробуй следующую страницу.")
        else:
            await message.answer("❌ Игроков с похожестью 80%+ не найдено.")

    if len(players) == 1:
        await send_player_profile_details(message, players[0]["id"], viewer_user_id=request_user_id)
    else:
        preview_tasks = [get_player_preview(p["id"]) for p in players]
        previews = await asyncio.gather(*preview_tasks, return_exceptions=True)

        # === ОДИН НИК = ОДНО СООБЩЕНИЕ ===
        for preview in previews:
            if isinstance(preview, Exception):
                continue
            block = f"👤 <b>Ник:</b> {escape_html(preview['name'])}\n"

            if preview["servers"]:
                for srv in preview["servers"]:
                    block += f"    🌐 Сервер: {escape_html(srv['name'])}\n"
            else:
                block += "    🌐 Сервер: неизвестно\n"

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔎 Подробнее",
                    callback_data=f"bm_profile:{preview['player_id']}"
                )
            ]])

            await message.answer(block, reply_markup=kb, parse_mode="HTML")

    # === ФИНАЛЬНОЕ СООБЩЕНИЕ С ПАГИНАЦИЕЙ ===
    if len(players) != 1:
        if total_pages is not None:
            nav_text = f"🔎 Результаты по <b>{safe_nickname}</b>\nСтраница <b>1</b>/{total_pages}"
        else:
            nav_text = f"🔎 Результаты по <b>{safe_nickname}</b>\nСтраница <b>1</b>"
        nav_buttons = []
        if has_next:
            nav_buttons.append(
                InlineKeyboardButton(
                    text="➡️ Следующая страница",
                    callback_data=f"bm_search_page:2:{sort_token}:{nickname}",
                )
            )

        nav_kb = InlineKeyboardMarkup(inline_keyboard=[nav_buttons]) if nav_buttons else None
        await message.answer(nav_text, reply_markup=nav_kb, parse_mode="HTML")


async def do_steam_search(message: types.Message, raw_input: str):
    increment_counter("search_user_clicks")
    if not STEAM_API_KEY:
        await message.answer("❌ STEAM_API_KEY не настроен в .env")
        return

    value, inp_type = parse_steam_input(raw_input)
    await bot.send_chat_action(message.chat.id, "typing")

    if inp_type == "vanity":
        await message.answer("🔍 Ищу SteamID...")
        steamid = await resolve_vanity(value)
        if not steamid:
            await message.answer("❌ Не удалось найти SteamID по этому значению.")
            return
    elif inp_type == "steamid":
        steamid = value
    else:
        await message.answer("❌ Введите SteamID64 или ссылку/ник Steam.")
        return

    player = await steam_player_info(steamid)
    if not player:
        await message.answer("❌ Не удалось получить данные Steam-профиля.")
        return

    name = player.get("personaname", "—")
    profile_url = player.get("profileurl", f"https://steamcommunity.com/profiles/{steamid}/")
    status_code = player.get("personastate", 0)
    game_name = player.get("gameextrainfo", "Не в игре") if player.get("gameid") else "Не в игре"
    hours = await rust_hours(steamid)
    hours_text = f"{hours:.2f}" if hours is not None else "скрыто"
    status = "🟢 В сети" if status_code != 0 else "🔴 Не в сети"

    out = (
        f"👤 Имя: {escape_html(name)}\n"
        f"🆔 Steam_id: {escape_html(steamid)} (<a href=\"{escape_html(profile_url)}\">профиль</a>)\n"
        f"💬 Статус: {status}\n"
        f"🎮 Игра: {escape_html(game_name)}\n"
        f"⏰ Часов в Rust: {escape_html(hours_text)}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👁 Отслеживать", callback_data=f"bm_track_steam:{steamid}")],
            [InlineKeyboardButton(text="🔎 Поиск по никнейму", callback_data=f"steam_to_nick:{steamid}")],
        ]
    )
    await message.answer(out, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)


@dp.callback_query(lambda c: c.data.startswith("bm_search_page"))
async def paginate_search(callback: types.CallbackQuery):
    parts = callback.data.split(":", 3)
    if len(parts) >= 4:
        _, page_str, sort_token, nickname = parts
        forced_sort = sort_token_to_mode(sort_token)
    else:
        _, page_str, nickname = callback.data.split(":", 2)
        forced_sort = None
        sort_token = sort_mode_to_token(forced_sort)

    safe_nickname = escape_html(nickname)
    page = int(page_str)
    if page < 1:
        page = 1
    try:
        await callback.answer("Загружаю страницу...")
    except TelegramBadRequest:
        pass
    print(f"[LOG PAGINATE] request page={page} nickname='{nickname}' forced_sort={forced_sort}")

    data, selected_sort_mode = await bm_search_players_with_variants(
        nickname,
        page=page,
        forced_sort=forced_sort,
    )
    sort_token = sort_mode_to_token(selected_sort_mode)
    print(f"[LOG PAGINATE] selected_sort={selected_sort_mode} token={sort_token}")

    raw_players = data.get("data", [])
    players = prioritize_exact_nickname(raw_players, nickname)
    print(f"[LOG PAGINATE] players_raw={len(players)}")

    players = filter_players_by_similarity(players, nickname)
    print(
        f"[LOG PAGINATE] players_filtered={len(players)} "
        f"threshold={MIN_NICKNAME_SIMILARITY}"
    )

    exact_on_page = sum(
        1 for p in players if ((p.get("attributes", {}) or {}).get("name", "") == nickname)
    )
    print(f"[LOG PAGINATE] exact_on_page={exact_on_page}")

    total_pages, has_next = get_pagination_info(data, page=page, page_size=SEARCH_PAGE_SIZE)
    if total_pages is not None:
        print(f"[LOG PAGINATE] page={page}/{total_pages} players={len(players)}")
    else:
        print(f"[LOG PAGINATE] page={page} (total unknown) players={len(players)}")

    # Удаляем старые сообщения (чтобы чат был чистым)
    try:
        for i in range(15):
            await bot.delete_message(callback.message.chat.id, callback.message.message_id - i)
    except:
        pass

    await callback.message.answer(f"🔍 Ищем страницу <b>{page}</b>...", parse_mode="HTML")

    if not players:
        if has_next:
            await callback.message.answer("❌ На этой странице нет игроков с похожестью 80%+. Можно перейти дальше.")
        else:
            await callback.message.answer("❌ На этой странице нет игроков с похожестью 80%+.")

    if len(players) == 1:
        await send_player_profile_details(
            callback.message,
            players[0]["id"],
            viewer_user_id=(callback.from_user.id if callback.from_user else None),
        )
    else:
        preview_tasks = [get_player_preview(p["id"]) for p in players]
        previews = await asyncio.gather(*preview_tasks, return_exceptions=True)

        # === ОДИН НИК = ОДНО СООБЩЕНИЕ ===
        for preview in previews:
            if isinstance(preview, Exception):
                continue
            block = f"👤 <b>Ник:</b> {escape_html(preview['name'])}\n"

            if preview["servers"]:
                for srv in preview["servers"]:
                    block += f"    🌐 Сервер: {escape_html(srv['name'])}\n"
            else:
                block += "    🌐 Сервер: неизвестно\n"

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔎 Подробнее",
                    callback_data=f"bm_profile:{preview['player_id']}"
                )
            ]])

            await callback.message.answer(block, reply_markup=kb, parse_mode="HTML")

    # === ФИНАЛЬНОЕ СООБЩЕНИЕ С ПАГИНАЦИЕЙ ===
    if len(players) != 1:
        if total_pages is not None:
            nav_text = f"🔎 Результаты по <b>{safe_nickname}</b>\nСтраница <b>{page}</b>/{total_pages}"
            page_marker = f"{page}/{total_pages}"
        else:
            nav_text = f"🔎 Результаты по <b>{safe_nickname}</b>\nСтраница <b>{page}</b>"
            page_marker = f"{page}"
        nav_buttons = []
        if page > 1:
            nav_buttons.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"bm_search_page:{page-1}:{sort_token}:{nickname}",
                )
            )
        nav_buttons.append(InlineKeyboardButton(text=page_marker, callback_data="noop"))
        if has_next:
            nav_buttons.append(
                InlineKeyboardButton(
                    text="➡️ Следующая страница",
                    callback_data=f"bm_search_page:{page+1}:{sort_token}:{nickname}",
                )
            )

        await callback.message.answer(
            nav_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[nav_buttons]),
            parse_mode="HTML",
        )


@dp.callback_query(lambda c: c.data == "noop")
async def noop_callback(callback: types.CallbackQuery):
    try:
        await callback.answer()
    except TelegramBadRequest:
        pass


@dp.callback_query(lambda c: c.data.startswith("bm_profile"))
async def show_profile(callback: types.CallbackQuery):
    increment_counter("details_clicks")
    try:
        await callback.answer("Загружаю профиль...")
    except TelegramBadRequest:
        pass

    player_id = callback.data.split(":")[1]
    await send_player_profile_details(
        callback.message,
        player_id,
        viewer_user_id=(callback.from_user.id if callback.from_user else None),
    )


@dp.callback_query(lambda c: c.data.startswith("bm_track:"))
async def start_tracking(callback: types.CallbackQuery):
    increment_counter("tracking_clicks")
    try:
        await callback.answer("Добавляю в отслеживание...")
    except TelegramBadRequest:
        pass

    player_id = callback.data.split(":")[1]
    name, already_tracked = await add_tracking_for_user(callback.from_user.id, player_id)

    text = (
        f"✅ Игрок <b>{escape_html(name)}</b> добавлен в отслеживание.\n"
    )
    if already_tracked:
        text = (
            f"ℹ️ Игрок <b>{escape_html(name)}</b> уже был в отслеживании.\n"
            f"Я обновил его состояние."
        )
    await callback.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=tracking_controls_kb(player_id),
    )


@dp.callback_query(lambda c: c.data.startswith("bm_untrack:"))
async def stop_tracking(callback: types.CallbackQuery):
    player_id = callback.data.split(":")[1]
    user_id = callback.from_user.id

    player_info = trackings.get(user_id, {}).get(player_id)
    if not player_info:
        try:
            await callback.answer("Этот игрок уже не отслеживается.", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    name = player_info.get("name") or "Unknown"
    del trackings[user_id][player_id]
    if not trackings[user_id]:
        del trackings[user_id]
    save_trackings()

    try:
        await callback.answer(f"Отслеживание отключено: {name}", show_alert=True)
    except TelegramBadRequest:
        pass


@dp.callback_query(lambda c: c.data == "check_sub")
async def check_sub_callback(callback: types.CallbackQuery):
    if get_setting("subscription_enabled", "1") != "1":
        try:
            await callback.answer("Проверка подписки сейчас выключена админом.", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    ok, reason = await subscribed(callback.from_user.id)
    try:
        if ok is True:
            await callback.answer("Подписка подтверждена ✅", show_alert=True)
        elif ok is False:
            await callback.answer("Подписка не найдена ❌", show_alert=True)
        else:
            if reason == "inaccessible":
                await callback.answer(
                    "Не могу проверить подписку: у бота нет доступа к списку участников канала.",
                    show_alert=True,
                )
            else:
                await callback.answer("Не удалось проверить подписку. Попробуйте позже.", show_alert=True)
    except TelegramBadRequest:
        pass


@dp.callback_query(lambda c: c.data.startswith("menu_mode:"))
async def menu_mode_callback(callback: types.CallbackQuery):
    mode = callback.data.split(":", 1)[1]
    user_modes[callback.from_user.id] = mode
    text = "Отправьте никнейм для поиска."
    if mode == "steam":
        text = "Режим поиска по Steam ID активирован. Отправь SteamID64/ссылку/vanity."
    elif mode == "donate":
        text = "Режим пожертвования активирован. Отправь количество звезд числом."
    try:
        await callback.answer("Готово")
    except TelegramBadRequest:
        pass
    await callback.message.answer(text)


@dp.callback_query(lambda c: c.data.startswith("steam_to_nick:"))
async def steam_to_nick_callback(callback: types.CallbackQuery):
    steamid = callback.data.split(":", 1)[1]
    try:
        await callback.answer("Ищу по никнейму...")
    except TelegramBadRequest:
        pass

    ok, reason = await can_use_search(callback.from_user.id)
    if not ok:
        await callback.message.answer(
            f"❌ {escape_html(reason)}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")]]
            ),
        )
        return

    player = await steam_player_info(steamid)
    nickname = (player or {}).get("personaname")
    if not nickname:
        await callback.message.answer("❌ Не удалось получить ник из Steam-профиля.")
        return
    await do_nickname_search(
        callback.message,
        nickname,
        actor_user_id=(callback.from_user.id if callback.from_user else None),
    )


@dp.callback_query(lambda c: c.data.startswith("bm_track_steam:"))
async def track_from_steam_callback(callback: types.CallbackQuery):
    increment_counter("tracking_clicks")
    steamid = callback.data.split(":", 1)[1]
    try:
        await callback.answer("Добавляю в отслеживание...")
    except TelegramBadRequest:
        pass

    player_id = await bm_find_player_by_steamid(steamid)
    if not player_id:
        await callback.message.answer("❌ Не удалось найти BattleMetrics player_id по этому Steam ID.")
        return

    name, already = await add_tracking_for_user(callback.from_user.id, player_id)
    text = f"✅ Игрок <b>{escape_html(name)}</b> добавлен в отслеживание."
    if already:
        text = f"ℹ️ Игрок <b>{escape_html(name)}</b> уже был в отслеживании."
    await callback.message.answer(text, parse_mode="HTML", reply_markup=tracking_controls_kb(player_id))


@dp.message(Command("menu"))
async def menu_cmd(message: types.Message):
    user_modes[message.from_user.id] = "nickname"
    await message.answer("Главное меню открыто.", reply_markup=main_menu_inline_kb())


@dp.message(Command("setchannel"))
async def cmd_setchannel(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Формат: /setchannel <@channel или ->")
        return
    channel = normalize_channel_ref(parts[1].strip())
    set_setting("required_channel", channel)
    await message.answer(f"Канал для подписки обновлен: {escape_html(channel)}", parse_mode="HTML")


@dp.message(Command("sub"))
async def cmd_sub_toggle(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        state = "ON" if get_setting("subscription_enabled", "1") == "1" else "OFF"
        await message.answer(f"Текущий режим проверки подписки: {state}\nИспользование: /sub on или /sub off")
        return
    arg = parts[1].strip().lower()
    if arg not in {"on", "off"}:
        await message.answer("Использование: /sub on или /sub off")
        return
    set_setting("subscription_enabled", "1" if arg == "on" else "0")
    await message.answer(f"Проверка подписки: {'включена' if arg == 'on' else 'выключена'}")


@dp.message(Command("settings"))
async def cmd_settings(message: types.Message):
    if not admin(message.from_user.id):
        await message.answer("Команда только для админа.")
        return

    report_text = build_settings_report_text()
    report_path = Path(__file__).with_name("settings_report.txt")
    report_path.write_text(report_text, encoding="utf-8")

    channel = get_setting("required_channel", "(не задан)")
    sub_state = "ON" if get_setting("subscription_enabled", "1") == "1" else "OFF"
    await message.answer(
        "Настройки:\n"
        f"📢 Канал: {channel}\n"
        f"🔒 Проверка подписки: {sub_state}"
    )
    await message.answer_document(
        document=types.FSInputFile(str(report_path)),
        caption="Статистика бота",
    )


@dp.pre_checkout_query()
async def on_pre_checkout_query(q: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(q.id, ok=True)


@dp.message(F.successful_payment)
async def on_successful_payment(message: types.Message):
    pay = message.successful_payment
    if not pay:
        return
    uid = message.from_user.id if message.from_user else 0
    save_donation(uid, int(pay.total_amount), pay.invoice_payload, pay.telegram_payment_charge_id)
    await message.answer(
        "✅ Платеж получен.\n"
        f"Зачислено: {int(pay.total_amount)} XTR\n"
        "Спасибо за поддержку!"
    )


@dp.message()
async def main_text_handler(message: types.Message):
    if not message.from_user:
        return
    text = (message.text or "").strip()
    if not text:
        return
    if text.startswith("/"):
        await message.answer("Неизвестная команда. Используй /menu")
        return

    user_id = message.from_user.id
    touch_user(user_id)

    if text == MENU_BACK:
        user_modes[user_id] = "nickname"
        await message.answer("Отправьте никнейм для поиска.", reply_markup=main_menu_kb())
        return
    if text == MENU_NICK:
        user_modes[user_id] = "nickname"
        await message.answer("Отправьте никнейм для поиска.", reply_markup=main_menu_kb())
        return
    if text == MENU_STEAM:
        user_modes[user_id] = "steam"
        await message.answer("Отправь SteamID64, ссылку на профиль или vanity.", reply_markup=main_menu_kb())
        return
    if text == MENU_DONATE:
        user_modes[user_id] = "donate"
        await message.answer(
            "Введи количество звезд для пожертвования (например: 25).",
            reply_markup=main_menu_kb(),
        )
        return

    mode = user_modes.get(user_id, "nickname")

    if mode == "donate":
        if not text.isdigit() or int(text) <= 0:
            await message.answer("Введите положительное число звезд, например 10.")
            return
        amount = int(text)
        await send_donation_invoice(message.chat.id, amount)
        await message.answer(f"Счет на {amount} XTR отправлен.")
        return

    ok, reason = await can_use_search(user_id)
    if not ok:
        await message.answer(
            f"❌ {escape_html(reason)}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")]]
            ),
        )
        return

    if mode == "steam":
        if is_probable_steam_input(text):
            await do_steam_search(message, text)
        else:
            # Keep nickname search behavior stable even if steam mode was selected earlier.
            await do_nickname_search(message, text)
    else:
        await do_nickname_search(message, text)


# ====================== RUN ======================

async def main():
    print("🚀 Rust онлайн чекер бот — отдельные сообщения + пагинация с кнопкой Следующая")
    init_tracking_db()
    load_trackings()
    asyncio.create_task(tracking_checker())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
