
# -*- coding: utf-8 -*-
import asyncio
import html as html_lib
import logging
import os
import re
from datetime import datetime

import a2s
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

# ====================== .env ======================
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

if not TELEGRAM_TOKEN or not STEAM_API_KEY:
    raise ValueError("❌ Не найдены TELEGRAM_TOKEN или STEAM_API_KEY в .env!")

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
# =================================================

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)


def bm_request_json(url: str, params: dict | None = None, timeout: int = 8, label: str = "BM") -> dict:
    """BattleMetrics request with debug logs for troubleshooting."""
    response = requests.get(url, params=params, timeout=timeout)
    logging.info(f"[BM DEBUG] {label}: {response.status_code} {response.url}")
    logging.info(f"[BM DEBUG] {label}: body preview: {response.text[:700]}")

    try:
        data = response.json()
    except Exception as e:
        logging.warning(f"[BM DEBUG] {label}: JSON parse error: {e}")
        logging.warning(f"[BM DEBUG] {label}: body preview: {response.text[:400]}")
        return {}

    payload = data.get("data")
    if isinstance(payload, list):
        logging.info(f"[BM DEBUG] {label}: items={len(payload)}")
        if payload and isinstance(payload[0], dict):
            first = payload[0]
            attr = first.get("attributes", {})
            logging.info(
                "[BM DEBUG] %s first: type=%s id=%s ip=%s port=%s name=%s",
                label,
                first.get("type"),
                first.get("id"),
                attr.get("ip"),
                attr.get("port"),
                attr.get("name"),
            )
    elif isinstance(payload, dict):
        attr = payload.get("attributes", {})
        logging.info(
            "[BM DEBUG] %s object: type=%s id=%s ip=%s port=%s name=%s",
            label,
            payload.get("type"),
            payload.get("id"),
            attr.get("ip"),
            attr.get("port"),
            attr.get("name"),
        )
    else:
        logging.info(f"[BM DEBUG] {label}: data is empty or unexpected")

    return data


def tsarvar_request_html(ip: str, port: str | int, timeout: int = 10) -> tuple[int, str, str]:
    """Tsarvar request with debug logs."""
    target = f"{ip}:{port}"
    candidates = [
        f"https://tsarvar.com/ru/servers/rust/{target}",
        f"https://tsarvar.com/servers/rust/{target}",
        f"https://tsarvar.com/en/servers/rust/{target}",
    ]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru,en;q=0.9",
    }

    last_status = 0
    last_text = ""
    last_url = candidates[0]
    for url in candidates:
        response = requests.get(url, timeout=timeout, allow_redirects=True, headers=headers)
        logging.info(f"[TS DEBUG] target={target} status={response.status_code} url={response.url}")
        logging.info(f"[TS DEBUG] body preview: {response.text[:700]}")
        last_status = response.status_code
        last_text = response.text
        last_url = response.url
        if response.status_code == 200:
            return response.status_code, response.text, response.url

    return last_status, last_text, last_url


def parse_tsarvar_name(html: str, ip: str, port: str | int) -> str:
    """Extract server name from Tsarvar page title/meta."""
    meta_match = re.search(
        r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']',
        html,
        re.I,
    )
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    raw_title = ""

    if meta_match:
        raw_title = meta_match.group(1)
    elif title_match:
        raw_title = title_match.group(1)

    raw_title = html_lib.unescape(raw_title).strip()
    if not raw_title:
        return ""

    target = re.escape(f"{ip}:{port}")
    name = re.sub(rf"\s*{target}\b.*$", "", raw_title).strip()

    # Remove tail like "— инфо и статистика сервера Rust"
    name = re.sub(r"\s*[—-]\s*инфо.*$", "", name, flags=re.I).strip()
    return name or raw_title


async def get_tsarvar_server_by_address(ip: str, port: str | int) -> dict | None:
    """Find server name on Tsarvar by ip:port page."""
    loop = asyncio.get_running_loop()
    try:
        ip = ip.strip()
        port_str = str(port).strip()
        ports = [port_str]
        if port_str.isdigit():
            base_port = int(port_str)
            for delta in (1, -1, 2, -2):
                alt_port = base_port + delta
                if alt_port > 0:
                    ports.append(str(alt_port))

        fallback: dict | None = None
        seen_ports: set[str] = set()

        for current_port in ports:
            if current_port in seen_ports:
                continue
            seen_ports.add(current_port)

            status_code, html, final_url = await loop.run_in_executor(
                None,
                lambda cp=current_port: tsarvar_request_html(ip, cp),
            )

            if status_code != 200:
                logging.info(f"[TS DEBUG] not found for {ip}:{current_port}, status={status_code}")
                continue

            server_name = parse_tsarvar_name(html, ip, current_port)
            if not server_name:
                continue

            result = {"name": server_name, "connect": f"{ip}:{current_port}", "url": final_url}
            logging.info(f"[TS DEBUG] parsed result: {result}")

            if current_port == port_str:
                return result
            if fallback is None:
                fallback = result

        return fallback
    except Exception as e:
        logging.warning(f"Tsarvar error: {e}")
        return None


def extract_input(steam_input: str):
    steam_input = steam_input.strip()
    if steam_input.isdigit() and len(steam_input) == 17:
        return steam_input, "steamid"
    if "/profiles/" in steam_input:
        sid = steam_input.split("/profiles/")[-1].split("/")[0].split("?")[0]
        if sid.isdigit() and len(sid) == 17:
            return sid, "steamid"
    if "/id/" in steam_input:
        vanity = steam_input.split("/id/")[-1].split("/")[0].split("?")[0]
        return vanity, "vanity"
    if not steam_input.startswith("http") and len(steam_input) < 100:
        return steam_input, "vanity"
    return steam_input, "unknown"


async def resolve_vanity_to_steamid(vanity: str) -> str | None:
    url = "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/"
    params = {"key": STEAM_API_KEY, "vanityurl": vanity}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=10).json()
        )
        response = data.get("response", {})
        return response.get("steamid") if response.get("success") == 1 else None
    except Exception as e:
        logging.error(f"ResolveVanity error: {e}")
        return None


async def get_player_info(steamid: str):
    url = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/"
    params = {"key": STEAM_API_KEY, "steamids": steamid}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=10).json()
        )
        players = data.get("response", {}).get("players", [])
        return players[0] if players else None
    except Exception as e:
        logging.error(f"GetPlayerSummaries error: {e}")
        return None


def split_endpoint(endpoint: str | None) -> tuple[str, str]:
    if not endpoint or ":" not in endpoint:
        return "", ""
    ip, port = endpoint.rsplit(":", 1)
    return ip.strip(), port.strip()


def country_name(code: str | None) -> str:
    if not code:
        return "Неизвестно"
    return COUNTRY_MAP.get(code.upper(), code.upper())


async def get_rust_hours(steamid: str) -> float | None:
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
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=10).json()
        )
        games = data.get("response", {}).get("games", [])
        if not games:
            return None
        minutes = games[0].get("playtime_forever")
        if minutes is None:
            return None
        return float(minutes) / 60.0
    except Exception as e:
        logging.warning(f"GetOwnedGames error: {e}")
        return None


async def get_battlemetrics_name_by_address(ip: str, port: str | int) -> str:
    """Try to find server name in BattleMetrics by ip:port."""
    loop = asyncio.get_running_loop()
    try:
        encoded = requests.utils.quote(f"{ip}:{port}", safe="")
        logging.info(f"[BM DEBUG] Web search URL: https://www.battlemetrics.com/servers/search?q={encoded}")

        search_url = "https://api.battlemetrics.com/servers"
        params = {"filter[search]": f"{ip}:{port}", "filter[game]": "rust"}
        search_data = await loop.run_in_executor(
            None, lambda: bm_request_json(search_url, params=params, timeout=8, label="servers_by_address")
        )

        for server in search_data.get("data", []):
            attr = server.get("attributes", {})
            if attr.get("ip") == ip and str(attr.get("port")) == str(port):
                return attr.get("name", "")

        return ""
    except Exception as e:
        logging.warning(f"BattleMetrics search by address error: {e}")
        return ""


async def get_battlemetrics_server(
    steamid: str,
    expected_ip: str | None = None,
    expected_port: str | None = None,
):
    """Return BattleMetrics server info for a player."""
    loop = asyncio.get_running_loop()
    try:
        search_url = "https://api.battlemetrics.com/players"
        search_params = {"filter[search]": steamid}
        search_data = await loop.run_in_executor(
            None, lambda: bm_request_json(search_url, params=search_params, timeout=8, label="players_search")
        )
        if not search_data.get("data"):
            return None

        player_id = search_data["data"][0]["id"]

        servers_url = f"https://api.battlemetrics.com/players/{player_id}/servers"
        servers_params = {"include": "server"}
        servers_data = await loop.run_in_executor(
            None, lambda: bm_request_json(servers_url, params=servers_params, timeout=8, label="player_servers")
        )
        if not servers_data.get("data"):
            return None

        entries = servers_data.get("data", [])
        if expected_ip:
            ip_matches = [
                item
                for item in entries
                if item.get("attributes", {}).get("ip") == expected_ip
            ]
            if not ip_matches:
                logging.info(
                    f"[BM DEBUG] skip fallback: no strict ip match for expected {expected_ip}:{expected_port or ''}"
                )
                return None

            if expected_port:
                exact = [
                    item
                    for item in ip_matches
                    if str(item.get("attributes", {}).get("port")) == str(expected_port)
                ]
                server_entry = exact[0] if exact else ip_matches[0]
            else:
                server_entry = ip_matches[0]
        else:
            server_entry = entries[0]

        attr = server_entry.get("attributes", {})
        ip = attr.get("ip")
        port = attr.get("port")
        connect_ip = f"{ip}:{port}" if ip and port else ""
        server_name = ""

        server_rel_id = (
            server_entry.get("relationships", {})
            .get("server", {})
            .get("data", {})
            .get("id")
        )
        for included in servers_data.get("included", []):
            if included.get("type") == "server" and included.get("id") == server_rel_id:
                server_name = included.get("attributes", {}).get("name", "")
                break

        if not server_name and server_rel_id:
            server_url = f"https://api.battlemetrics.com/servers/{server_rel_id}"
            server_data = await loop.run_in_executor(
                None, lambda: bm_request_json(server_url, timeout=8, label="server_by_id")
            )
            server_name = server_data.get("data", {}).get("attributes", {}).get("name", "")

        if not server_name and ip and port:
            server_name = await get_battlemetrics_name_by_address(ip, port)

        if not server_name:
            server_name = connect_ip or "Unknown"

        return {"name": server_name, "connect": connect_ip}

    except Exception as e:
        logging.warning(f"BattleMetrics error: {e}")
        return None


@dp.message(Command(commands=["start", "help"]))
async def send_welcome(message: types.Message):
    await message.answer(
        "👋 Привет!\n\n"
        "Отправь SteamID64 или ссылку."
    )


@dp.message()
async def handle_steamid(message: types.Message):
    steam_input = message.text.strip()
    value, input_type = extract_input(steam_input)

    await bot.send_chat_action(message.chat.id, "typing")

    if input_type == "vanity":
        await message.answer("🔍 Ищу SteamID...")
        steamid = await resolve_vanity_to_steamid(value)
        if not steamid:
            await message.answer("❌ Не удалось найти профиль.")
            return
    else:
        steamid = value

    player = await get_player_info(steamid)

    if not player:
        await message.answer("❌ Не удалось получить данные профиля.")
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
    rust_hours = await get_rust_hours(steamid)

    country = country_name(loc_country)
    status = "🟢 В сети 🟢" if status_code != 0 else "⚪ Не в сети ⚪"

    server_line = "🌐 Сервер: не найден"
    if str(gameid) == RUST_APPID:
        expected_connect = ""
        if gameserverip and gameserverip != "0.0.0.0:0":
            expected_connect = gameserverip

        expected_ip, expected_port = split_endpoint(expected_connect)
        final_name = ""
        final_connect = expected_connect

        # Основной источник: TSARVAR
        if expected_ip and expected_port:
            ts_info = await get_tsarvar_server_by_address(expected_ip, expected_port)
            if ts_info:
                final_name = ts_info.get("name", "")
                final_connect = ts_info.get("connect", final_connect)

        # Запасной источник: BattleMetrics с обязательной проверкой IP.
        if not final_name and expected_ip:
            bm_info = await get_battlemetrics_server(
                steamid,
                expected_ip=expected_ip or None,
                expected_port=expected_port or None,
            )
            if bm_info:
                final_name = bm_info.get("name", "")
                final_connect = bm_info.get("connect", final_connect)

        if not final_name and expected_ip and expected_port:
            final_name = await get_battlemetrics_name_by_address(expected_ip, expected_port)

        if not final_name and final_connect:
            final_name = await get_server_name(final_connect)

        if final_name and final_connect:
            server_line = (
                f"🌐 Сервер: {html_lib.escape(final_name)} | "
                f"<code>connect {html_lib.escape(final_connect)}</code>"
            )
        elif final_name:
            server_line = f"🌐 Сервер: {html_lib.escape(final_name)}"
        elif final_connect:
            server_line = f"🌐 Сервер: <code>connect {html_lib.escape(final_connect)}</code>"

    account_created = datetime.fromtimestamp(timecreated).strftime("%d.%m.%Y") if timecreated else "—"
    rust_hours_text = f"{rust_hours:.2f}" if rust_hours is not None else "скрыто"

    text = (
        f"👤 Имя: {html_lib.escape(name)}\n"
        f"🆔 Steam_id: {html_lib.escape(steamid)} ({html_lib.escape(profile_url)})\n"
        f"🌎 Страна: {html_lib.escape(country)}\n"
        f"💬 Статус: {html_lib.escape(status)}\n"
        f"🎮 Игра: {html_lib.escape(game_name)}\n"
        f"{server_line}\n"
        f"📅 Аккаунт создан: {html_lib.escape(account_created)}\n"
        f"⏰ Часов в игре: {html_lib.escape(rust_hours_text)}"
    )

    if avatar:
        await message.answer_photo(photo=avatar, caption=text, parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")


async def get_server_name(gameserverip: str) -> str:  # Оставил на всякий случай
    if not gameserverip or gameserverip == "0.0.0.0:0":
        return ""
    try:
        ip, port = gameserverip.split(":")
        address = (ip, int(port))
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, lambda: a2s.info(address, timeout=4.0))
        return info.server_name.strip() if info and info.server_name else ""
    except Exception:
        return ""


async def main():
    print("🤖 Бот запущен (основной + запасной источник серверов)")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
