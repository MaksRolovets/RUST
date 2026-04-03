# -*- coding: utf-8 -*-
import asyncio
import html
import logging
import os
from datetime import datetime

import a2s
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv
from pathlib import Path

# ====================== .env ======================
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH, encoding="utf-8-sig")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

if not TELEGRAM_TOKEN or not STEAM_API_KEY:
    raise ValueError("❌ Не найдены TELEGRAM_TOKEN или STEAM_API_KEY в .env!")

RUST_APPID = "252490"

COUNTRY_MAP = {
    "RU": "Россия", "BY": "Беларусь", "UA": "Украина", "KZ": "Казахстан",
    "US": "США", "DE": "Германия", "FR": "Франция", "GB": "Великобритания",
}

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)


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
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=10).json()
        )
        resp = data.get("response", {})
        return resp.get("steamid") if resp.get("success") == 1 else None
    except Exception:
        return None


async def player_info(steamid: str) -> dict | None:
    url = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/"
    params = {"key": STEAM_API_KEY, "steamids": steamid}
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=10).json()
        )
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
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=10).json()
        )
        games = data.get("response", {}).get("games", [])
        if not games:
            return None
        mins = games[0].get("playtime_forever")
        return float(mins) / 60.0 if mins is not None else None
    except Exception:
        return None


async def bm_server_name(ip: str, port: str) -> str:
    try:
        url = "https://api.battlemetrics.com/servers"
        params = {"filter[search]": f"{ip}:{port}", "filter[game]": "rust"}
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=8).json()
        )
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
        info = await loop.run_in_executor(
            None, lambda: a2s.info((ip, int(port)), timeout=4.0)
        )
        return info.server_name.strip() if info and info.server_name else ""
    except Exception:
        return ""


@dp.message(Command(commands=["start", "help"]))
async def send_welcome(message: types.Message):
    await message.answer(
        "👋 Привет!\n\n"
        "Отправь мне SteamID64 или ссылку на Steam профиль — я покажу информацию по игроку."
    )


@dp.message()
async def handle_steam(message: types.Message):
    if not message.text or message.text.startswith("/"):
        return

    text = message.text.strip()
    value, inp_type = parse_input(text)

    await bot.send_chat_action(message.chat.id, "typing")

    # Resolve vanity → steamid
    if inp_type == "vanity":
        await message.answer("🔍 Ищу SteamID...")
        steamid = await resolve_vanity(value)
        if not steamid:
            await message.answer("❌ Не удалось найти профиль по этой ссылке/нику.")
            return
    else:
        steamid = value

    player = await player_info(steamid)
    if not player:
        await message.answer("❌ Не удалось получить данные профиля Steam.")
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
    status = "🟢 В сети" if status_code != 0 else "⚪ Не в сети"
    hours = await rust_hours(steamid)
    hours_text = f"{hours:.2f}" if hours is not None else "скрыто"

    server_line = "🌐 Сервер: не найден"
    if str(gameid) == RUST_APPID and gameserverip and gameserverip != "0.0.0.0:0":
        ip, port = split_endpoint(gameserverip)
        srv_name = await bm_server_name(ip, port) or await a2s_name(gameserverip)

        if srv_name:
            server_line = f"🌐 Сервер: {html.escape(srv_name)} | <code>connect {html.escape(gameserverip)}</code>"
        else:
            server_line = f"🌐 Сервер: <code>connect {html.escape(gameserverip)}</code>"

    created = datetime.fromtimestamp(timecreated).strftime("%d.%m.%Y") if timecreated else "—"

    out = (
        f"👤 Имя: {html.escape(name)}\n"
        f"🆔 SteamID: {html.escape(steamid)}\n"
        f"🔗 Профиль: {html.escape(profile_url)}\n"
        f"🌎 Страна: {html.escape(country)}\n"
        f"💬 Статус: {status}\n"
        f"🎮 Игра: {html.escape(game_name)}\n"
        f"{server_line}\n"
        f"📅 Аккаунт создан: {html.escape(created)}\n"
        f"⏰ Часов в Rust: {html.escape(hours_text)}"
    )

    if avatar:
        await message.answer_photo(photo=avatar, caption=out, parse_mode="HTML")
    else:
        await message.answer(out, parse_mode="HTML")


async def main():
    print("🤖 Rust Tracker бот запущен (без ограничений доступа)")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())