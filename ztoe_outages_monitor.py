#!/usr/bin/env python3
"""
Перевірка аварійних / планових відключень Житомиробленерго для підписників бота.

Читає users з Mongo (notify=true, є addresses),
тягне /pw_accidents + /pw_accident_planeds,
порівнює з outages_cache, шле Telegram при нових відключеннях по вулиці користувача.

Secrets / env:
  MAYACHOK_MONGO_URI або MONGO_URI
  ZTOE_MONGO_DB або MONGO_DB          (напр. vidkluchennya)
  ZTOE_BOT_TOKEN або TELEGRAM_BOT_TOKEN
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx
from pymongo import MongoClient
from telegram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_outages_monitor")

API_BASE = "https://api-ztoe-poweron.inneti.net/api"

TYPE_LABELS = {
    0: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
    1: ("Аварійне", "несподівана аварія в мережі"),
    2: ("Оперативне", "технічні перемикання в мережі"),
    3: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
}


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def _fetch_list(path: str, otg_id: int, city_id: int = None) -> List[dict]:
    params = {"page": 1, "otg.id": otg_id}
    if city_id:
        params["city.id"] = city_id
    with httpx.Client(timeout=25) as client:
        r = client.get(f"{API_BASE}{path}", params=params)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "hydra:member" in data:
            return data["hydra:member"]
        return data if isinstance(data, list) else []


def fetch_all_outages(otg_id: int, city_id: int = None) -> List[dict]:
    accidents = _fetch_list("/pw_accidents", otg_id, city_id)
    try:
        planned = _fetch_list("/pw_accident_planeds", otg_id, city_id)
    except Exception as e:
        log.warning("planeds error otg=%s city=%s: %s", otg_id, city_id, e)
        planned = []
    for a in accidents:
        a["_source"] = "accidents"
    for p in planned:
        p["_source"] = "planeds"
    return accidents + planned


def make_outage_key(outage: dict) -> str:
    return f"{outage.get('_source', 'x')}:{outage.get('id')}"


def format_outage(outage: dict) -> str:
    city = (outage.get("city") or {}).get("name", "?")
    street = (outage.get("street") or {}).get("name", "?")
    buildings = (outage.get("buildingNames") or "").strip()
    date_event = (outage.get("dateEvent") or "")[:16].replace("T", " ")
    date_plan = (outage.get("datePlanIn") or "")[:16].replace("T", " ")
    type_off = outage.get("typeOff")
    type_name, type_desc = TYPE_LABELS.get(type_off, (f"Тип {type_off}", ""))
    icon = {0: "🟠", 1: "🔴", 2: "🟡", 3: "🟠"}.get(type_off, "⚪")

    text = f"{icon} <b>{city}, {street}</b>"
    text += f"\n📌 {type_name}"
    if type_desc:
        text += f" — {type_desc}"
    if buildings:
        text += f"\n🏘 Будинки: {buildings}"
    if date_event:
        text += f"\n⏰ З: {date_event}"
    if date_plan:
        text += f"\n⏳ До (план): {date_plan}"
    koment = (outage.get("koment") or "").strip()
    if koment:
        text += f"\n💬 {koment}"
    return text


def matches_street(outage: dict, street_id, street_name: str) -> bool:
    o_street = outage.get("street") or {}
    o_street_id = o_street.get("id")
    o_street_name = (o_street.get("name") or "").lower()
    if street_id and o_street_id == street_id:
        return True
    if street_name and street_name in o_street_name:
        return True
    return False



def send_log(text: str) -> None:
    """Пише підсумок у лог-канал ботом репо (TELEGRAM_BOT_TOKEN), не ботом відключень."""
    channel = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or os.getenv("ZTOE_LOG_CHANNEL_ID")
    log_token = os.getenv("TELEGRAM_BOT_TOKEN")  # той самий, що в інших monitor'ах
    if not channel:
        log.info("TELEGRAM_LOG_CHANNEL_ID не задано — пропуск лог-каналу")
        return
    if not log_token:
        log.info("TELEGRAM_BOT_TOKEN не задано — пропуск лог-каналу")
        return
    try:
        log_bot = Bot(token=log_token)
        async def _send():
            await log_bot.send_message(chat_id=channel, text=text, parse_mode="HTML")
        asyncio.run(_send())
        log.info("Лог надіслано в %s", channel)
    except Exception as e:
        log.error("Не вдалося надіслати в лог-канал: %s", e)



def main() -> int:
    mongo_uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    mongo_db = os.getenv("ZTOE_MONGO_DB") or os.getenv("MONGO_DB") or "vidkluchennya"
    bot_token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

    if not mongo_uri:
        log.error("Немає MAYACHOK_MONGO_URI / MONGO_URI")
        return 1
    if not bot_token:
        log.error("Немає ZTOE_BOT_TOKEN / TELEGRAM_BOT_TOKEN")
        return 1

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=15000)
    db = client[mongo_db]
    users_col = db["users"]
    outages_col = db["outages_cache"]

    users = list(users_col.find({"notify": True}))
    users = [u for u in users if get_user_addresses(u)]
    log.info("Користувачів з адресами: %s (db=%s)", len(users), mongo_db)

    if not users:
        log.info("Немає кого перевіряти")
        bot = Bot(token=bot_token)
        send_log(f"✅ <b>ZTOE outages</b>\ndb: <code>{mongo_db}</code>\nusers: 0\n(немає підписників)")
        return 0

    # groups[(otg_id, city_id)] = [(user, addr), ...]
    groups: Dict[Tuple, list] = {}
    for user in users:
        for addr in get_user_addresses(user):
            key = (addr.get("otg_id"), addr.get("city_id"))
            if key[0] is None:
                continue
            groups.setdefault(key, []).append((user, addr))

    bot = Bot(token=bot_token)
    notified: Dict[int, list] = {}  # telegram_id -> list of outages
    groups_checked = 0

    for (otg_id, city_id), pairs in groups.items():
        try:
            outages = fetch_all_outages(int(otg_id), int(city_id) if city_id else None)
        except Exception as e:
            log.error("Fetch error otg=%s city=%s: %s", otg_id, city_id, e)
            continue

        groups_checked += 1
        current_ids = {make_outage_key(o) for o in outages}
        cache_key = f"{otg_id}_{city_id or 0}"
        cache = outages_col.find_one({"_id": cache_key})
        prev_ids = set(cache.get("ids", [])) if cache else set()
        new_ids = current_ids - prev_ids

        outages_col.update_one(
            {"_id": cache_key},
            {
                "$set": {
                    "ids": list(current_ids),
                    "updated_at": datetime.now(timezone.utc),
                    "count": len(current_ids),
                }
            },
            upsert=True,
        )

        if not new_ids:
            continue

        new_outages = [o for o in outages if make_outage_key(o) in new_ids]
        log.info(
            "otg=%s city=%s: %s нових відключень",
            otg_id, city_id, len(new_outages),
        )

        for user, addr in pairs:
            street_id = addr.get("street_id")
            street_name = (addr.get("street_name") or "").lower()
            tid = user["telegram_id"]
            for outage in new_outages:
                if matches_street(outage, street_id, street_name):
                    notified.setdefault(tid, [])
                    key = make_outage_key(outage)
                    if not any(make_outage_key(x) == key for x in notified[tid]):
                        notified[tid].append(outage)

    sent = 0
    for tid, outage_list in notified.items():
        text = "⚠️ <b>Нові відключення по твоїх адресах:</b>\n\n"
        text += "\n\n".join(format_outage(o) for o in outage_list[:10])

        async def _send(chat_id=tid, body=text):
            await bot.send_message(
                chat_id=chat_id,
                text=body,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

        try:
            asyncio.run(_send())
            sent += 1
            log.info("Надіслано %s (%s відключень)", tid, len(outage_list))
            time.sleep(0.35)
        except Exception as e:
            log.error("Send fail %s: %s", tid, e)

    summary = (
        f"✅ <b>ZTOE outages</b>\n"
        f"db: <code>{mongo_db}</code>\n"
        f"users: {len(users)}\n"
        f"groups: {groups_checked}\n"
        f"notifications: {sent}"
    )
    log.info(
        "Готово: users=%s groups=%s notifications=%s",
        len(users), groups_checked, sent,
    )
    send_log(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
