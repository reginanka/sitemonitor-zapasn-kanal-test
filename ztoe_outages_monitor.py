#!/usr/bin/env python3
"""
Перевірка аварійних / планових відключень Житомиробленерго для підписників бота.

Читає users з Mongo (notify=true, є addresses),
тягне /pw_accidents + /pw_accident_planeds (усі сторінки),
порівнює з outages_cache за content-fingerprint (не лише id),
пише full docs у outages (для /check у боті),
шле Telegram лише на реально нові події по вулиці.

Secrets / env:
  MAYACHOK_MONGO_URI або MONGO_URI
  ZTOE_MONGO_DB або MONGO_DB
  ZTOE_BOT_TOKEN або TELEGRAM_BOT_TOKEN
  ZTOE_API_BASE (напр. https://api-ztoe-poweron.inneti.net/api)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

import httpx
from pymongo import MongoClient
from telegram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_outages_monitor")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or os.getenv("API_BASE") or "").rstrip("/")
if not API_BASE:
    raise SystemExit("Немає ZTOE_API_BASE (базовий URL API відключень)")

TYPE_LABELS = {
    0: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
    1: ("Аварійне", "несподівана аварія в мережі"),
    2: ("Оперативне", "технічні перемикання в мережі"),
    3: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
}

# typeOff → «сімейство» для дедупу (аварія vs план)
def type_family(type_off) -> str:
    try:
        t = int(type_off)
    except (TypeError, ValueError):
        return "x"
    if t in (1,):
        return "emerg"
    if t in (0, 3):
        return "planned"
    if t == 2:
        return "oper"
    return str(t)


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def _fetch_all_pages(path: str, otg_id: int, city_id: int = None) -> List[dict]:
    """Усі сторінки hydra-колекції (не лише page=1)."""
    items: List[dict] = []
    page = 1
    max_pages = 40
    with httpx.Client(
        timeout=30,
        headers={
            "Accept": "application/ld+json",
            "User-Agent": "ztoe-outages-monitor/1.1",
        },
    ) as client:
        while page <= max_pages:
            params = {"page": page, "itemsPerPage": 100, "otg.id": otg_id}
            if city_id:
                params["city.id"] = city_id
            r = client.get(f"{API_BASE}{path}", params=params)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                items.extend(data)
                break
            batch = data.get("hydra:member") or []
            items.extend(batch)
            view = data.get("hydra:view") or {}
            if not view.get("hydra:next") or not batch:
                break
            page += 1
    return items


def fetch_all_outages(otg_id: int, city_id: int = None) -> List[dict]:
    accidents = _fetch_all_pages("/pw_accidents", otg_id, city_id)
    try:
        planned = _fetch_all_pages("/pw_accident_planeds", otg_id, city_id)
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


def content_fingerprint(outage: dict) -> str:
    """
    Стабільний ключ події: вулиця + будинки + година старту + сімейство типу.
    API часто видає НОВИЙ id на ту саму ГПВ/аварію → без цього йдуть дублікати.
    """
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    # нормалізуємо список будинків
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    bnorm = ",".join(parts)
    start = (outage.get("dateEvent") or "")[:13]  # YYYY-MM-DDTHH
    fam = type_family(outage.get("typeOff"))
    city = (outage.get("city") or {}).get("id") or ""
    return f"{city}|{sid}|{bnorm}|{start}|{fam}"


def soft_fingerprint(outage: dict) -> str:
    """Без типу: та сама вулиця/час/будинки (щоб план після аварії не дублював)."""
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    bnorm = ",".join(parts)
    start = (outage.get("dateEvent") or "")[:13]
    city = (outage.get("city") or {}).get("id") or ""
    return f"{city}|{sid}|{bnorm}|{start}"


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
    if street_name and o_street_name and o_street_name in street_name:
        return True
    return False


def matches_building(outage: dict, house: str) -> bool:
    """Якщо в адресі є номер будинку — перевіряємо, що він у buildingNames."""
    house = (house or "").strip()
    if not house:
        return True
    buildings = (outage.get("buildingNames") or "").strip()
    if not buildings:
        return True  # API без будинків — не відсікаємо
    tokens = {p.strip().lower() for p in buildings.replace(";", ",").split(",") if p.strip()}
    h = house.lower()
    if h in tokens:
        return True
    # 066 vs 66
    h2 = h.lstrip("0") or "0"
    if h2 in tokens or any(t.lstrip("0") == h2 for t in tokens):
        return True
    return False


def outage_to_mongo_doc(outage: dict) -> dict:
    city = outage.get("city") or {}
    street = outage.get("street") or {}
    otg = outage.get("otg") or {}
    return {
        "source_id": outage.get("id"),
        "source": outage.get("_source"),
        "typeOff": outage.get("typeOff"),
        "city_id": city.get("id"),
        "city_name": city.get("name"),
        "street_id": street.get("id"),
        "street_name": street.get("name"),
        "otg_id": otg.get("id"),
        "otg_name": otg.get("name"),
        "buildingNames": outage.get("buildingNames"),
        "dateEvent": outage.get("dateEvent"),
        "datePlanIn": outage.get("datePlanIn"),
        "koment": outage.get("koment"),
        "fingerprint": content_fingerprint(outage),
        "soft_fp": soft_fingerprint(outage),
        "updated_at": datetime.now(timezone.utc),
    }


def main() -> int:
    mongo_uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not mongo_uri:
        log.error("Немає MONGO_URI")
        return 1
    db_name = os.getenv("ZTOE_MONGO_DB") or os.getenv("MONGO_DB") or "vidkluchennya"
    bot_token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        log.error("Немає ZTOE_BOT_TOKEN")
        return 1

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    users_col = db["users"]
    outages_col = db["outages_cache"]
    outages_full = db["outages"]

    users = list(users_col.find({"notify": True}))
    # збираємо (user, addr) з валідним otg
    pairs: List[Tuple[dict, dict]] = []
    groups: Dict[Tuple[int, int], List[Tuple[dict, dict]]] = {}
    for u in users:
        for addr in get_user_addresses(u):
            if not isinstance(addr, dict):
                continue
            otg_id = addr.get("otg_id") or addr.get("otgId")
            if not otg_id:
                continue
            city_id = addr.get("city_id") or addr.get("cityId") or 0
            try:
                otg_id = int(otg_id)
                city_id = int(city_id) if city_id else 0
            except (TypeError, ValueError):
                continue
            pairs.append((u, addr))
            groups.setdefault((otg_id, city_id), []).append((u, addr))

    log.info("Підписників: %s, пар адрес: %s, груп: %s", len(users), len(pairs), len(groups))

    bot = Bot(token=bot_token)
    notified: Dict[int, list] = {}
    groups_checked = 0

    for (otg_id, city_id), group_pairs in groups.items():
        try:
            outages = fetch_all_outages(otg_id, city_id if city_id else None)
        except Exception as e:
            log.error("Fetch error otg=%s city=%s: %s", otg_id, city_id, e)
            continue

        groups_checked += 1
        current_ids = {make_outage_key(o) for o in outages}
        current_fps = {content_fingerprint(o) for o in outages}
        current_soft = {soft_fingerprint(o) for o in outages}

        cache_key = f"{otg_id}_{city_id or 0}"
        cache = outages_col.find_one({"_id": cache_key}) or {}
        prev_ids = set(cache.get("ids") or [])
        prev_fps = set(cache.get("fps") or [])
        prev_soft = set(cache.get("soft_fps") or [])

        # Нові за fingerprint (не за id)
        new_fps = current_fps - prev_fps
        new_outages = [o for o in outages if content_fingerprint(o) in new_fps]

        # План після вже відомої аварії на той самий soft_fp — не спамимо
        filtered: List[dict] = []
        for o in new_outages:
            soft = soft_fingerprint(o)
            fam = type_family(o.get("typeOff"))
            if fam == "planned" and soft in prev_soft:
                log.info(
                    "skip planned after known event soft=%s id=%s",
                    soft, o.get("id"),
                )
                continue
            filtered.append(o)
        new_outages = filtered

        outages_col.update_one(
            {"_id": cache_key},
            {
                "$set": {
                    "ids": list(current_ids),
                    "fps": list(current_fps),
                    "soft_fps": list(current_soft | prev_soft),  # накопичуємо soft
                    "updated_at": datetime.now(timezone.utc),
                    "count": len(current_ids),
                }
            },
            upsert=True,
        )

        # full docs у outages — для /check у боті
        for o in outages:
            doc = outage_to_mongo_doc(o)
            outages_full.update_one(
                {
                    "source": doc["source"],
                    "source_id": doc["source_id"],
                },
                {"$set": doc},
                upsert=True,
            )

        if not new_outages:
            log.info("otg=%s city=%s: без нових (fp)", otg_id, city_id)
            continue

        log.info(
            "otg=%s city=%s: %s нових (fp), ids_new=%s",
            otg_id, city_id, len(new_outages), len(current_ids - prev_ids),
        )

        for user, addr in group_pairs:
            street_id = addr.get("street_id")
            street_name = (addr.get("street_name") or "").lower()
            house = (
                addr.get("house")
                or addr.get("building")
                or addr.get("bud")
                or ""
            )
            tid = user["telegram_id"]
            for outage in new_outages:
                if not matches_street(outage, street_id, street_name):
                    continue
                if not matches_building(outage, str(house)):
                    continue
                notified.setdefault(tid, [])
                fp = content_fingerprint(outage)
                if any(content_fingerprint(x) == fp for x in notified[tid]):
                    continue
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

    log.info(
        "Готово: users=%s groups=%s sent=%s",
        len(users), groups_checked, sent,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
