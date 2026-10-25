#!/usr/bin/env python3
"""
Аварійні / планові відключення ЖОЕ → підписники бота.

- Усі сторінки API
- Дедуп по fingerprint (не спам на новий id)
- План після відомої аварії не дублює
- Full docs у Mongo outages (+ queue_key) → /check і PNG (planned/emerg)
- При новій події: перегенерація PNG черги + повідомлення з картинкою
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
from telegram import Bot, InputMediaPhoto
from telegram import InputFile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_outages_monitor")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or os.getenv("API_BASE") or "").rstrip("/")
if not API_BASE:
    raise SystemExit("Немає ZTOE_API_BASE")

TYPE_LABELS = {
    0: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
    1: ("Аварійне", "несподівана аварія в мережі"),
    2: ("Оперативне", "технічні перемикання в мережі"),
    3: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
}


def type_family(type_off) -> str:
    try:
        t = int(type_off)
    except (TypeError, ValueError):
        return "x"
    if t == 1:
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
    items: List[dict] = []
    page = 1
    with httpx.Client(
        timeout=30,
        headers={
            "Accept": "application/ld+json",
            "User-Agent": "ztoe-outages-monitor/1.2",
        },
    ) as client:
        while page <= 40:
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
            if not (data.get("hydra:view") or {}).get("hydra:next") or not batch:
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
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    start = (outage.get("dateEvent") or "")[:13]
    fam = type_family(outage.get("typeOff"))
    city = (outage.get("city") or {}).get("id") or ""
    return f"{city}|{sid}|{','.join(parts)}|{start}|{fam}"


def soft_fingerprint(outage: dict) -> str:
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    start = (outage.get("dateEvent") or "")[:13]
    city = (outage.get("city") or {}).get("id") or ""
    return f"{city}|{sid}|{','.join(parts)}|{start}"


def format_outage(outage: dict) -> str:
    city = (outage.get("city") or {}).get("name", "?")
    street = (outage.get("street") or {}).get("name", "?")
    buildings = (outage.get("buildingNames") or "").strip()
    date_event = (outage.get("dateEvent") or "")[:16].replace("T", " ")
    date_plan = (outage.get("datePlanIn") or "")[:16].replace("T", " ")
    type_off = outage.get("typeOff")
    type_name, type_desc = TYPE_LABELS.get(type_off, (f"Тип {type_off}", ""))
    icon = {0: "🟠", 1: "🔴", 2: "🟡", 3: "🟠"}.get(type_off, "⚪")
    text = f"{icon} <b>{city}, {street}</b>\n📌 {type_name}"
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
    house = (house or "").strip()
    if not house:
        return True
    buildings = (outage.get("buildingNames") or "").strip()
    if not buildings:
        return True
    tokens = {p.strip().lower() for p in buildings.replace(";", ",").split(",") if p.strip()}
    h = house.lower()
    if h in tokens:
        return True
    h2 = h.lstrip("0") or "0"
    return h2 in tokens or any(t.lstrip("0") == h2 for t in tokens)


def outage_to_mongo_doc(outage: dict, queue_key: str = "") -> dict:
    from outage_events import parse_queue_from_koment

    city = outage.get("city") or {}
    street = outage.get("street") or {}
    otg = outage.get("otg") or {}
    qk = queue_key or parse_queue_from_koment(outage.get("koment") or "") or ""
    ch, pid = "", ""
    if qk:
        parts = qk.replace("|", ".").split(".")
        ch = parts[0]
        pid = parts[1] if len(parts) > 1 else ""
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
        "queue_key": qk,
        "queue": ch,
        "subqueue": pid,
        "fingerprint": content_fingerprint(outage),
        "soft_fp": soft_fingerprint(outage),
        "updated_at": datetime.now(timezone.utc),
    }


def user_queue_key(addr: dict) -> str:
    q = (addr.get("queue") or "").strip()
    sq = (addr.get("subqueue") or "").strip()
    if not q:
        return ""
    if "." in q or "|" in q:
        return q.replace("|", ".")
    return f"{q}.{sq}" if sq else q


def regenerate_queue_png(queue_key: str) -> bytes | None:
    """Перемалювати PNG черги з ГПВ + planned/emerg з outages."""
    try:
        from mongo_store import get_db, get_schedule_png, save_schedule_png
        from regenerate_images import load_ukrenergo_status, regenerate_one
    except Exception as e:
        log.warning("regenerate import: %s", e)
        return None

    db = get_db()
    ch, _, *rest = (queue_key.replace("|", ".") + ".").split(".")
    pid = rest[0] if rest and rest[0] else ""
    # parts properly
    parts = queue_key.replace("|", ".").split(".")
    ch = parts[0]
    pid = parts[1] if len(parts) > 1 else ""

    doc = None
    for _id in (f"{ch}|{pid}" if pid else ch, f"{ch}.{pid}" if pid else ch, ch):
        doc = db["queue_schedules"].find_one({"_id": _id})
        if doc:
            break
    if not doc:
        doc = {"queue": ch, "subqueue": pid, "slots": [], "events": [], "known": True}

    ukr = load_ukrenergo_status(db)
    status = regenerate_one(doc, ukr, dry_run=False, only_missing=False)
    if status != "ok":
        log.warning("regenerate %s → %s", queue_key, status)
    return get_schedule_png(ch, pid) or get_schedule_png(queue_key, "")


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
            groups.setdefault((otg_id, city_id), []).append((u, addr))

    log.info("users=%s groups=%s", len(users), len(groups))
    bot = Bot(token=bot_token)
    notified: Dict[int, list] = {}
    queues_to_regen: set = set()
    groups_checked = 0

    for (otg_id, city_id), group_pairs in groups.items():
        try:
            outages = fetch_all_outages(otg_id, city_id if city_id else None)
        except Exception as e:
            log.error("Fetch otg=%s city=%s: %s", otg_id, city_id, e)
            continue

        groups_checked += 1
        current_ids = {make_outage_key(o) for o in outages}
        current_fps = {content_fingerprint(o) for o in outages}
        current_soft = {soft_fingerprint(o) for o in outages}

        cache_key = f"{otg_id}_{city_id or 0}"
        cache = outages_col.find_one({"_id": cache_key}) or {}
        prev_fps = set(cache.get("fps") or [])
        prev_soft = set(cache.get("soft_fps") or [])

        new_fps = current_fps - prev_fps
        new_outages = [o for o in outages if content_fingerprint(o) in new_fps]

        filtered = []
        for o in new_outages:
            soft = soft_fingerprint(o)
            if type_family(o.get("typeOff")) == "planned" and soft in prev_soft:
                log.info("skip planned after known soft=%s", soft)
                continue
            filtered.append(o)
        new_outages = filtered

        outages_col.update_one(
            {"_id": cache_key},
            {
                "$set": {
                    "ids": list(current_ids),
                    "fps": list(current_fps),
                    "soft_fps": list(current_soft | prev_soft),
                    "updated_at": datetime.now(timezone.utc),
                    "count": len(current_ids),
                }
            },
            upsert=True,
        )

        for o in outages:
            doc = outage_to_mongo_doc(o)
            outages_full.update_one(
                {"source": doc["source"], "source_id": doc["source_id"]},
                {"$set": doc},
                upsert=True,
            )

        if not new_outages:
            continue

        log.info("otg=%s city=%s new=%s", otg_id, city_id, len(new_outages))

        # Прив'язка queue_key + PNG для ВСІХ поточних outages по адресах
        # (навіть якщо сповіщення не шлемо — щоб /check і картинки були актуальні)
        for user, addr in group_pairs:
            street_id = addr.get("street_id")
            street_name = (addr.get("street_name") or "").lower()
            house = addr.get("house") or addr.get("building") or addr.get("bud") or ""
            uqk = user_queue_key(addr)
            tid = user["telegram_id"]
            for outage in outages:
                if not matches_street(outage, street_id, street_name):
                    continue
                if not matches_building(outage, str(house)):
                    continue
                if uqk:
                    doc = outage_to_mongo_doc(outage, queue_key=uqk)
                    outages_full.update_one(
                        {"source": doc["source"], "source_id": doc["source_id"]},
                        {"$set": doc},
                        upsert=True,
                    )
                    outage["_user_queue"] = uqk
                    queues_to_regen.add(uqk)
            # сповіщення лише по new_outages
            for outage in new_outages:
                if not matches_street(outage, street_id, street_name):
                    continue
                if not matches_building(outage, str(house)):
                    continue
                if uqk:
                    outage["_user_queue"] = uqk
                notified.setdefault(tid, [])
                fp = content_fingerprint(outage)
                if any(content_fingerprint(x) == fp for x in notified[tid]):
                    continue
                notified[tid].append(outage)

    # перемалювати PNG для черг, де з'явились outages
    png_by_queue: Dict[str, bytes] = {}
    for qk in queues_to_regen:
        try:
            png = regenerate_queue_png(qk)
            if png:
                png_by_queue[qk] = png
                log.info("PNG оновлено для %s (%s bytes)", qk, len(png))
        except Exception as e:
            log.warning("PNG %s: %s", qk, e)

    sent = 0
    for tid, outage_list in notified.items():
        text = "⚠️ <b>Нові відключення по твоїх адресах:</b>\n\n"
        text += "\n\n".join(format_outage(o) for o in outage_list[:10])
        caption = text if len(text) <= 1024 else text[:1000] + "\n…"

        # унікальні черги з цієї пачки
        qkeys = []
        for o in outage_list:
            qk = o.get("_user_queue") or ""
            if qk and qk not in qkeys:
                qkeys.append(qk)
        images = [png_by_queue[q] for q in qkeys if q in png_by_queue]

        async def _send(chat_id=tid, body=text, cap=caption, imgs=images):
            if imgs:
                from io import BytesIO
                media = []
                for i, png in enumerate(imgs[:10]):
                    bio = BytesIO(png)
                    bio.name = f"schedule_{i}.png"
                    media.append(
                        InputMediaPhoto(
                            media=bio,
                            caption=cap if i == 0 else None,
                            parse_mode="HTML" if i == 0 else None,
                        )
                    )
                await bot.send_media_group(chat_id=chat_id, media=media)
                if len(body) > 1024:
                    await bot.send_message(
                        chat_id=chat_id, text=body[:4096], parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
            else:
                await bot.send_message(
                    chat_id=chat_id, text=body[:4096], parse_mode="HTML",
                    disable_web_page_preview=True,
                )

        try:
            asyncio.run(_send())
            sent += 1
            log.info("Надіслано %s (outages=%s images=%s)", tid, len(outage_list), len(images))
            time.sleep(0.4)
        except Exception as e:
            log.error("Send fail %s: %s", tid, e)

    # короткий звіт у лог (і опційно в ADMIN_CHAT_ID)
    summary = (
        f"✅ Перевірка відключень\n"
        f"Підписників: {len(users)}\n"
        f"Локацій: {groups_checked}\n"
        f"Сповіщень: {sent}\n"
        f"PNG оновлено: {len(png_by_queue)}"
    )
    log.info("%s", summary.replace("\n", " | "))
    admin = os.getenv("ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if admin and (sent or png_by_queue):
        try:
            async def _adm():
                await bot.send_message(chat_id=int(admin), text=summary)
            asyncio.run(_adm())
        except Exception as e:
            log.warning("admin summary: %s", e)

    log.info("Готово groups=%s sent=%s regen=%s", groups_checked, sent, len(png_by_queue))
    return 0


if __name__ == "__main__":
    sys.exit(main())
