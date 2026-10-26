#!/usr/bin/env python3
"""
Погодинний звіт про аварійні відключення в Житомирській області (ЖОЕ).

- Тягне всі записи з /pw_accidents
- Рахує унікальні вулиці / нас. пункти по РЕМ і ключових містах
- Сортує від найгіршої ситуації
- Зберігає знімок у Mongo (колекція accidents_summary, БД vidkluchennya)
- Шле в Telegram-канал
"""

from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

import httpx

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:
    import pytz
    KYIV = pytz.timezone("Europe/Kyiv")

from telegram_handler import send_notification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_accidents_summary")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or os.getenv("API_BASE") or "").rstrip("/")
URL = (os.getenv("URL") or "").strip()
SUBSCRIBE = (os.getenv("SUBSCRIBE") or "").strip()

# Ключові міста (завжди в блоці, якщо є дані)
KEY_CITIES = {
    "Житомир",
    "Звягель",
    "Бердичів",
    "Овруч",
    "Коростень",
    "Малин",
    "Коростишів",
    "Радомишль",
    "Олевськ",
    "Андрушівка",
    "Чуднів",
    "Баранівка",
}

CITY_PREFIX = {
    "Житомир", "Звягель", "Бердичів", "Овруч", "Коростень", "Малин",
}


def severity_emoji(streets: int) -> str:
    """Колір за кількістю вулиць (чим більше — тим гірше)."""
    if streets >= 400:
        return "🔴"
    if streets >= 150:
        return "🟠"
    if streets >= 50:
        return "🟡"
    if streets > 0:
        return "🟢"
    return "⚪"


def fetch_all_accidents() -> List[dict]:
    """Тягне всі аварії. Спочатку pagination=false, інакше по сторінках."""
    if not API_BASE:
        raise SystemExit("Немає ZTOE_API_BASE / API_BASE")

    headers = {
        "Accept": "application/ld+json",
        "User-Agent": "ztoe-accidents-summary/1.1",
    }
    items: List[dict] = []

    with httpx.Client(timeout=60, headers=headers) as client:
        try:
            r = client.get(
                f"{API_BASE}/pw_accidents",
                params={"pagination": "false"},
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return data
            batch = data.get("hydra:member") or []
            if batch:
                log.info("pagination=false → %s записів", len(batch))
                return batch
        except Exception as e:
            log.warning("pagination=false failed: %s — сторінки", e)

        page = 1
        while page <= 100:
            r = client.get(
                f"{API_BASE}/pw_accidents",
                params={"page": page, "itemsPerPage": 100},
            )
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
        log.info("по сторінках → %s записів (сторінок: %s)", len(items), page)

    return items


def aggregate(accidents: List[dict]) -> Dict[str, Any]:
    all_streets: Set[Any] = set()
    all_cities: Set[Any] = set()
    zhytomyr_streets: Set[Any] = set()

    rem: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"records": 0, "streets": set(), "cities": set()}
    )
    city_streets: Dict[str, Set[Any]] = defaultdict(set)

    for a in accidents:
        city = a.get("city") or {}
        street = a.get("street") or {}
        otg = a.get("otg") or {}

        cid = city.get("id")
        cname = (city.get("name") or "").strip() or "?"
        sid = street.get("id")
        oname = (otg.get("name") or "").strip() or "?"

        if "Житомиробленерго" in oname and "РЕМ" not in oname:
            oname = "Інше (обленерго)"

        street_key = sid if sid is not None else (cname, street.get("name"))
        city_key = cid if cid is not None else cname

        all_streets.add(street_key)
        all_cities.add(city_key)

        if cname == "Житомир":
            zhytomyr_streets.add(street_key)

        rem[oname]["records"] += 1
        rem[oname]["streets"].add(street_key)
        rem[oname]["cities"].add(city_key)
        city_streets[cname].add(street_key)

    return {
        "total_records": len(accidents),
        "total_streets": len(all_streets),
        "total_cities": len(all_cities),
        "zhytomyr_streets": len(zhytomyr_streets),
        "rem": {
            name: {
                "records": st["records"],
                "streets": len(st["streets"]),
                "cities": len(st["cities"]),
            }
            for name, st in rem.items()
        },
        "city_streets": {name: len(sids) for name, sids in city_streets.items()},
    }


def format_message(stats: Dict[str, Any], as_of: datetime) -> str:
    time_full = as_of.strftime("%d.%m.%Y %H:%M")
    time_short = as_of.strftime("%H:%M %d.%m")

    total_cities = stats["total_cities"]
    total_streets = stats["total_streets"]

    lines: List[str] = [
        "⚡️ <b>Стан електромереж у Житомирській області</b>",
        f"(станом на {time_full})",
        "",
        (
            f"Аварійні відключення зараз у <b>{total_cities}</b> населених пунктах "
            f"(загалом <b>{total_streets:,}</b> вулиць)."
        ).replace(",", " "),
        "",
    ]

    # --- РЕМ: сортуємо від найгіршої (спочатку вулиці, потім нас. пункти) ---
    rem_sorted = sorted(
        stats["rem"].items(),
        key=lambda x: (-x[1]["streets"], -x[1]["cities"], -x[1]["records"]),
    )
    rem_lines: List[str] = []
    for name, st in rem_sorted:
        if st["streets"] == 0:
            continue
        short = name.replace(" РЕМ", "").strip()
        emoji = severity_emoji(st["streets"])
        rem_lines.append(
            f"{emoji} <b>{short}</b> — {st['cities']} нас.пунктів / "
            f"{st['streets']:,} вулиць".replace(",", " ")
        )

    lines.append("⚠️ <b>Ситуація по РЕМ</b> (від найбільш проблемних):")
    if rem_lines:
        # expandable blockquote — у Telegram клієнтах згортається стрілкою
        lines.append("<blockquote expandable>")
        lines.extend(rem_lines)
        lines.append("</blockquote>")
    else:
        lines.append("<i>немає даних</i>")

    lines.append("")

    # --- Ключові міста (тільки KEY_CITIES, від найгіршої) ---
    city_items = [
        (name, cnt)
        for name, cnt in stats["city_streets"].items()
        if name in KEY_CITIES and cnt > 0
    ]
    city_items.sort(key=lambda x: -x[1])

    city_lines: List[str] = []
    for name, cnt in city_items:
        emoji = severity_emoji(cnt)
        prefix = "м. " if name in CITY_PREFIX or name in KEY_CITIES else ""
        city_lines.append(f"{emoji} {prefix}{name} — {cnt} вулиць")

    lines.append("🏙 <b>По ключових містах</b> (від найбільш проблемних):")
    if city_lines:
        lines.append("<blockquote expandable>")
        lines.extend(city_lines)
        lines.append("</blockquote>")
    else:
        lines.append("<i>немає даних</i>")

    lines.append("")

    # --- Футер як у інших моніторах ---
    footer_parts: List[str] = []
    if URL:
        footer_parts.append(f'<a href="{URL}">🔗 Сайт "ЖОЕ"</a>')
    if SUBSCRIBE:
        footer_parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
    if footer_parts:
        lines.append(" | ".join(footer_parts))
    lines.append(f"🕐 {time_short}")

    return "\n".join(lines)


def save_to_mongo(stats: Dict[str, Any], as_of: datetime, message: str) -> None:
    """
    Зберігає знімок у нову колекцію accidents_summary.
    БД: vidkluchennya (або ZTOE_MONGO_DB / MONGO_DB).
    Не чіпає users, outages, queue_schedules тощо.
    """
    uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not uri:
        log.warning("Немає MONGO_URI — пропускаємо запис у Mongo")
        return

    db_name = (
        os.getenv("ZTOE_MONGO_DB")
        or os.getenv("MONGO_DB")
        or os.getenv("MAYACHOK_MONGO_DB")
        or "vidkluchennya"
    )

    try:
        from pymongo import MongoClient

        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        db = client[db_name]
        col = db["accidents_summary"]

        doc = {
            "_id": "latest",  # завжди один актуальний документ
            "as_of": as_of.isoformat(),
            "as_of_utc": datetime.now(timezone.utc),
            "total_records": stats["total_records"],
            "total_streets": stats["total_streets"],
            "total_cities": stats["total_cities"],
            "zhytomyr_streets": stats["zhytomyr_streets"],
            "rem": stats["rem"],
            "city_streets": stats["city_streets"],
            "message": message,
            "updated_at": datetime.now(timezone.utc),
        }
        col.update_one({"_id": "latest"}, {"$set": doc}, upsert=True)

        # також історія (опційно, по годинах)
        history_id = as_of.strftime("%Y-%m-%dT%H")
        col.update_one(
            {"_id": history_id},
            {
                "$set": {
                    **{k: v for k, v in doc.items() if k != "_id"},
                    "_id": history_id,
                }
            },
            upsert=True,
        )
        log.info("Mongo: збережено в %s.accidents_summary (latest + %s)", db_name, history_id)
    except Exception as e:
        log.error("Mongo write failed: %s", e)


def main() -> int:
    log.info("Старт зведення аварій")
    try:
        accidents = fetch_all_accidents()
    except Exception as e:
        log.error("Не вдалося отримати дані: %s", e)
        return 1

    as_of = datetime.now(KYIV)

    if not accidents:
        log.warning("Порожня відповідь API")
        msg = (
            "⚡️ <b>Стан електромереж у Житомирській області</b>\n"
            f"(станом на {as_of.strftime('%d.%m.%Y %H:%M')})\n\n"
            "Зараз аварійних відключень за даними API немає."
        )
        if URL or SUBSCRIBE:
            parts = []
            if URL:
                parts.append(f'<a href="{URL}">🔗 Сайт "ЖОЕ"</a>')
            if SUBSCRIBE:
                parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
            msg += "\n\n" + " | ".join(parts)
            msg += f"\n🕐 {as_of.strftime('%H:%M %d.%m')}"
        send_notification(msg)
        return 0

    stats = aggregate(accidents)
    msg = format_message(stats, as_of)

    log.info(
        "records=%s streets=%s cities=%s zhytomyr_streets=%s",
        stats["total_records"],
        stats["total_streets"],
        stats["total_cities"],
        stats["zhytomyr_streets"],
    )

    # Mongo (нова колекція, існуючі не чіпаємо)
    save_to_mongo(stats, as_of, msg)

    ok = send_notification(msg)
    if not ok:
        log.error("Не вдалося відправити в Telegram")
        return 1

    log.info("✓ Звіт відправлено (%s символів)", len(msg))
    return 0


if __name__ == "__main__":
    sys.exit(main())
