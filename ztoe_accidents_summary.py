#!/usr/bin/env python3
"""
Погодинний звіт про аварійні відключення в Житомирській області (ЖОЕ).

Тягне всі записи з /pw_accidents?pagination=false,
рахує унікальні вулиці/населені пункти по РЕМ та ключових містах,
формує повідомлення і шле в Telegram-канал.
"""

from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

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

# Ключові міста для окремого блоку (порядок пріоритету)
KEY_CITIES = [
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
]

# Емодзі за кількістю вулиць (для РЕМ і міст)
def severity_emoji(streets: int) -> str:
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
    """Тягне всі аварії. Спочатку пробує pagination=false, інакше по сторінках."""
    if not API_BASE:
        raise SystemExit("Немає ZTOE_API_BASE / API_BASE")

    headers = {
        "Accept": "application/ld+json",
        "User-Agent": "ztoe-accidents-summary/1.0",
    }
    items: List[dict] = []

    with httpx.Client(timeout=60, headers=headers) as client:
        # 1) Спроба одним запитом
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
            log.warning("pagination=false failed: %s — переходимо на сторінки", e)

        # 2) Посторінково
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
    """
    Рахує:
    - unique streets overall
    - unique cities overall (і окремо без Житомира)
    - per REM: records, unique streets, unique cities
    - per city: unique streets
    """
    all_streets: Set[Any] = set()
    all_cities: Set[Any] = set()
    zhytomyr_streets: Set[Any] = set()

    # REM name → stats
    rem: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"records": 0, "streets": set(), "cities": set()}
    )
    # city name → unique street ids
    city_streets: Dict[str, Set[Any]] = defaultdict(set)

    for a in accidents:
        city = a.get("city") or {}
        street = a.get("street") or {}
        otg = a.get("otg") or {}

        cid = city.get("id")
        cname = (city.get("name") or "").strip() or "?"
        sid = street.get("id")
        oname = (otg.get("name") or "").strip() or "?"

        # нормалізуємо назву РЕМ (іноді приходить "АТ ...")
        if "Житомиробленерго" in oname and "РЕМ" not in oname:
            oname = "Інше (обленерго)"

        all_streets.add(sid if sid is not None else (cname, street.get("name")))
        all_cities.add(cid if cid is not None else cname)

        if cname == "Житомир":
            zhytomyr_streets.add(sid if sid is not None else street.get("name"))

        rem[oname]["records"] += 1
        rem[oname]["streets"].add(sid if sid is not None else street.get("name"))
        rem[oname]["cities"].add(cid if cid is not None else cname)
        city_streets[cname].add(sid if sid is not None else street.get("name"))

    # унікальні нас. пункти без Житомира
    cities_no_zh = len(all_cities) - (1 if any(
        (c.get("name") if isinstance(c, dict) else None) == "Житомир"
        or c == "Житомир"
        for c in []  # placeholder
    ) else 0)
    # простіше: якщо є Житомир у city_streets
    total_cities = len(all_cities)
    cities_except_zhytomyr = total_cities - (1 if "Житомир" in city_streets else 0)

    return {
        "total_records": len(accidents),
        "total_streets": len(all_streets),
        "total_cities": total_cities,
        "cities_except_zhytomyr": cities_except_zhytomyr,
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
    time_str = as_of.strftime("%d.%m.%Y %H:%M")

    total_cities = stats["total_cities"]
    total_streets = stats["total_streets"]

    lines = [
        f"⚡️ <b>Стан електромереж у Житомирській області</b>",
        f"(станом на {time_str})",
        "",
        f"Аварійні відключення зараз у <b>{total_cities}</b> населених пунктах "
        f"(загалом <b>{total_streets:,}</b> вулиць).".replace(",", " "),
        "",
        "⚠️ <b>Ситуація по РЕМ</b> (від найбільш проблемних):",
    ]

    # РЕМ за кількістю нас. пунктів, потім за вулицями
    rem_sorted = sorted(
        stats["rem"].items(),
        key=lambda x: (-x[1]["cities"], -x[1]["streets"]),
    )

    shown = 0
    for name, st in rem_sorted:
        if st["streets"] == 0:
            continue
        # прибираємо суфікс « РЕМ» для короткості, якщо є
        short = name.replace(" РЕМ", "").strip()
        emoji = severity_emoji(st["streets"])
        lines.append(
            f"{emoji} <b>{short}</b> — {st['cities']} нас.пунктів / {st['streets']:,} вулиць".replace(",", " ")
        )
        shown += 1
        if shown >= 12:  # щоб не роздувати повідомлення
            rest = len(rem_sorted) - shown
            if rest > 0:
                lines.append(f"<i>(…ще {rest} РЕМ)</i>")
            break

    lines.append("")
    lines.append("🏙 <b>По ключових містах</b> (від найбільш проблемних):")

    # ключові + всі міста з ≥ 20 вулицями
    city_items = []
    for name, cnt in stats["city_streets"].items():
        if name in KEY_CITIES or cnt >= 20:
            city_items.append((name, cnt))

    city_items.sort(key=lambda x: -x[1])

    for name, cnt in city_items[:15]:
        emoji = severity_emoji(cnt)
        prefix = "м. " if name in KEY_CITIES or name in (
            "Житомир", "Звягель", "Бердичів", "Овруч", "Коростень", "Малин"
        ) else ""
        lines.append(f"{emoji} {prefix}{name} — {cnt} вулиць")

    lines.append("")
    lines.append("<i>Дані: API Житомиробленерго · оновлення раз на годину</i>")

    return "\n".join(lines)


def main() -> int:
    log.info("Старт зведення аварій")
    try:
        accidents = fetch_all_accidents()
    except Exception as e:
        log.error("Не вдалося отримати дані: %s", e)
        return 1

    if not accidents:
        log.warning("Порожня відповідь API")
        msg = (
            "⚡️ <b>Стан електромереж у Житомирській області</b>\n"
            f"(станом на {datetime.now(KYIV).strftime('%d.%m.%Y %H:%M')})\n\n"
            "Зараз аварійних відключень за даними API немає."
        )
        send_notification(msg)
        return 0

    stats = aggregate(accidents)
    as_of = datetime.now(KYIV)
    msg = format_message(stats, as_of)

    log.info(
        "records=%s streets=%s cities=%s zhytomyr_streets=%s",
        stats["total_records"],
        stats["total_streets"],
        stats["total_cities"],
        stats["zhytomyr_streets"],
    )
    log.info("Повідомлення (%s символів):\n%s", len(msg), msg[:500])

    ok = send_notification(msg)
    if not ok:
        log.error("Не вдалося відправити в Telegram")
        return 1

    log.info("✓ Звіт відправлено")
    return 0


if __name__ == "__main__":
    sys.exit(main())
