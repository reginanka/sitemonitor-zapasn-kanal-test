#!/usr/bin/env python3
"""
Погодинний звіт про аварійні відключення в Житомирській області (ЖОЕ).

- Тягне всі записи з /pw_accidents
- Рахує унікальні вулиці / нас. пункти по РЕМ
- Під кожним РЕМ — топ населених пунктів (географія)
- Сортує від найгіршої ситуації
- Зберігає знімок у Mongo (колекція accidents_summary)
- Шле в Telegram-канал + лог у TELEGRAM_LOG_CHANNEL_ID
"""

from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

import httpx

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:
    import pytz
    KYIV = pytz.timezone("Europe/Kyiv")

def send_notification(message: str, image_path=None) -> bool:
    """Шле в TELEGRAM_CHANNEL_ID без preview лінків."""
    token = os.getenv("TELEGRAM_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID") or ""
    if not token or not chat_id:
        print("❌ TELEGRAM_BOT_TOKEN або TELEGRAM_CHANNEL_ID не налаштовані")
        return False
    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": message[:4096],
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
            timeout=30,
        )
        if r.status_code == 200:
            return True
        print(f"❌ Telegram send HTTP {r.status_code}: {r.text[:300]}")
        return False
    except Exception as e:
        print(f"❌ Telegram send failed: {e}")
        return False


# --- logging buffer (сумісно з log_utils + fallback) ---
_log_lines: list[str] = []

try:
    from log_utils import log_to_buffer as _lu_log, send_log_to_channel as _lu_send
    _HAS_LOG_UTILS = True
except Exception:
    _HAS_LOG_UTILS = False

    def _lu_log(msg: str) -> None:
        pass

    def _lu_send(title: str = "") -> None:
        pass


def log_to_buffer(msg: str) -> None:
    """Пише в консоль + буфер + log_utils (якщо є)."""
    print(msg)
    _log_lines.append(msg)
    try:
        _lu_log(msg)
    except Exception:
        pass


def send_log_to_channel(title: str = "📊 ЛОГ · АВАРІЇ ЖОЕ") -> None:
    """
    Шле лог у TELEGRAM_LOG_CHANNEL_ID.
    1) пробує log_utils
    2) fallback на httpx (якщо log_utils/requests злетіли)
    """
    try:
        if _HAS_LOG_UTILS:
            _lu_send(title)
    except Exception as e:
        print(f"log_utils send failed: {e}")

    token = os.getenv("TELEGRAM_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or ""
    if not token or not chat_id or not _log_lines:
        if not chat_id:
            print("⚠️ TELEGRAM_LOG_CHANNEL_ID не задано — лог у канал не піде")
        return

    try:
        body = "\n".join(_log_lines)
        text_msg = f"{title}\n\n<pre>{body}</pre>"
        if len(text_msg) > 4000:
            text_msg = text_msg[:3900] + "\n…</pre>"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        r = httpx.post(
            url,
            data={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
            timeout=15,
        )
        if r.status_code == 200:
            print(f"✓ Лог відправлено в {chat_id}")
        else:
            print(f"❌ Лог Telegram HTTP {r.status_code}: {r.text[:300]}")
    except Exception as e:
        print(f"❌ Лог fallback failed: {e}")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_accidents_summary")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or "").rstrip("/")
AVARII_ZHOE = (os.getenv("AVARII_ZHOE") or "").strip()
SUBSCRIBE = (os.getenv("SUBSCRIBE") or "").strip()

# скільки топ-населених пунктів показувати під кожним РЕМ
TOP_CITIES_PER_REM = 5

CITY_PREFIX = {
    "Житомир", "Звягель", "Бердичів", "Овруч", "Коростень", "Малин",
}


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


def city_label(name: str) -> str:
    if name in CITY_PREFIX:
        return f"м. {name}"
    return name


def fetch_all_accidents() -> List[dict]:
    if not API_BASE:
        raise SystemExit("Немає ZTOE_API_BASE")

    headers = {
        "Accept": "application/ld+json",
        "User-Agent": "ztoe-accidents-summary/1.3",
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
                log_to_buffer(f"API: list → {len(data)} записів")
                return data
            batch = data.get("hydra:member") or []
            if batch:
                log_to_buffer(f"API: pagination=false → {len(batch)} записів")
                return batch
        except Exception as e:
            log_to_buffer(f"⚠️ pagination=false failed: {e} — сторінки")

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
        log_to_buffer(f"API: по сторінках → {len(items)} записів (сторінок: {page})")

    return items


def aggregate(accidents: List[dict]) -> Dict[str, Any]:
    """
    rem[name] = {
      records, streets(set), cities(set),
      city_streets: {city_name: set(street_keys)}
    }
    """
    all_streets: Set[Any] = set()
    all_cities: Set[Any] = set()
    zhytomyr_streets: Set[Any] = set()

    rem: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "records": 0,
            "streets": set(),
            "cities": set(),
            "city_streets": defaultdict(set),
        }
    )

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
        rem[oname]["city_streets"][cname].add(street_key)

    # серіалізація для Mongo / форматування
    rem_out: Dict[str, Any] = {}
    for name, st in rem.items():
        city_counts = {
            c: len(sids) for c, sids in st["city_streets"].items()
        }
        rem_out[name] = {
            "records": st["records"],
            "streets": len(st["streets"]),
            "cities": len(st["cities"]),
            "city_streets": city_counts,
        }

    return {
        "total_records": len(accidents),
        "total_streets": len(all_streets),
        "total_cities": len(all_cities),
        "zhytomyr_streets": len(zhytomyr_streets),
        "rem": rem_out,
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
        "⚠️ <b>Ситуація по РЕМ</b> (від найбільш проблемних):",
    ]

    # сортування РЕМ: вулиці → нас. пункти → records
    rem_sorted = sorted(
        stats["rem"].items(),
        key=lambda x: (-x[1]["streets"], -x[1]["cities"], -x[1]["records"]),
    )

    block: List[str] = []
    for name, st in rem_sorted:
        if st["streets"] == 0:
            continue
        short = name.replace(" РЕМ", "").strip()
        emoji = severity_emoji(st["streets"])
        block.append(
            f"{emoji} <b>{short}</b> — {st['cities']} нас.п. / "
            f"{st['streets']:,} вул.".replace(",", " ")
        )

        # топ населених пунктів у цьому РЕМ (географія)
        cities_sorted = sorted(
            st.get("city_streets", {}).items(),
            key=lambda x: -x[1],
        )[:TOP_CITIES_PER_REM]
        for cname, cnt in cities_sorted:
            block.append(f"    · {city_label(cname)} — {cnt}")

    if block:
        lines.append("<blockquote expandable>")
        lines.extend(block)
        lines.append("</blockquote>")
    else:
        lines.append("<i>немає даних</i>")

    lines.append("")

    # футер
    footer_parts: List[str] = []
    if AVARII_ZHOE:
        footer_parts.append(f'<a href="{AVARII_ZHOE}">🔗 Сайт "ЖОЕ"</a>')
    if SUBSCRIBE:
        footer_parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
    if footer_parts:
        lines.append("  ".join(footer_parts))
    lines.append(f"🕐 {time_short}")

    return "\n".join(lines)


def save_to_mongo(stats: Dict[str, Any], as_of: datetime, message: str) -> None:
    uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not uri:
        log_to_buffer("⚠️ Немає MONGO_URI — пропускаємо запис у Mongo")
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

        # city_streets вкладені — ок для Mongo
        doc = {
            "_id": "latest",
            "as_of": as_of.isoformat(),
            "as_of_utc": datetime.now(timezone.utc),
            "total_records": stats["total_records"],
            "total_streets": stats["total_streets"],
            "total_cities": stats["total_cities"],
            "zhytomyr_streets": stats["zhytomyr_streets"],
            "rem": stats["rem"],
            "message": message,
            "updated_at": datetime.now(timezone.utc),
        }
        col.update_one({"_id": "latest"}, {"$set": doc}, upsert=True)

        history_id = as_of.strftime("%Y-%m-%dT%H")
        col.update_one(
            {"_id": history_id},
            {"$set": {**{k: v for k, v in doc.items() if k != "_id"}, "_id": history_id}},
            upsert=True,
        )
        log_to_buffer(f"🗄 Mongo: {db_name}.accidents_summary (latest + {history_id})")
    except Exception as e:
        log_to_buffer(f"❌ Mongo write failed: {e}")


def main() -> int:
    log_to_buffer("=" * 40)
    log_to_buffer("🚀 СТАРТ · Зведення аварій ЖОЕ (погодинний)")
    log_to_buffer(f"ZTOE_API_BASE={'задано' if API_BASE else 'НЕМАЄ'}")
    log_to_buffer(f"AVARII_ZHOE={'є' if AVARII_ZHOE else 'немає'} · SUBSCRIBE={'є' if SUBSCRIBE else 'немає'}")

    try:
        accidents = fetch_all_accidents()
    except Exception as e:
        log_to_buffer(f"❌ Fetch failed: {e}")
        send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ (помилка)")
        return 1

    as_of = datetime.now(KYIV)

    if not accidents:
        log_to_buffer("⚠️ Порожня відповідь API")
        msg = (
            "⚡️ <b>Стан електромереж у Житомирській області</b>\n"
            f"(станом на {as_of.strftime('%d.%m.%Y %H:%M')})\n\n"
            "Зараз аварійних відключень за даними API немає."
        )
        if AVARII_ZHOE or SUBSCRIBE:
            parts = []
            if AVARII_ZHOE:
                parts.append(f'<a href="{AVARII_ZHOE}">Чому немає світла?|ЖОЕ</a>')
            if SUBSCRIBE:
                parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
            msg += "\n\n" + "  ".join(parts)
            msg += f"\n🕐 {as_of.strftime('%H:%M %d.%m')}"
        ok = send_notification(msg)
        log_to_buffer(f"Telegram (empty): {'✓' if ok else '✗'}")
        send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ")
        return 0

    stats = aggregate(accidents)
    msg = format_message(stats, as_of)

    log_to_buffer(
        f"📊 records={stats['total_records']} "
        f"streets={stats['total_streets']} "
        f"cities={stats['total_cities']} "
        f"zhytomyr={stats['zhytomyr_streets']}"
    )
    top_rem = sorted(
        stats["rem"].items(),
        key=lambda x: (-x[1]["streets"], -x[1]["cities"]),
    )[:5]
    for name, st in top_rem:
        top_cities = sorted(st.get("city_streets", {}).items(), key=lambda x: -x[1])[:3]
        cities_s = ", ".join(f"{c}:{n}" for c, n in top_cities)
        log_to_buffer(
            f"  REM {name}: streets={st['streets']} cities={st['cities']} [{cities_s}]"
        )

    save_to_mongo(stats, as_of, msg)

    ok = send_notification(msg)
    if ok:
        log_to_buffer(f"✓ Telegram: звіт відправлено ({len(msg)} символів)")
    else:
        log_to_buffer("❌ Telegram: не вдалося відправити")

    send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
