#!/usr/bin/env python3
"""
Звіт про аварійні відключення в Житомирській області (ЖОЕ).

- Тягне всі записи з /pw_accidents
- Рахує унікальні вулиці / нас. пункти по РЕМ
- Під кожним РЕМ — топ населених пунктів (географія)
- Завжди зберігає знімок у Mongo (accidents_summary: latest + історія по годині)
- У Telegram-канал шле ЛИШЕ при рівні CRITICAL
  (антиспам: last_notified_date / last_notified_level у Mongo)
- FORCE_NOTIFY=1 — примусово в канал
- Лог у TELEGRAM_LOG_CHANNEL_ID

Рівні (за кількістю н.п.):
  NORM <30 | WARN 30–150 | HIGH 150–400 | CRITICAL >400
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



# Рівень по кількості населених пунктів (область ~1400 н.п.)
# 🟢 NORM     < 30
# 🟡 WARN     30–150
# 🟠 HIGH     150–400
# 🔴 CRITICAL > 400
LEVEL_ORDER = {"NORM": 0, "WARN": 1, "HIGH": 2, "CRITICAL": 3}


def level_from_cities(total_cities: int) -> Tuple[str, str, str]:
    """Повертає (code, emoji, label_uk).

    Код (NORM/WARN/HIGH/CRITICAL) — для антиспаму в Mongo.
    label_uk — для повідомлення в канал (варіант A).
    """
    n = int(total_cities or 0)
    if n > 400:
        return "CRITICAL", "🔴", "КРИТИЧНИЙ"
    if n >= 150:
        return "HIGH", "🟠", "СКЛАДНИЙ"
    if n >= 30:
        return "WARN", "🟡", "Погіршення стану"
    return "NORM", "🟢", "Стабільний"


def get_mongo_col():
    """Колекція accidents_summary або None."""
    uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not uri:
        return None
    db_name = (
        os.getenv("ZTOE_MONGO_DB")
        or os.getenv("MONGO_DB")
        or os.getenv("MAYACHOK_MONGO_DB")
        or "vidkluchennya"
    )
    try:
        from pymongo import MongoClient

        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        return client[db_name]["accidents_summary"]
    except Exception as e:
        log_to_buffer(f"⚠️ Mongo connect failed: {e}")
        return None


def load_latest_summary() -> dict | None:
    col = get_mongo_col()
    if col is None:
        return None
    try:
        return col.find_one({"_id": "latest"})
    except Exception as e:
        log_to_buffer(f"⚠️ Mongo read latest failed: {e}")
        return None


def should_notify_channel(
    level: str,
    prev: dict | None,
    as_of: datetime,
) -> Tuple[bool, str]:
    """
    Mongo — завжди. Канал — без щогодинного спаму.

    1) FORCE_NOTIFY=1/true/yes — завжди слати
    2) Рівень CRITICAL і ескалація (або перше за день) — слати
    3) Всі інші рівні (NORM / WARN / HIGH) — у канал не слати
    """
    force = (os.getenv("FORCE_NOTIFY") or "").strip().lower() in ("1", "true", "yes")
    if force:
        return True, "FORCE_NOTIFY"

    rank = LEVEL_ORDER.get(level, 0)
    today = as_of.strftime("%Y-%m-%d")
    prev = prev or {}

    last_date = str(prev.get("last_notified_date") or "")[:10]
    last_notified_level = prev.get("last_notified_level") or ""
    last_rank = (
        LEVEL_ORDER.get(last_notified_level, -1) if last_notified_level else -1
    )

    # Тільки CRITICAL
    if rank < LEVEL_ORDER["CRITICAL"]:
        return False, f"рівень {level}, у канал не шлемо (тільки CRITICAL)"

    # Ескалація до CRITICAL (навіть якщо вже писали сьогодні)
    if rank > last_rank:
        return True, f"ескалація {last_notified_level or '—'} → {level}"

    # Сьогодні вже є сповіщення — не спамимо
    if last_date == today and last_rank >= 0:
        return False, f"вже сповіщали сьогодні ({last_notified_level}, {last_date})"

    # Перше за день при CRITICAL
    return True, f"перше сповіщення за день, рівень {level}"


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



def is_real_accident(doc: dict) -> bool:
    """
    True лише для справжніх аварій / ГАВ / СГАВ.
    ГПВ (графік погодинних) і явні згадки ГПВ у коментарі — відкидаємо.

    Логіка як у bot_notify._classify_outage:
      switchSubReason: 1=ГАВ, 2=ГПВ(ігнор), 3=СГАВ
      typeOff (якщо sub==0): 1=аварійне
    """
    koment = (doc.get("koment") or doc.get("comment") or "").upper()
    if "ГПВ" in koment:
        return False

    # текстовий тип з API / таблиці
    type_name = (
        doc.get("type_name")
        or doc.get("typeName")
        or doc.get("typeOffName")
        or ""
    )
    if isinstance(type_name, str):
        tn = type_name.lower()
        if "погодин" in tn or "гпв" in tn:
            return False

    try:
        sub = int(doc.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        sub = 0
    try:
        t = int(doc.get("typeOff")) if doc.get("typeOff") is not None else -1
    except (TypeError, ValueError):
        t = -1

    if sub == 2:
        return False  # ГПВ
    if sub in (1, 3):
        return True   # ГАВ / СГАВ
    if sub > 0:
        # невідомий sub — не рахуємо як аварію
        return False

    # sub == 0 → дивимось typeOff
    return t == 1


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
                filtered = [x for x in data if is_real_accident(x)]
                log_to_buffer(
                    f"Фільтр аварій: {len(data)} → {len(filtered)} "
                    f"(відкинуто ГПВ/не-аварії: {len(data) - len(filtered)})"
                )
                return filtered
            batch = data.get("hydra:member") or []
            if batch:
                log_to_buffer(f"API: pagination=false → {len(batch)} записів")
                filtered = [x for x in batch if is_real_accident(x)]
                log_to_buffer(
                    f"Фільтр аварій: {len(batch)} → {len(filtered)} "
                    f"(відкинуто ГПВ/не-аварії: {len(batch) - len(filtered)})"
                )
                return filtered
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

    raw_n = len(items)
    items = [a for a in items if is_real_accident(a)]
    log_to_buffer(
        f"Фільтр аварій: {raw_n} → {len(items)} "
        f"(відкинуто ГПВ/не-аварії: {raw_n - len(items)})"
    )
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


def format_message(
    stats: Dict[str, Any],
    as_of: datetime,
    level_meta: Tuple[str, str, str] | None = None,
) -> str:
    time_full = as_of.strftime("%d.%m.%Y %H:%M")
    time_short = as_of.strftime("%H:%M %d.%m")

    total_cities = stats["total_cities"]
    total_streets = stats["total_streets"]

    if level_meta is None:
        level_meta = level_from_cities(total_cities)
    _level_code, level_emoji, level_label = level_meta

    lines: List[str] = [
        f"{level_emoji}<b>{level_label}</b> стан електромереж у Житомирській області",
        "",
        (
            f"Аварійні відключення зараз у <b>{total_cities}</b> населених пунктах "
            f"(загалом <b>{total_streets:,}</b> вулиць)."
        ).replace(",", " "),
        "",
        "⚠️ <b>Ситуація по РЕМ</b> (від найбільш проблемних):",
    ]

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

    # Лише для CRITICAL — заклик після блоку РЕМ, перед лінками
    if _level_code == "CRITICAL":
        lines.append("<i>Будь ласка, не користуйтесь потужними електроприладами.</i>")
        lines.append("")

    footer_parts: List[str] = []
    if AVARII_ZHOE:
        footer_parts.append(f'<a href="{AVARII_ZHOE}">🔗 Сайт "ЖОЕ"</a>')
    if SUBSCRIBE:
        footer_parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
    if footer_parts:
        lines.append("  ".join(footer_parts))
    lines.append(f"🕐 {time_short}")

    return '\n'.join(lines)



def save_to_mongo(
    stats: Dict[str, Any],
    as_of: datetime,
    message: str,
    *,
    level: str,
    level_emoji: str,
    notified: bool,
    prev: dict | None = None,
) -> None:
    """
    Завжди пише latest + історію по годині.
    notified=True → оновлює last_notified_* (антиспам на день).
    notified=False → зберігає попередні last_notified_* з prev.
    """
    col = get_mongo_col()
    if col is None:
        log_to_buffer("⚠️ Немає MONGO_URI — пропускаємо запис у Mongo")
        return

    db_name = (
        os.getenv("ZTOE_MONGO_DB")
        or os.getenv("MONGO_DB")
        or os.getenv("MAYACHOK_MONGO_DB")
        or "vidkluchennya"
    )

    try:
        prev = prev or {}
        today = as_of.strftime("%Y-%m-%d")

        if notified:
            last_notified_date = today
            last_notified_level = level
            last_notified_at = datetime.now(timezone.utc)
            last_notified_cities = stats["total_cities"]
        else:
            last_notified_date = prev.get("last_notified_date")
            last_notified_level = prev.get("last_notified_level")
            last_notified_at = prev.get("last_notified_at")
            last_notified_cities = prev.get("last_notified_cities")

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
            "level": level,
            "level_emoji": level_emoji,
            "updated_at": datetime.now(timezone.utc),
            "last_notified_date": last_notified_date,
            "last_notified_level": last_notified_level,
            "last_notified_at": last_notified_at,
            "last_notified_cities": last_notified_cities,
        }
        col.update_one({"_id": "latest"}, {"$set": doc}, upsert=True)

        history_id = as_of.strftime("%Y-%m-%dT%H")
        hist = {k: v for k, v in doc.items() if k != "_id"}
        hist["_id"] = history_id
        col.update_one({"_id": history_id}, {"$set": hist}, upsert=True)

        log_to_buffer(
            f"🗄 Mongo: {db_name}.accidents_summary (latest + {history_id}), "
            f"level={level}, notified={notified}"
        )
    except Exception as e:
        log_to_buffer(f"❌ Mongo write failed: {e}")



def main() -> int:
    log_to_buffer("=" * 40)
    log_to_buffer("🚀 СТАРТ · Зведення аварій ЖОЕ")
    log_to_buffer(f"ZTOE_API_BASE={'задано' if API_BASE else 'НЕМАЄ'}")
    log_to_buffer(
        f"AVARII_ZHOE={'є' if AVARII_ZHOE else 'немає'} · "
        f"SUBSCRIBE={'є' if SUBSCRIBE else 'немає'}"
    )
    force = (os.getenv("FORCE_NOTIFY") or "").strip()
    if force:
        log_to_buffer(f"FORCE_NOTIFY={force}")

    try:
        accidents = fetch_all_accidents()
    except Exception as e:
        log_to_buffer(f"❌ Fetch failed: {e}")
        send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ (помилка)")
        return 1

    as_of = datetime.now(KYIV)
    prev = load_latest_summary()

    if not accidents:
        log_to_buffer("⚠️ Порожня відповідь API")
        stats = {
            "total_records": 0,
            "total_streets": 0,
            "total_cities": 0,
            "zhytomyr_streets": 0,
            "rem": {},
        }
        level, level_emoji, level_label = level_from_cities(0)
        msg = (
            f"{level_emoji}<b>{level_label}</b> стан електромереж у Житомирській області\n\n"
            "Зараз аварійних відключень за даними API немає."
        )
        if AVARII_ZHOE or SUBSCRIBE:
            parts = []
            if AVARII_ZHOE:
                parts.append(f'<a href="{AVARII_ZHOE}">🔗 Сайт "ЖОЕ"</a>')
            if SUBSCRIBE:
                parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
            msg += "\n\n" + "  ".join(parts)
            msg += f"\n🕐 {as_of.strftime('%H:%M %d.%m')}"

        do_notify, reason = should_notify_channel(level, prev, as_of)
        log_to_buffer(f"📢 notify={do_notify} ({reason})")
        save_to_mongo(
            stats, as_of, msg,
            level=level, level_emoji=level_emoji,
            notified=False,  # порожньо / NORM — не ставимо мітку «вже слали»
            prev=prev,
        )
        # при порожньому API в канал не шлемо (база оновлена)
        send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ")
        return 0

    stats = aggregate(accidents)
    level, level_emoji, level_label = level_from_cities(stats["total_cities"])
    msg = format_message(stats, as_of, (level, level_emoji, level_label))

    log_to_buffer(
        f"📊 records={stats['total_records']} "
        f"streets={stats['total_streets']} "
        f"cities={stats['total_cities']} "
        f"zhytomyr={stats['zhytomyr_streets']} "
        f"level={level}"
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

    do_notify, reason = should_notify_channel(level, prev, as_of)
    log_to_buffer(f"📢 notify={do_notify} ({reason})")

    notified_ok = False
    if do_notify:
        ok = send_notification(msg)
        if ok:
            notified_ok = True
            log_to_buffer(f"✓ Telegram: звіт відправлено ({len(msg)} символів)")
        else:
            log_to_buffer("❌ Telegram: не вдалося відправити")
    else:
        log_to_buffer("⏭ Telegram: пропущено (антиспам / рівень)")

    # Mongo завжди; мітку last_notified оновлюємо лише якщо реально відправили
    save_to_mongo(
        stats, as_of, msg,
        level=level,
        level_emoji=level_emoji,
        notified=notified_ok,
        prev=prev,
    )

    send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ")
    if do_notify and not notified_ok:
        return 1
    return 0



if __name__ == "__main__":
    sys.exit(main())
