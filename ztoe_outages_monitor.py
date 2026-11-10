#!/usr/bin/env python3
"""
Аварійні / планові відключення ЖОЕ → підписники бота.

Оптимізації:
- один fetch на otg_id (без city), фільтр міста в пам'яті
- ловить продовження аварії по addr_day (city+street+buildings+день),
  навіть коли ЖОЕ видає новий id замість оновлення старого
- спільний httpx.Client
- bulk_write у Mongo замість поштучних update_one
- без подвійного retry empty+city

Архітектура сповіщень (v1.4):
- Глобальний outages_cache (fps / soft_fps / plan_by_addr_day) — лише для
  детекції змін в API і продовжень аварій.
- Per-user дедуп у колекції user_outage_notified:
  ключ = telegram_id + notify_dedup_key.
  Завдяки цьому користувач, який додав адресу ПІСЛЯ появи аварії,
  все одно отримає сповіщення про поточні активні відключення.
- matches_building нормалізує номери (033 ≡ 33, 33а ≡ 33А тощо).

Cleanup (щоб база не роздувалась):
- outages: delete документи з updated_at старші за OUTAGES_RETENTION_HOURS (default 48)
- outages_cache: soft_fps обрізається до SOFT_FPS_MAX (default 3000);
  документи cache, які давно не оновлювались (OUTAGES_CACHE_RETENTION_DAYS, default 30),
  видаляються (застарілі групи адрес)
- user_outage_notified: TTL через expires_at (default 7 днів)

Env:
  OUTAGES_RETENTION_HOURS      — TTL повних записів у outages (default 48)
  SOFT_FPS_MAX                 — макс. розмір soft_fps у cache (default 3000)
  OUTAGES_CACHE_RETENTION_DAYS — TTL неактивних ключів cache (default 30)
  USER_NOTIFIED_TTL_DAYS       — TTL записів per-user дедупу (default 7)
"""

from __future__ import annotations

import re

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import httpx
from pymongo import MongoClient, UpdateOne
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
    raise SystemExit("Немає ZTOE_API_BASE")

# --- retention / caps (не чіпають queue_schedules, users, ГПВ) ---
OUTAGES_RETENTION_HOURS = int(os.getenv("OUTAGES_RETENTION_HOURS", "48"))
SOFT_FPS_MAX = int(os.getenv("SOFT_FPS_MAX", "3000"))
OUTAGES_CACHE_RETENTION_DAYS = int(os.getenv("OUTAGES_CACHE_RETENTION_DAYS", "30"))
USER_NOTIFIED_TTL_DAYS = int(os.getenv("USER_NOTIFIED_TTL_DAYS", "7"))


def merge_soft_fps(current_soft: Set[str], prev_soft: Set[str], limit: int = SOFT_FPS_MAX) -> List[str]:
    """
    Об'єднує soft_fps, але не дає масиву рости безмежно.
    Поточні fingerprint'и з API завжди зберігаються;
    зі старих добираємо до limit.
    """
    if limit <= 0:
        return list(current_soft)
    if len(current_soft) >= limit:
        return list(current_soft)[:limit]
    extra = list(prev_soft - current_soft)
    need = limit - len(current_soft)
    return list(current_soft) + extra[:need]

TYPE_LABELS = {
    0: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
    1: ("Аварійне", "несподівана аварія в мережі"),
    2: ("Оперативне", "технічні перемикання в мережі"),
    3: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
}

# switchSubReason з API ЖОЕ (як на сайті / у JS power-on):
#   0 — звичайне (дивись typeOff)
#   1 — ГАВ (аварійний графік)
#   2 — ГПВ (погодинні) ← НЕ аварія, не нотифікуємо
#   3 — СГАВ
SUBREASON_GPV = 2
SUBREASON_GAV = 1
SUBREASON_SGAV = 3


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


def _switch_sub_reason(outage: dict) -> int:
    try:
        return int(outage.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        return 0


def is_gpv_outage(outage: dict) -> bool:
    """
    Графік погодинних відключень (ГПВ), не аварія.
    На сайті: «Графік погодинних відключень», switchSubReason=2.
    typeOff при цьому часто = 1 — через це раніше слали «🔴 Аварійне».
    """
    if _switch_sub_reason(outage) == SUBREASON_GPV:
        return True
    koment = (outage.get("koment") or "").upper()
    if "ГПВ" in koment:
        return True
    return False


def should_notify_outage(outage: dict) -> bool:
    """
    Сповіщати лише про реальні події поза звичайним ГПВ:
      - ГПВ (sub=2) → ні (це графік, не сюрприз)
      - планове / аварійне / оперативне / ГАВ / СГАВ → так
    """
    if is_gpv_outage(outage):
        return False
    return True


def outage_notify_label(outage: dict) -> tuple:
    """(icon, title, desc) для тексту сповіщення."""
    sub = _switch_sub_reason(outage)
    if is_gpv_outage(outage):
        return ("⏰", "Погодинне (ГПВ)", "графік погодинних відключень")
    if sub in (SUBREASON_GAV, SUBREASON_SGAV):
        return ("🔴", "Аварійне (ГАВ)" if sub == SUBREASON_GAV else "Аварійне (СГАВ)",
                "аварійний графік відключень")
    type_off = outage.get("typeOff")
    try:
        t = int(type_off)
    except (TypeError, ValueError):
        t = -1
    name, desc = TYPE_LABELS.get(t, (f"Тип {type_off}", ""))
    icon = {0: "🟠", 1: "🔴", 2: "🟡", 3: "🟠"}.get(t, "⚪")
    return (icon, name, desc)


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def _fetch_all_pages(
    client: httpx.Client,
    path: str,
    otg_id: int,
    city_id: Optional[int] = None,
) -> List[dict]:
    items: List[dict] = []
    page = 1
    while page <= 40:
        params: dict = {"page": page, "itemsPerPage": 100, "otg.id": otg_id}
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


def fetch_all_outages(
    client: httpx.Client,
    otg_id: int,
    city_id: Optional[int] = None,
) -> List[dict]:
    accidents = _fetch_all_pages(client, "/pw_accidents", otg_id, city_id)
    try:
        planned = _fetch_all_pages(client, "/pw_accident_planeds", otg_id, city_id)
    except Exception as e:
        log.warning("planeds error otg=%s city=%s: %s", otg_id, city_id, e)
        planned = []
    for a in accidents:
        a["_source"] = "accidents"
    for p in planned:
        p["_source"] = "planeds"
    return accidents + planned


def _fetch_pages_by_city(
    client: httpx.Client,
    path: str,
    city_id: int,
) -> List[dict]:
    """Фетч лише по city.id — обходить криві otg-прив'язки в довіднику міст."""
    items: List[dict] = []
    page = 1
    while page <= 40:
        params = {"page": page, "itemsPerPage": 100, "city.id": city_id}
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


def fetch_outages_by_city(client: httpx.Client, city_id: int) -> List[dict]:
    """
    Тягне аварії/плани по city.id.
    Важливо: у довіднику ЖОЕ деякі міста (напр. Коростень id=430)
    прив'язані до чужого РЕМ (Хорошівський замість Коростенського).
    Фетч по otg.id тоді повертає 0 записів. По city.id — коректні дані.
    """
    accidents = _fetch_pages_by_city(client, "/pw_accidents", city_id)
    try:
        planned = _fetch_pages_by_city(client, "/pw_accident_planeds", city_id)
    except Exception as e:
        log.warning("planeds error city=%s: %s", city_id, e)
        planned = []
    for a in accidents:
        a["_source"] = "accidents"
    for p in planned:
        p["_source"] = "planeds"
    return accidents + planned


def filter_by_city(outages: List[dict], city_id: int) -> List[dict]:
    """Якщо city_id=0 — повертаємо все. Інакше лишаємо збіг або без city (плани часто без міста)."""
    if not city_id:
        return outages
    result = []
    for o in outages:
        cid = (o.get("city") or {}).get("id")
        if cid is None or int(cid) == int(city_id):
            result.append(o)
    return result


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


def notify_dedup_key(outage: dict, house: str = "") -> str:
    """
    Ключ для злиття дублів: одна вулиця + будинок користувача + той самий проміжок.
    Ігнорує різний список buildingNames з API (Шкільна vs ШКІЛЬНА, 1..8 vs 1,15,2,4…).
    """
    street = outage.get("street") or {}
    sname = _norm_street(street.get("name") or "")
    sid = street.get("id") or sname
    city = (outage.get("city") or {}).get("id") or (outage.get("city") or {}).get("name") or ""
    start = (outage.get("dateEvent") or "")[:16]
    end = (outage.get("datePlanIn") or "")[:16]
    h = (house or "").strip().lower().lstrip("0") or "0"
    return f"{city}|{sid}|{sname}|{h}|{start}|{end}"


def addr_day_key(outage: dict) -> str:
    """
    Ключ «та сама адреса + той самий день» — незалежно від id і години старту.
    Потрібен, щоб ловити продовження аварії, коли ЖОЕ закриває старий запис
    і відкриває новий id з іншим dateEvent / datePlanIn.
    """
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    city = (outage.get("city") or {}).get("id") or ""
    day = (outage.get("dateEvent") or outage.get("datePlanIn") or "")[:10]
    return f"{city}|{sid}|{','.join(parts)}|{day}"


def _fmt_ddmm(dt: datetime) -> str:
    return dt.strftime("%d.%m")


def _fmt_hhmm_dt(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def format_forecast_period(date_event: str, date_plan: str) -> str:
    """
    Один рядок періоду:
      той самий день → Прогнозований період: 09:00-20:10 (09.11)
      кілька днів   → Прогнозований період: з 22:00 (09.11) до 01:00 (17.11)
    """
    start = _parse_iso_dt(date_event)
    end = _parse_iso_dt(date_plan)

    if start is not None:
        start = start.replace(tzinfo=None) if getattr(start, "tzinfo", None) else start
    if end is not None:
        end = end.replace(tzinfo=None) if getattr(end, "tzinfo", None) else end

    if start and end:
        if start.date() == end.date():
            return (
                f"Прогнозований період: {_fmt_hhmm_dt(start)}-{_fmt_hhmm_dt(end)} "
                f"({_fmt_ddmm(start)})"
            )
        return (
            f"Прогнозований період: з {_fmt_hhmm_dt(start)} ({_fmt_ddmm(start)}) "
            f"до {_fmt_hhmm_dt(end)} ({_fmt_ddmm(end)})"
        )
    if start:
        return f"Прогнозований період: з {_fmt_hhmm_dt(start)} ({_fmt_ddmm(start)})"
    if end:
        return f"Прогнозований період: до {_fmt_hhmm_dt(end)} ({_fmt_ddmm(end)})"
    return ""


def format_outage(outage: dict, user_house: str = "") -> str:
    """
    Формує текст однієї події.

    Видима частина:
      🔴 Місто, вул. X, 33
      📅 Прогнозований період: ...

    Під expandable-цитатою (якщо є що ховати):
      💬 koment
      📌 Тип
      🔔 для будинків: ...
    """
    city = (outage.get("city") or {}).get("name", "?")
    street = (outage.get("street") or {}).get("name", "?")
    buildings = (outage.get("buildingNames") or "").strip()
    date_event_raw = outage.get("dateEvent") or ""
    date_plan_raw = outage.get("datePlanIn") or ""
    date_plan = (date_plan_raw or "")[:16].replace("T", " ")
    icon, type_name, type_desc = outage_notify_label(outage)
    koment = (outage.get("koment") or "").strip()

    house = (user_house or "").strip()
    addr_line = f"{city}, {street}"
    if house:
        addr_line += f", {house}"

    # --- видима шапка ---
    if outage.get("_plan_updated"):
        text = f"⏱ <b>Оновлення:</b> {addr_line}"
        was = (outage.get("_plan_was") or "")[:16].replace("T", " ")
        period = format_forecast_period(date_event_raw, date_plan_raw)
        if was and date_plan and was != date_plan:
            text += f"\n⏳ План: <s>{was}</s> → <b>{date_plan}</b>"
            if period:
                text += f"\n📅 {period}"
        elif period:
            text += f"\n📅 {period}"
    else:
        text = f"{icon} <b>{addr_line}</b>"
        period = format_forecast_period(date_event_raw, date_plan_raw)
        if period:
            text += f"\n📅 {period}"

    # --- деталі (тип, koment, список будинків) ---
    details: list[str] = []
    if koment:
        details.append(f"💬 {koment}")
    type_line = f"📌 {type_name}"
    if type_desc:
        type_line += f" — {type_desc}"
    details.append(type_line)
    if buildings:
        details.append(f"🔔 для будинків: {buildings}")

    if not details:
        return text

    # довгий список будинків → ховаємо все під expandable blockquote
    LONG_BUILDINGS = 60
    use_spoiler = bool(buildings) and len(buildings) > LONG_BUILDINGS

    if use_spoiler:
        inner = "\n".join(details)
        text += f"\n<blockquote expandable>{inner}</blockquote>"
    else:
        text += "\n" + "\n".join(details)
    return text


# ----- блок графіка під сповіщенням (як у каналі / bot_notify) -----

def _parse_hhmm(t: str) -> Optional[int]:
    import re
    m = re.match(r"^(\d{1,2}):(\d{2})", (t or "").strip())
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h == 24 and mi == 0:
        return 24 * 60
    return h * 60 + mi


def _minutes_to_hhmm(m: int) -> str:
    if m >= 24 * 60:
        return "24:00"
    return f"{m // 60:02d}:{m % 60:02d}"


def _merge_intervals(intervals: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    parsed = []
    for fr, to in intervals:
        a, b = _parse_hhmm(fr), _parse_hhmm(to)
        if a is None or b is None:
            continue
        if b <= a:
            b += 24 * 60
        parsed.append((a, b))
    if not parsed:
        return []
    parsed.sort()
    merged = [list(parsed[0])]
    for a, b in parsed[1:]:
        if a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return [(_minutes_to_hhmm(a), _minutes_to_hhmm(b)) for a, b in merged]


def _total_minutes(intervals: List[Tuple[str, str]]) -> int:
    total = 0
    for fr, to in _merge_intervals(intervals):
        a, b = _parse_hhmm(fr), _parse_hhmm(to)
        if a is None or b is None:
            continue
        if b <= a:
            b += 24 * 60
        total += b - a
    return total


def _fmt_duration(total_min: int) -> str:
    if total_min < 0:
        total_min = 0
    h, m = divmod(int(total_min), 60)
    if h and m:
        return f"{h} год {m}хв"
    if h:
        return f"{h} год"
    return f"{m}хв"


def _fmt_range(a: str, b: str) -> str:
    return f"<b><u>{a}-{b}</u></b>"


def _interval_key(fr: str, to: str) -> Tuple[int, int]:
    a = _parse_hhmm(fr) or -1
    b = _parse_hhmm(to) or -1
    if a >= 0 and b >= 0 and b <= a:
        b += 24 * 60
    return (a, b)


def _subtract_intervals(
    slots: List[Tuple[str, str]],
    covers: List[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    cover_m = _merge_intervals(covers)
    if not cover_m:
        return _merge_intervals(slots)
    result: List[Tuple[int, int]] = []
    for fr, to in _merge_intervals(slots):
        a, b = _interval_key(fr, to)
        if a < 0 or b < 0:
            continue
        pieces = [(a, b)]
        for cfr, cto in cover_m:
            ca, cb = _interval_key(cfr, cto)
            if ca < 0:
                continue
            new_pieces = []
            for pa, pb in pieces:
                if cb <= pa or ca >= pb:
                    new_pieces.append((pa, pb))
                    continue
                if pa < ca:
                    new_pieces.append((pa, ca))
                if pb > cb:
                    new_pieces.append((cb, pb))
            pieces = new_pieces
        result.extend(pieces)
    return [(_minutes_to_hhmm(a), _minutes_to_hhmm(b)) for a, b in result if b > a]


def _load_queue_doc(db, label: str) -> Optional[dict]:
    if not label:
        return None
    s = label.replace("|", ".")
    parts = s.split(".")
    ch, pid = parts[0], (parts[1] if len(parts) > 1 else "")
    for _id in (
        f"{ch}|{pid}" if pid else ch,
        f"{ch}.{pid}" if pid else ch,
        label,
        ch,
    ):
        doc = db["queue_schedules"].find_one({"_id": _id})
        if doc:
            return doc
    if pid:
        doc = db["queue_schedules"].find_one({"queue": ch, "subqueue": pid})
        if doc:
            return doc
    return None


def _slots_from_sched(doc: Optional[dict], *, day: int = 0) -> List[Tuple[str, str]]:
    """day=0/1 — слоти з queue_schedules; day<0 або >1 — ГПВ ще немає (далека дата)."""
    if not doc or day < 0 or day > 1:
        return []
    if day == 1:
        slots = (
            doc.get("slots_tomorrow")
            or (doc.get("extra") or {}).get("slots_tomorrow")
            or doc.get("events_tomorrow")
            or []
        )
    else:
        slots = doc.get("slots") or doc.get("events") or []
    out = []
    for s in slots or []:
        if not isinstance(s, dict):
            continue
        fr = (s.get("from") or s.get("start") or "").strip()[:5]
        to = (s.get("to") or s.get("end") or "").strip()[:5]
        if fr and to:
            out.append((fr, to))
    return _merge_intervals(out)


def _outage_day_ymd(outage: dict) -> str:
    de = str(outage.get("dateEvent") or "")[:10]
    if de and len(de) >= 10 and de[4] == "-":
        return de
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _display_date_from_ymd(ymd: str) -> str:
    try:
        return datetime.strptime(ymd[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return ymd


def _parse_iso_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        if "T" in s:
            return datetime.fromisoformat(s)
        if len(s) >= 16 and s[10] == " ":
            return datetime.fromisoformat(s.replace(" ", "T", 1))
        if len(s) >= 10 and s[4] == "-":
            return datetime.fromisoformat(s[:10])
    except Exception:
        return None
    return None


def _interval_on_day(de: str, dp: str, ymd: str) -> Optional[Tuple[str, str]]:
    """
    Розрізає інтервал [dateEvent, datePlanIn] на шматок саме для дня ymd.
    Приклад: 2026-11-03 21:35 → 2026-11-04 17:00
      ymd=03 → 21:35-24:00
      ymd=04 → 00:00-17:00
    """
    start_dt = _parse_iso_dt(de)
    end_dt = _parse_iso_dt(dp)
    if not start_dt or not end_dt:
        # fallback: якщо обидві дати того ж дня / одна з них
        start = de[11:16] if len(de) >= 16 else ""
        end = dp[11:16] if len(dp) >= 16 else ""
        if de.startswith(ymd) and dp.startswith(ymd) and start and end:
            return (start, end)
        if de.startswith(ymd) and start:
            return (start, "24:00")
        if dp.startswith(ymd) and end:
            return ("00:00", end)
        return None

    # naive compare by calendar day (API timestamps are local-ish)
    day0 = datetime.strptime(ymd[:10], "%Y-%m-%d")
    day1 = day0 + timedelta(days=1)
    s = start_dt.replace(tzinfo=None) if start_dt.tzinfo else start_dt
    e = end_dt.replace(tzinfo=None) if end_dt.tzinfo else end_dt
    if e <= s:
        return None
    # no overlap with [day0, day1)
    if e <= day0 or s >= day1:
        return None
    a = max(s, day0)
    b = min(e, day1)
    if b <= a:
        return None
    start_s = a.strftime("%H:%M")
    if b >= day1:
        end_s = "24:00"
    else:
        end_s = b.strftime("%H:%M")
        if end_s == "00:00" and b > a:
            end_s = "24:00"
    if start_s == end_s:
        return None
    return (start_s, end_s)


def _outage_span_ymds(outage: dict, *, from_ymd: Optional[str] = None) -> List[str]:
    """Усі календарні дні, які покриває аварія (від max(start, from_ymd) до end)."""
    de = str(outage.get("dateEvent") or outage.get("date_from") or "")
    dp = str(outage.get("datePlanIn") or outage.get("date_to") or "")
    start_dt = _parse_iso_dt(de)
    end_dt = _parse_iso_dt(dp)
    if not start_dt:
        y = _outage_day_ymd(outage)
        return [y] if y else []
    s = start_dt.replace(tzinfo=None) if getattr(start_dt, "tzinfo", None) else start_dt
    if end_dt:
        e = end_dt.replace(tzinfo=None) if getattr(end_dt, "tzinfo", None) else end_dt
    else:
        e = s
    if e < s:
        e = s
    # якщо кінець рівно опівночі — останній день не включаємо
    end_day = e.date()
    if e.hour == 0 and e.minute == 0 and getattr(e, "second", 0) == 0 and e > s:
        end_day = (e - timedelta(seconds=1)).date()
    start_day = s.date()
    if from_ymd:
        try:
            floor = datetime.strptime(from_ymd[:10], "%Y-%m-%d").date()
            if start_day < floor:
                start_day = floor
        except ValueError:
            pass
    if start_day > end_day:
        return []
    out = []
    cur = start_day
    # захист від дуже довгих інтервалів
    for _ in range(14):
        out.append(cur.strftime("%Y-%m-%d"))
        if cur >= end_day:
            break
        cur = cur + timedelta(days=1)
    return out


def _sched_day_index(sched: Optional[dict], ymd: str) -> int:
    """
    0 = slots (date з графіка), 1 = slots_tomorrow (date+1).
    -1 = дата поза відомим ГПВ (далекий ремонт/аварія) — погодинних не підставляти.
    """
    if not sched or not ymd:
        return -1
    raw = (sched.get("date") or "").strip()
    if not raw:
        return -1
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            anchor = datetime.strptime(raw[:10] if fmt.startswith("%Y") else raw, fmt)
            a = anchor.strftime("%Y-%m-%d")
            if a == ymd[:10]:
                return 0
            nxt = (anchor + timedelta(days=1)).strftime("%Y-%m-%d")
            if nxt == ymd[:10]:
                return 1
            return -1
        except ValueError:
            continue
    return -1


def _classify_mongo_outage(doc: dict) -> Optional[str]:
    """
    emergency | repair | other | None (ГПВ).
    typeOff=1 часто плутали з ГПВ — відсікаємо лише sub=2 / «ГПВ» у koment.
    """
    koment = (doc.get("koment") or "").upper()
    if "ГПВ" in koment:
        return None
    raw_sub = doc.get("switchSubReason")
    has_sub = raw_sub is not None and raw_sub != ""
    try:
        sub = int(raw_sub) if has_sub else 0
    except (TypeError, ValueError):
        sub = 0
        has_sub = False
    try:
        t = int(doc.get("typeOff")) if doc.get("typeOff") is not None else -1
    except (TypeError, ValueError):
        t = -1
    if has_sub and sub == 2:
        return None
    if has_sub and sub in (1, 3):
        return "emergency"
    if has_sub and sub > 0:
        return "other"
    # typeOff=1 без sub — реальна аварія (ГПВ уже відсіяні вище)
    if t == 1:
        return "emergency"
    if t in (0, 3):
        return "repair"
    if t == 2:
        return "other"
    return None


def _buckets_for_addr_day(db, addr: dict, ymd: str) -> Dict[str, List[Tuple[str, str]]]:
    empty: Dict[str, List[Tuple[str, str]]] = {
        "emergency": [],
        "repair": [],
        "other": [],
    }
    city = (addr.get("city_name") or "").strip()
    street = (addr.get("street_name") or addr.get("street") or "").strip()
    house = str(addr.get("house") or addr.get("building") or "").strip()
    if not city and not street:
        return empty
    q: dict = {}
    if city:
        q["city_name"] = {"$regex": f"^{city}$", "$options": "i"}
    buckets = {"emergency": [], "repair": [], "other": []}
    for doc in db["outages"].find(q).limit(200):
        de = str(doc.get("dateEvent") or doc.get("date_from") or "")
        dp = str(doc.get("datePlanIn") or doc.get("date_to") or "")
        if not (de.startswith(ymd) or dp.startswith(ymd)):
            continue
        ds = (doc.get("street_name") or "").strip().lower()
        sn = _norm_street(street)
        dn = _norm_street(ds)
        if sn and dn and not (sn == dn or sn in dn or dn in sn):
            continue
        buildings = (doc.get("buildingNames") or doc.get("buildings") or "").strip()
        if house and buildings:
            tokens = {
                p.strip().lower().lstrip("0") or "0"
                for p in buildings.replace(";", ",").split(",")
                if p.strip()
            }
            h = house.lower().lstrip("0") or "0"
            if h not in tokens:
                continue
        kind = _classify_mongo_outage(doc)
        if not kind:
            continue
        piece = _interval_on_day(de, dp, ymd)
        if piece:
            buckets[kind].append(piece)
    for k in buckets:
        buckets[k] = _merge_intervals(buckets[k])
    return buckets


def format_schedule_block_for_notify(
    db,
    addr: dict,
    outage: dict,
    ymd: Optional[str] = None,
) -> str:
    """
    Блок під подією — стиль /check:
      Світла не буде (N год): <u>…</u>
      Деталі лише якщо є аварійні / ремонтні / інші
      (без спаму «🟠 Ремонтні: немає»).
    ymd — конкретний день (для багатоденних аварій); інакше день старту.
    """
    ymd = ymd or _outage_day_ymd(outage)
    display = _display_date_from_ymd(ymd)
    lab = user_queue_key(addr) or outage.get("_user_queue") or ""
    sched = _load_queue_doc(db, lab) if lab else None
    day_idx = _sched_day_index(sched, ymd)
    scheduled = _slots_from_sched(sched, day=day_idx)
    buckets = _buckets_for_addr_day(db, addr, ymd)
    emerg = list(buckets.get("emergency") or [])
    repair = list(buckets.get("repair") or [])
    other = list(buckets.get("other") or [])
    # Додаємо інтервал САМОЇ події зі сповіщення в правильний бакет
    # (раніше завжди йшло в «Аварія» — через це ремонт показувався як аварія).
    de = str(outage.get("dateEvent") or outage.get("date_from") or "")
    dp = str(outage.get("datePlanIn") or outage.get("date_to") or "")
    self_piece = _interval_on_day(de, dp, ymd)
    if self_piece:
        # outage з API / notify може не мати тих самих ключів, що mongo-doc
        kind = _classify_mongo_outage(outage)
        if kind is None:
            # ГПВ не додаємо сюди — вони вже в scheduled
            pass
        elif kind == "emergency":
            emerg.append(self_piece)
        elif kind == "repair":
            repair.append(self_piece)
        else:
            other.append(self_piece)
    emerg = _subtract_intervals(_merge_intervals(emerg), scheduled)
    repair = _subtract_intervals(
        _merge_intervals(repair), scheduled + emerg
    )
    other = _subtract_intervals(
        _merge_intervals(other), scheduled + emerg + repair
    )
    all_merged = _merge_intervals(scheduled + emerg + repair + other)

    # day_idx < 0 → дата поза ГПВ; але якщо є аварія/ремонт — показуємо їх
    has_gpv_data = day_idx >= 0

    if all_merged:
        ranges = ", ".join(_fmt_range(a, b) for a, b in all_merged)
        summary = (
            f"<b>🪫 Світла не буде ({_fmt_duration(_total_minutes(all_merged))}):</b> "
            f"<u>{ranges}</u>"
        )
    elif has_gpv_data:
        summary = "🔋 Світло буде увесь день"
    else:
        summary = "🔄 Дані очікуємо"

    def line(prefix: str, slots: List[Tuple[str, str]]) -> str:
        parts = [_fmt_range(a, b) for a, b in slots]
        return (
            f"{prefix}: <u>{', '.join(parts)}</u> "
            f"({_fmt_duration(_total_minutes(slots))})"
        )

    # Деталі лише якщо є аварійні / ремонтні / інші (не дублюємо чисті ГПВ)
    extra_lines = []
    if emerg:
        extra_lines.append(line("⚠️ Аварійні", emerg))
    if repair:
        extra_lines.append(line("🛠 Ремонтні", repair))
    if other:
        extra_lines.append(line("⚪ Інші", other))

    title = f"<b>ОНОВЛЕНИЙ ГРАФІК ВІДКЛЮЧЕНЬ на {display}</b>"

    # Немає погодинних, є лише аварія/ремонт → цитата з годинами (без згортання)
    if not scheduled and extra_lines and all_merged:
        tags = []
        if emerg:
            tags.append("⚠️Аварія")
        if repair:
            tags.append("🛠Ремонт")
        if other:
            tags.append("⚪Інше")
        tag_s = f" ({tags[0]})" if len(tags) == 1 else f" ({', '.join(tags)})"
        ranges = ", ".join(f"{a}-{b}" for a, b in all_merged)
        summary = f"<u>{ranges}</u> ({_fmt_duration(_total_minutes(all_merged))}){tag_s}"
        return f"{title}\n\n<blockquote>{summary}</blockquote>"

    if not extra_lines:
        body = "\n".join([title, "", summary])
        return f"<blockquote>{body}</blockquote>"

    detail = [summary, "⬇️⬇️⬇️", "Деталі:"]
    if scheduled:
        detail.append(line("⏰ Погодинні", scheduled))
    detail.extend(extra_lines)
    body = "\n".join([title, "", *detail])
    return f"<blockquote expandable>{body}</blockquote>"


def build_notify_message(db, items: List[Tuple[dict, dict]]) -> str:
    """
    items: [(outage, addr), ...]
    Верх: список подій з датами.
    Низ: графік на кожен день покриття (від сьогодні / старту до кінця плану).
    """
    lines = ["⚠️ <b>Нові відключення по твоїх адресах:</b>", ""]
    seen_sched: set = set()
    # «сьогодні» по Києву (UTC+2 зимою; для відсікання минулих днів достатньо)
    today_ymd = datetime.now(timezone(timedelta(hours=2))).strftime("%Y-%m-%d")
    for outage, addr in items[:10]:
        user_house = str(addr.get("house") or addr.get("building") or "").strip()
        lines.append(format_outage(outage, user_house=user_house))
        lines.append("")
        ymds = _outage_span_ymds(outage, from_ymd=today_ymd)
        if not ymds:
            ymds = [_outage_day_ymd(outage)]
        lab = user_queue_key(addr) or ""
        city = (addr.get("city_name") or "")
        street = (addr.get("street_name") or "")
        for ymd in ymds:
            key = f"{lab}|{ymd}|{city}|{street}"
            if key in seen_sched:
                continue
            seen_sched.add(key)
            try:
                block = format_schedule_block_for_notify(db, addr, outage, ymd=ymd)
                if block:
                    lines.append(block)
                    lines.append("")
            except Exception as e:
                log.warning("schedule block: %s", e)
    return "\n".join(lines).strip()


def _norm_street(name: str) -> str:
    s = (name or "").strip().lower()
    # прибрати префікси типу вулиці
    for pfx in (
        "вул.", "вул ", "вулиця ", "пров.", "провулок ",
        "проспект ", "пр.", "бульвар ", "б-р ", "пл.", "площа ",
        "м.", "м ",
    ):
        if s.startswith(pfx):
            s = s[len(pfx):].strip()
    # зайві пробіли / апострофи різних видів
    s = s.replace("'", "").replace("’", "").replace("`", "")
    s = " ".join(s.split())
    return s


def matches_street(outage: dict, street_id, street_name: str) -> bool:
    o_street = outage.get("street") or {}
    o_street_id = o_street.get("id")
    o_street_name = (o_street.get("name") or "").lower()
    # точний id
    if street_id and o_street_id is not None and str(o_street_id) == str(street_id):
        return True
    a = _norm_street(street_name)
    b = _norm_street(o_street_name)
    if not a or not b:
        return False
    # повний збіг після нормалізації (Грушевського ≡ ГРУШЕВСЬКОГО)
    if a == b:
        return True
    # один містить інший (Михайла Грушевського ⊃ Грушевського)
    if a in b or b in a:
        return True
    return False


def _parse_house(h: str) -> tuple[str, str]:
    """
    Повертає (числова_частина, суфікс у lower).
    '033'  → ('33', '')
    '33а'  → ('33', 'а')
    '33-А' → ('33', 'а')
    '33 а' → ('33', 'а')
    '33А'  → ('33', 'а')
    """
    h = (h or "").strip().lower().replace(" ", "").replace("-", "")
    m = re.match(r"^0*(\d+)([а-яa-z]*)$", h, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)
    return h, ""


def matches_building(outage: dict, house: str) -> bool:
    """
    Матч будинку користувача з buildingNames з API.
    - 33  матчить 33, 033, 33а, 33А, 33 а
    - 33а матчить тільки 33а / 33А (суфікс обов'язковий)
    - 33  НЕ матчить 330
    """
    house = (house or "").strip()
    if not house:
        return True
    buildings = (outage.get("buildingNames") or "").strip()
    if not buildings:
        return True

    tokens = {
        p.strip().lower()
        for p in buildings.replace(";", ",").split(",")
        if p.strip()
    }

    user_num, user_suf = _parse_house(house)

    for t in tokens:
        tok_num, tok_suf = _parse_house(t)
        if tok_num != user_num:
            continue
        # користувач без суфікса → підходить будь-який токен з цим номером
        # користувач із суфіксом → потрібен точний збіг суфікса
        if not user_suf or user_suf == tok_suf:
            return True
    return False



_GPV_RE = re.compile(
    r"(?:ГПВ|черг[аи]?)\s*[-–]?\s*(\d+)\s*[.,]\s*(\d+)",
    re.IGNORECASE,
)


def parse_queue_from_koment(koment: str):
    """Витягує мітку черги на кшталт «3.1» з koment, або None."""
    if not koment:
        return None
    m = _GPV_RE.search(koment)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return None


def outage_to_mongo_doc(outage: dict, queue_key: str = "") -> dict:
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
        # 0 звичайне; 1 ГАВ; 2 ГПВ (не аварія!); 3 СГАВ — критично для /check
        "switchSubReason": outage.get("switchSubReason"),
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


def main() -> int:
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        log.error("Немає MONGO_URI")
        return 1
    db_name = os.getenv("MONGO_DB") or "vidkluchennya"
    bot_token = os.getenv("ZTOE_BOT_TOKEN")
    if not bot_token:
        log.error("Немає ZTOE_BOT_TOKEN")
        return 1

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    users_col = db["users"]
    outages_col = db["outages_cache"]
    outages_full = db["outages"]
    notified_col = db["user_outage_notified"]

    users = list(users_col.find({"notify": True}))
    try:
        from bot_notify import user_wants, is_quiet_now
        users = [u for u in users if user_wants(u, "accidents")]
    except Exception as e:
        log.warning("prefs filter failed: %s", e)
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

    unique_otgs = sorted({otg for otg, _ in groups})
    log.info("users=%s groups=%s unique_otgs=%s", len(users), len(groups), len(unique_otgs))

    bot = Bot(token=bot_token)
    notified: Dict[int, list] = {}
    groups_checked = 0

    # Один HTTP-клієнт на весь прогін
    http = httpx.Client(
        timeout=30,
        headers={
            "Accept": "application/ld+json",
            "User-Agent": "ztoe-outages-monitor/1.3",
        },
    )

    # Кеш fetch:
    # - по city_id (надійніше: обходить криві otg у довіднику міст)
    # - по otg_id (для адрес без city_id)
    city_outages_cache: Dict[int, List[dict]] = {}
    otg_outages_cache: Dict[int, List[dict]] = {}

    unique_cities = sorted({cid for (_, cid) in groups if cid})
    try:
        for city_id in unique_cities:
            try:
                city_outages_cache[city_id] = fetch_outages_by_city(http, city_id)
                log.info("city=%s fetched_total=%s", city_id, len(city_outages_cache[city_id]))
            except Exception as e:
                log.error("Fetch city=%s: %s", city_id, e)
                city_outages_cache[city_id] = []

        # otg-фетч лише для груп без city_id
        otgs_without_city = sorted({otg for (otg, cid) in groups if not cid})
        for otg_id in otgs_without_city:
            try:
                otg_outages_cache[otg_id] = fetch_all_outages(http, otg_id, None)
                log.info("otg=%s (no city) fetched_total=%s", otg_id, len(otg_outages_cache[otg_id]))
            except Exception as e:
                log.error("Fetch otg=%s: %s", otg_id, e)
                otg_outages_cache[otg_id] = []

        for (otg_id, city_id), group_pairs in groups.items():
            if city_id:
                outages = list(city_outages_cache.get(city_id) or [])
            else:
                outages = list(otg_outages_cache.get(otg_id) or [])

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
                # ГПВ (switchSubReason=2) — це графік, не «нова аварія»
                if not should_notify_outage(o):
                    log.info(
                        "skip GPV/non-notify id=%s sub=%s type=%s",
                        o.get("id"),
                        o.get("switchSubReason"),
                        o.get("typeOff"),
                    )
                    continue
                soft = soft_fingerprint(o)
                if type_family(o.get("typeOff")) == "planned" and soft in prev_soft:
                    log.info("skip planned after known soft=%s", soft)
                    continue
                filtered.append(o)
            new_outages = filtered

            # --- продовження / зміна часу по тій самій адресі+дню (навіть при новому id) ---
            # Порівнюємо саме агрегований max(datePlanIn) на addr_day, а не кожен
            # окремий запис. Якщо на сайті одночасно є кілька інтервалів (напр.
            # 10:15→17:00 і 10:31→13:00), max лишається 17:00 — не шлемо
            # «час став менший».
            prev_plans_addr = {
                str(k): str(v)[:16]
                for k, v in (cache.get("plan_by_addr_day") or {}).items()
            }
            # поточний max plan на addr_day + представник (запис з цим max)
            cur_max_plan: Dict[str, str] = {}
            cur_rep: Dict[str, dict] = {}
            for o in outages:
                if not should_notify_outage(o):
                    continue
                adk = addr_day_key(o)
                if not adk:
                    continue
                plan = (o.get("datePlanIn") or "")[:16]
                if not plan:
                    continue
                prev = cur_max_plan.get(adk, "")
                if not prev or plan > prev:
                    cur_max_plan[adk] = plan
                    cur_rep[adk] = o

            plan_updates: list = []
            seen_plan_adk: set = set()
            new_fp_set = {content_fingerprint(o) for o in new_outages}

            for adk, new_plan in cur_max_plan.items():
                old_plan = (prev_plans_addr.get(adk) or "")[:16]
                if not old_plan or old_plan == new_plan:
                    continue
                o = cur_rep.get(adk)
                if not o:
                    continue
                # уже піде як «нова» — не дублюємо
                if content_fingerprint(o) in new_fp_set:
                    continue
                if adk in seen_plan_adk:
                    continue
                oo = dict(o)
                oo["_plan_updated"] = True
                oo["_plan_was"] = old_plan
                oo["_plan_reason"] = "addr_day"
                # для форматування — показуємо саме агрегований новий план
                oo["datePlanIn"] = new_plan if len(new_plan) == 16 else (o.get("datePlanIn") or new_plan)
                plan_updates.append(oo)
                seen_plan_adk.add(adk)
                log.info(
                    "otg=%s city=%s plan change (addr_day max): %s → %s adk=%s id=%s",
                    otg_id,
                    city_id,
                    old_plan,
                    new_plan,
                    adk,
                    o.get("id"),
                )

            if plan_updates:
                log.info(
                    "otg=%s city=%s plan_updates=%s",
                    otg_id,
                    city_id,
                    len(plan_updates),
                )
                new_outages = list(new_outages) + plan_updates

            log.info(
                "otg=%s city=%s fetched=%s cache_fps=%s new=%s plan_upd=%s",
                otg_id,
                city_id,
                len(outages),
                len(prev_fps),
                max(0, len(new_outages) - len(plan_updates)),
                len(plan_updates),
            )

            # Bulk upsert full docs (для /check)
            if outages:
                ops = []
                for o in outages:
                    doc = outage_to_mongo_doc(o)
                    ops.append(
                        UpdateOne(
                            {"source": doc["source"], "source_id": doc["source_id"]},
                            {"$set": doc},
                            upsert=True,
                        )
                    )
                if ops:
                    # chunk на випадок дуже великих списків
                    for i in range(0, len(ops), 500):
                        outages_full.bulk_write(ops[i : i + 500], ordered=False)

            matched_fps: set = set()

            # --- Per-user нотифікації по ВСІХ активних аваріях ---
            # Глобальний fps-кеш лишається для детекції змін/продовжень.
            # Тут перевіряємо кожного підписника окремо: якщо йому ще не
            # слали цей notify_dedup_key — шлемо (навіть якщо аварія стара).
            active_outages = [o for o in outages if should_notify_outage(o)]
            # plan_updates теж треба врахувати (вони вже додані в new_outages,
            # але можуть не бути в active_outages як окремі об'єкти з _plan_updated)
            plan_update_ids = {id(o) for o in new_outages if o.get("_plan_updated")}
            for o in new_outages:
                if o.get("_plan_updated") and o not in active_outages:
                    active_outages.append(o)

            if active_outages:
                for user, addr in group_pairs:
                    tid = user.get("telegram_id")
                    if not tid:
                        continue
                    street_id = addr.get("street_id") or addr.get("streetId")
                    street_name = (addr.get("street_name") or addr.get("street") or "").strip().lower()
                    house = str(addr.get("house") or addr.get("building") or "")
                    uqk = user_queue_key(addr)

                    # queue_key на full docs тільки для зматчених адрес
                    if uqk:
                        queue_ops = []
                        for outage in outages:
                            if not matches_street(outage, street_id, street_name):
                                continue
                            if not matches_building(outage, house):
                                continue
                            doc = outage_to_mongo_doc(outage, queue_key=uqk)
                            queue_ops.append(
                                UpdateOne(
                                    {"source": doc["source"], "source_id": doc["source_id"]},
                                    {"$set": doc},
                                    upsert=True,
                                )
                            )
                        if queue_ops:
                            outages_full.bulk_write(queue_ops, ordered=False)

                    # які dedup_key вже слали цій людині
                    already_keys = {
                        d["dedup_key"]
                        for d in notified_col.find(
                            {"telegram_id": int(tid)},
                            projection={"dedup_key": 1, "_id": 0},
                        )
                    }

                    for outage in active_outages:
                        if not matches_street(outage, street_id, street_name):
                            continue
                        if not matches_building(outage, house):
                            continue

                        dkey = notify_dedup_key(outage, house)
                        if dkey in already_keys:
                            continue

                        # внутрішній дедуп у межах одного прогону
                        fp = content_fingerprint(outage)
                        already_run = False
                        for x in notified.get(tid, []):
                            prev_o = x[0] if isinstance(x, tuple) else x
                            if content_fingerprint(prev_o) == fp:
                                already_run = True
                                break
                            if notify_dedup_key(prev_o, house) == dkey:
                                already_run = True
                                break
                        if already_run:
                            continue

                        outage_copy = dict(outage)
                        if uqk:
                            outage_copy["_user_queue"] = uqk

                        notified.setdefault(tid, []).append((outage_copy, addr))
                        matched_fps.add(fp)
                        already_keys.add(dkey)

                        # фіксуємо, що цій людині вже слали
                        notify_id = f"{int(tid)}:{dkey}"
                        notified_col.update_one(
                            {"_id": notify_id},
                            {
                                "$set": {
                                    "telegram_id": int(tid),
                                    "dedup_key": dkey,
                                    "notified_at": datetime.now(timezone.utc),
                                    "expires_at": datetime.now(timezone.utc)
                                    + timedelta(days=USER_NOTIFIED_TTL_DAYS),
                                    "otg_id": otg_id,
                                    "city_id": city_id,
                                    "house": house,
                                }
                            },
                            upsert=True,
                        )

                log.info(
                    "otg=%s city=%s matched_active=%s (addrs=%s active=%s)",
                    otg_id,
                    city_id,
                    len(matched_fps),
                    len(group_pairs),
                    len(active_outages),
                )
                if active_outages and not matched_fps:
                    sample = []
                    for o in active_outages[:5]:
                        st = (o.get("street") or {}).get("name") or "?"
                        bld = (o.get("buildingNames") or "")[:40]
                        sample.append(f"{st}|{bld}|type={o.get('typeOff')}")
                    log.info("no street/house match; API sample: %s", " || ".join(sample))
                    for user, addr in group_pairs:
                        log.info(
                            "  user street=%r id=%s house=%r",
                            addr.get("street_name") or addr.get("street"),
                            addr.get("street_id") or addr.get("streetId"),
                            addr.get("house") or addr.get("building"),
                        )

            # Кеш завжди оновлюємо (інакше втрачаємо plan_by_addr_day і «не бачимо» продовжень)
            soft_stored = merge_soft_fps(current_soft, prev_soft, SOFT_FPS_MAX)
            plan_by_addr_day = {}
            for o in outages:
                adk = addr_day_key(o)
                plan = (o.get("datePlanIn") or "")[:16]
                if adk and plan:
                    # беремо найпізніший plan на випадок кількох записів
                    prev_p = plan_by_addr_day.get(adk, "")
                    if not prev_p or plan > prev_p:
                        plan_by_addr_day[adk] = plan
            outages_col.update_one(
                {"_id": cache_key},
                {
                    "$set": {
                        "ids": list(current_ids),
                        "fps": list(current_fps),
                        "soft_fps": soft_stored,
                        "plan_by_addr_day": plan_by_addr_day,
                        "updated_at": datetime.now(timezone.utc),
                        "count": len(current_ids),
                    }
                },
                upsert=True,
            )
            if new_outages and not matched_fps:
                log.warning(
                    "otg=%s city=%s: new/plan=%s but 0 matched — адреса не збіглась",
                    otg_id,
                    city_id,
                    len(new_outages),
                )
    finally:
        http.close()

    # --- cleanup: outages + outages_cache + user_outage_notified ---
    try:
        if OUTAGES_RETENTION_HOURS > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=OUTAGES_RETENTION_HOURS)
            del_out = outages_full.delete_many({"updated_at": {"$lt": cutoff}})
            log.info(
                "outages cleanup: deleted=%s older_than=%sh",
                del_out.deleted_count,
                OUTAGES_RETENTION_HOURS,
            )
            # Прибираємо лише явні ГПВ-сироти (typeOff=1 + «ГПВ» у koment),
            # НЕ чіпаємо звичайні аварії без switchSubReason — вони потрібні /check і monitor.py.
            del_gpv = outages_full.delete_many(
                {
                    "typeOff": 1,
                    "koment": {"$regex": "ГПВ", "$options": "i"},
                    "$or": [
                        {"switchSubReason": {"$exists": False}},
                        {"switchSubReason": None},
                        {"switchSubReason": 2},
                    ],
                }
            )
            log.info(
                "outages cleanup GPV orphans typeOff=1: deleted=%s",
                del_gpv.deleted_count,
            )
        if OUTAGES_CACHE_RETENTION_DAYS > 0:
            cache_cutoff = datetime.now(timezone.utc) - timedelta(
                days=OUTAGES_CACHE_RETENTION_DAYS
            )
            del_cache = outages_col.delete_many({"updated_at": {"$lt": cache_cutoff}})
            log.info(
                "outages_cache cleanup: deleted=%s older_than=%sd",
                del_cache.deleted_count,
                OUTAGES_CACHE_RETENTION_DAYS,
            )
        # Per-user дедуп: видаляємо прострочені записи (expires_at)
        if USER_NOTIFIED_TTL_DAYS > 0:
            del_notified = notified_col.delete_many(
                {"expires_at": {"$lt": datetime.now(timezone.utc)}}
            )
            log.info(
                "user_outage_notified cleanup: deleted=%s expired",
                del_notified.deleted_count,
            )
    except Exception as e:
        log.warning("cleanup failed (non-fatal): %s", e)


    sent = 0
    sent_quiet = 0
    sent_sound = 0
    try:
        from bot_notify import is_quiet_now
    except Exception:
        def is_quiet_now(_u):
            return False
    # notified tid -> user doc (збережемо з groups)
    _user_by_tid = {}
    for _pairs in groups.values():
        for _u, _a in _pairs:
            tid_u = _u.get("telegram_id")
            if tid_u is not None:
                _user_by_tid[int(tid_u)] = _u

    for tid, item_list in notified.items():
        # [(outage, addr), ...]
        text = build_notify_message(db, item_list)
        silent = is_quiet_now(_user_by_tid.get(int(tid)))

        async def _send(chat_id=tid, body=text, silent=silent):
            await bot.send_message(
                chat_id=chat_id,
                text=body[:4096],
                parse_mode="HTML",
                disable_web_page_preview=True,
                disable_notification=silent,
            )

        try:
            asyncio.run(_send())
            sent += 1
            if silent:
                sent_quiet += 1
            else:
                sent_sound += 1
            log.info("Надіслано %s (outages=%s silent=%s)", tid, len(item_list), silent)
            time.sleep(0.4)
        except Exception as e:
            log.error("Send fail %s: %s", tid, e)

    n_events = sum(len(v) for v in notified.values())
    summary = (
        f"✅ Перевірка відключень\n"
        f"Підписників: {len(users)}\n"
        f"Локацій: {groups_checked}\n"
        f"Подій для розсилки: {n_events}\n"
        f"Сповіщень: {sent} (🔊={sent_sound} 🔇={sent_quiet})"
    )
    log.info("%s", summary.replace("\n", " | "))

    log_chat = (
        os.getenv("TELEGRAM_LOG_CHANNEL_ID")
        or os.getenv("ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
    )
    if log_chat:
        try:

            async def _log():
                # 153 — гілка «Аварії, ремонти крон»
                await bot.send_message(
                    chat_id=int(log_chat),
                    text=summary,
                    message_thread_id=153,
                )

            asyncio.run(_log())
        except Exception as e:
            log.warning("log channel: %s", e)

    log.info("Готово groups=%s sent=%s events=%s", groups_checked, sent, n_events)
    return 0


if __name__ == "__main__":
    sys.exit(main())
