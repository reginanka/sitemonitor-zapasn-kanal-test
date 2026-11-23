#!/usr/bin/env python3
"""
Аварійні / планові відключення ЖОЕ → підписники бота.

Оптимізації:
- один fetch на city_id (надійніше за otg) / otg_id fallback
- агрегація по addr_day (city+street+buildings+день): кілька записів API
  на той самий день зливаються в один логічний інтервал (min start, max end)
- заміна запису (старий id зник, новий з іншим часом) → «зміна періоду»,
  а не «нове + скасовано»
- спільний httpx.Client, bulk_write, prune зниклих з Mongo після успішного fetch

Архітектура сповіщень (v1.7):
- ЖОЕ часто тримає кілька записів на день (план 08:00–17:00 + accident 09:28–18:00)
  або міняє id/час замість оновлення. Ми агрегуємо non-GPV записи по addr_day
  і порівнюємо агреговані (start, end) з попереднім знімком.
- Зміна min(start)/max(end) на тому самому addr_day → «⏱ Зміна періоду»
  з Було/Стало (лише якщо рядки після форматування різняться);
  блок графіка знизу — актуальне об’єднання інтервалів.
- Інтервал той самий, але з’явився пізніший старт з /pw_accidents
  (напр. план 08:00–18:00 уже був, accident 09:28) → окреме
  «ℹ️ Фактичний старт робіт» без Було/Стало (дедуп actual|…).
- Якщо план+accident в одному першому fetch → нове сповіщення з рядком
  «Фактичний старт» у звичайному тексті.
- Новий addr_day → нове сповіщення. Повне зникнення addr_day до datePlanIn
  (після CANCEL_MISS_RUNS пропусків) → «Скасовано».
- Після успішного повного fetch записи, яких більше немає в API для цієї
  локації, видаляються з колекції outages (не лише TTL).
- Per-user дедуп (user_outage_notified) і quiet hours без змін:
  ключ пишеться лише після успішної відправки; is_quiet_now як раніше.
- matches_building нормалізує номери (033 ≡ 33, 33а ≡ 33А тощо).

Cleanup:
- outages: prune зниклих після fetch + TTL OUTAGES_RETENTION_HOURS
- outages_cache / user_outage_notified — як раніше

Env:
  OUTAGES_RETENTION_HOURS, SOFT_FPS_MAX, OUTAGES_CACHE_RETENTION_DAYS,
  USER_NOTIFIED_TTL_DAYS, CANCEL_MISS_RUNS
"""

from __future__ import annotations

import re

import asyncio
import html
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Dict, List, Optional, Set, Tuple

import httpx
from pymongo import MongoClient, UpdateOne
from telegram import Bot
from telegram.request import HTTPXRequest

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
# Скільки успішних прогонів підряд запис має бути відсутній в API,
# перш ніж слати «скасовано». 1 = як раніше (перший пропуск уже сповіщення).
CANCEL_MISS_RUNS = max(1, int(os.getenv("CANCEL_MISS_RUNS", "2")))


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


def _iso_sort_key(s: str) -> str:
    """Нормалізує ISO-подібний час для порівняння/min/max як рядок."""
    s = (s or "").strip().replace(" ", "T")
    return s[:19] if s else ""


def _fmt_hhmm_from_iso(s: str) -> str:
    """Витягує HH:MM з ISO-рядка (для «Фактичний старт: 09:28»)."""
    s = _iso_sort_key(s)
    if len(s) >= 16 and s[10] in ("T", " "):
        return s[11:16]
    if len(s) >= 5 and s[2] == ":":
        return s[:5]
    return ""


def aggregate_by_addr_day(outages: List[dict]) -> Dict[str, dict]:
    """
    Зливає non-GPV записи по addr_day в один логічний інтервал.

    Повертає {addr_day_key: aggregated_outage}:
      dateEvent = найраніший start, datePlanIn = найпізніший end,
      buildingNames — об’єднання, _member_keys — id усіх сирих записів,
      _actual_start — якщо є пізніший старт з accidents (фактичний початок робіт),
      _was_* заповнюється ззовні при детекції змін.
    """
    groups: Dict[str, List[dict]] = {}
    for o in outages:
        if not should_notify_outage(o):
            continue
        adk = addr_day_key(o)
        if not adk:
            continue
        groups.setdefault(adk, []).append(o)

    result: Dict[str, dict] = {}
    for adk, members in groups.items():
        starts = [_iso_sort_key(m.get("dateEvent") or "") for m in members]
        ends = [_iso_sort_key(m.get("datePlanIn") or "") for m in members]
        starts = [s for s in starts if s]
        ends = [e for e in ends if e]
        # Представник: пріоритет typeOff in (0,3) планове, інакше перший
        rep = None
        for m in members:
            try:
                t = int(m.get("typeOff"))
            except (TypeError, ValueError):
                t = -1
            if t in (0, 3):
                rep = m
                break
        if rep is None:
            rep = members[0]
        agg = dict(rep)
        if starts:
            agg["dateEvent"] = min(starts)
        if ends:
            agg["datePlanIn"] = max(ends)
        # об’єднати будинки
        bn = merge_building_names(*(m.get("buildingNames") or "" for m in members))
        if bn:
            agg["buildingNames"] = bn
        # koment: перший непорожній
        if not (agg.get("koment") or "").strip():
            for m in members:
                if (m.get("koment") or "").strip():
                    agg["koment"] = m.get("koment")
                    break
        agg["_member_keys"] = [make_outage_key(m) for m in members]
        agg["_addr_day"] = adk

        # Фактичний старт: пізніший dateEvent з /pw_accidents (коли план був раніше)
        # Приклад: план 08:00 + accidents 09:28 → _actual_start = 09:28
        if len(members) > 1 and starts:
            min_start = min(starts)
            actual_candidates = []
            for m in members:
                src = (m.get("_source") or m.get("source") or "").lower()
                s = _iso_sort_key(m.get("dateEvent") or "")
                if not s or s <= min_start:
                    continue
                # accidents або typeOff=0 з пізнішим стартом вважаємо фактичним
                try:
                    t = int(m.get("typeOff"))
                except (TypeError, ValueError):
                    t = -1
                if src == "accidents" or (src != "planeds" and t == 0):
                    actual_candidates.append(s)
            if actual_candidates:
                agg["_actual_start"] = min(actual_candidates)

        result[adk] = agg
    return result


def prev_intervals_by_addr_day(prev_records: List[dict]) -> Dict[str, Tuple[str, str]]:
    """З попереднього знімка: addr_day → (min_start, max_end)."""
    groups: Dict[str, List[dict]] = {}
    for rec in prev_records or []:
        if not isinstance(rec, dict):
            continue
        if not should_notify_outage(rec):
            continue
        adk = addr_day_key(rec)
        if not adk:
            continue
        groups.setdefault(adk, []).append(rec)
    out: Dict[str, Tuple[str, str]] = {}
    for adk, members in groups.items():
        starts = [_iso_sort_key(m.get("dateEvent") or "") for m in members]
        ends = [_iso_sort_key(m.get("datePlanIn") or "") for m in members]
        starts = [s for s in starts if s]
        ends = [e for e in ends if e]
        if not starts and not ends:
            continue
        out[adk] = (min(starts) if starts else "", max(ends) if ends else "")
    return out


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


def h(value) -> str:
    """Екранує текст для Telegram HTML. Інакше «<» у koment/будинках ламає parse_mode."""
    return html.escape("" if value is None else str(value), quote=False)


def format_outage(outage: dict, user_house: str = "") -> str:
    """
    Формує текст однієї події.

    Тип події (нове / зміна / старт / скасування) — у заголовку повідомлення,
    тут лише адреса 📍 і деталі, без дублювання типу в шапці блоку.

    Видима частина:
      📍 Місто, вул. X, 33
      📅 Прогнозований період: ...

    Під expandable-цитатою (якщо є що ховати):
      💬 koment
      📌 Тип
      🔔 для будинків: ...
    """
    city = h((outage.get("city") or {}).get("name", "?"))
    street = h((outage.get("street") or {}).get("name", "?"))
    buildings = h((outage.get("buildingNames") or "").strip())
    date_event_raw = outage.get("dateEvent") or ""
    date_plan_raw = outage.get("datePlanIn") or ""
    icon, type_name, type_desc = outage_notify_label(outage)
    type_name, type_desc = h(type_name), h(type_desc)
    koment = h((outage.get("koment") or "").strip())

    house = h((user_house or "").strip())
    addr_line = f"{city}, {street}"
    if house:
        addr_line += f", {house}"

    # --- видима шапка (тип — у header повідомлення, тут 📍 + деталі) ---
    actual_hhmm = _fmt_hhmm_from_iso(str(outage.get("_actual_start") or ""))

    if outage.get("_cancelled"):
        # Запис зник з API до кінця плану.
        # _fixed_early: період уже йшов — час завершення це прохід скрипта.
        if outage.get("_fixed_early"):
            text = f"📍 {addr_line}"
            done = datetime.now(_kyiv_zone()).strftime("%H:%M")
            text += f"\n✅ Завершили о {h(done)}"
        else:
            text = f"📍 {addr_line}"
        period = h(format_forecast_period(date_event_raw, date_plan_raw))
        if period:
            text += f"\n📅 {period}"
    elif outage.get("_finished_early"):
        # Кінець урізали, і новий кінець уже в минулому — роботи вже не йдуть
        text = f"📍 {addr_line}"
        done_hhmm = _fmt_hhmm_from_iso(date_plan_raw)
        if done_hhmm:
            text += f"\n✅ Завершили о {h(done_hhmm)}"
        was_event = outage.get("_was_dateEvent") or ""
        was_plan = outage.get("_was_datePlanIn") or ""
        was_period = h(format_forecast_period(was_event, was_plan)) if (was_event or was_plan) else ""
        if was_period:
            text += f"\n📅 Було: <s>{was_period}</s>"
    elif outage.get("_actual_start_only"):
        # Інтервал той самий — лише факт старту (заголовок повідомлення вже про це)
        text = f"📍 {addr_line}"
        if actual_hhmm:
            text += f"\n🕐 Фактичний старт робіт: {h(actual_hhmm)}"
        period = h(format_forecast_period(date_event_raw, date_plan_raw))
        if period:
            text += f"\n📅 {period}"
    elif outage.get("_plan_updated"):
        text = f"📍 {addr_line}"
        was_event = outage.get("_was_dateEvent") or ""
        was_plan = outage.get("_plan_was") or outage.get("_was_datePlanIn") or ""
        was_period = h(format_forecast_period(was_event, was_plan)) if (was_event or was_plan) else ""
        if not was_period:
            was = h((was_plan or "")[:16].replace("T", " "))
            if was:
                was_period = was
        period = h(format_forecast_period(date_event_raw, date_plan_raw))
        # Було/Стало лише коли після форматування рядки реально різняться
        if was_period and period and was_period != period:
            text += f"\n⏳ Було: <s>{was_period}</s>"
            text += f"\n📅 Стало: {period}"
        elif period:
            text += f"\n📅 {period}"
        elif was_period:
            text += f"\n⏳ Було: <s>{was_period}</s>"
        if actual_hhmm:
            text += f"\nℹ️ Фактичний старт робіт: {h(actual_hhmm)}"
    else:
        # Нова подія — іконка типу + адреса
        text = f"{icon} <b>{addr_line}</b>"
        period = h(format_forecast_period(date_event_raw, date_plan_raw))
        if period:
            text += f"\n📅 {period}"
        if actual_hhmm:
            text += f"\nℹ️ Фактичний старт робіт: {h(actual_hhmm)}"

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


TG_TEXT_LIMIT = 4096
_HTML_TAG_RE = re.compile(
    r"<(/?)(blockquote|b|u|s|i|code|pre|a|tg-spoiler)\b[^>]*>",
    re.IGNORECASE,
)


def fit_telegram_html(text: str, limit: int = TG_TEXT_LIMIT) -> str:
    """Обрізає текст до ліміту Telegram і закриває теги, які розріз обірвав.

    text[:4096] лишав <blockquote> без </blockquote> —
    Telegram відповідав Can't parse entities.
    """
    text = (text or "").strip()
    if len(text) <= limit:
        return text

    def _stack(fragment: str) -> list:
        stack: list = []
        for m in _HTML_TAG_RE.finditer(fragment):
            name = m.group(2).lower()
            if m.group(1):
                if name in stack:
                    while stack and stack[-1] != name:
                        stack.pop()
                    if stack and stack[-1] == name:
                        stack.pop()
            else:
                stack.append(name)
        return stack

    cut = text[:limit]
    lt, gt = cut.rfind("<"), cut.rfind(">")
    if lt > gt:
        cut = cut[:lt]
    nl = cut.rfind("\n")
    if nl >= int(limit * 0.5):
        cut = cut[:nl]
    cut = cut.rstrip()
    closers = "".join(f"</{name}>" for name in reversed(_stack(cut)))
    if len(cut) + len(closers) > limit:
        cut = cut[: max(0, limit - len(closers))]
        lt, gt = cut.rfind("<"), cut.rfind(">")
        if lt > gt:
            cut = cut[:lt]
        cut = cut.rstrip()
        closers = "".join(f"</{name}>" for name in reversed(_stack(cut)))
        if len(cut) + len(closers) > limit:
            cut = cut[: max(0, limit - len(closers))].rstrip()
    return (cut + closers).strip()


def pack_html_messages(
    blocks: List[str],
    header: str,
    limit: int = TG_TEXT_LIMIT,
) -> List[str]:
    """Складає блоки в кілька повідомлень, кожне ≤ limit і з закритими тегами.

    Завеликий блок (довгий список будинків) ділиться на частини, а не обрізається.
    """
    msgs: List[str] = []
    cur = header.strip()
    cont = "⚠️ <b>Продовження:</b>"
    budget = limit - len(cont) - 4
    for raw in blocks:
        for block in _split_block_for_telegram((raw or "").strip(), max(200, budget)):
            if not block:
                continue
            candidate = f"{cur}\n\n{block}" if cur else block
            if len(candidate) <= limit:
                cur = candidate
                continue
            if cur:
                msgs.append(cur)
            cur = f"{cont}\n\n{block}"
            if len(cur) > limit:
                cur = block
    if cur:
        msgs.append(cur)
    return [m for m in msgs if m]


def _split_block_for_telegram(block: str, limit: int) -> List[str]:
    """Ділить один HTML-блок, не викидаючи хвіст списку будинків."""
    block = (block or "").strip()
    if not block or len(block) <= limit:
        return [block] if block else []

    m = re.match(
        r"^(?P<pre>.*?)<blockquote(?P<attrs>\s[^>]*)?>(?P<body>.*)</blockquote>\s*$",
        block,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return _pack_plain_chunks(block, limit)

    pre = (m.group("pre") or "").strip()
    open_tag = f"<blockquote{m.group('attrs') or ''}>"
    close_tag = "</blockquote>"
    body = (m.group("body") or "").strip()
    overhead = len(open_tag) + len(close_tag) + 1
    first_limit = limit - overhead - (len(pre) + 1 if pre else 0)
    rest_limit = limit - overhead
    first_limit = max(120, first_limit)
    rest_limit = max(120, rest_limit)
    parts = _pack_plain_chunks(body, min(first_limit, rest_limit))
    out: List[str] = []
    for i, part in enumerate(parts):
        quoted = f"{open_tag}{part}{close_tag}"
        if i == 0 and pre:
            quoted = f"{pre}\n{quoted}"
        if len(quoted) > limit:
            # запасний розріз уже всередині частини
            for sub in _pack_plain_chunks(part, max(80, limit - overhead - len(pre) - 1)):
                piece = f"{open_tag}{sub}{close_tag}"
                if i == 0 and pre and not out:
                    piece = f"{pre}\n{piece}"
                out.append(piece)
        else:
            out.append(quoted)
    return out


def _pack_plain_chunks(text: str, limit: int) -> List[str]:
    atoms: List[str] = []
    for line in (text or "").split("\n"):
        if len(line) <= limit or "<" in line:
            atoms.append(line)
            continue
        buf = ""
        bits = line.split(", ")
        for i, bit in enumerate(bits):
            piece = bit if i == len(bits) - 1 else bit + ", "
            if buf and len(buf) + len(piece) > limit:
                atoms.append(buf.rstrip())
                buf = piece
            else:
                buf += piece
        if buf:
            atoms.append(buf.rstrip())
    if not atoms:
        return []
    chunks: List[str] = []
    cur = ""
    for atom in atoms:
        nxt = atom if not cur else cur + "\n" + atom
        if cur and len(nxt) > limit:
            chunks.append(cur)
            cur = atom
        else:
            cur = nxt
    if cur:
        chunks.append(cur)
    return chunks


def _building_identity(token: str) -> str:
    """'12 А' == '12а' == '12-А'. '6' і '6А - Г' лишаються різними."""
    return (
        (token or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("-", "")
        .replace("–", "")
        .replace("—", "")
    )


def merge_building_names(*lists: str) -> str:
    """Зліва направо: номери з наступного запису дописуються, якщо їх ще не було."""
    seen: set = set()
    out: List[str] = []
    for raw in lists:
        for part in (raw or "").replace(";", ",").split(","):
            part = " ".join(part.split())
            if not part:
                continue
            key = _building_identity(part)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(part)
    return ", ".join(out)


def _msg_dedup_key(outage: dict, house: str) -> str:
    """Одна адреса + той самий початок і кінець.

    Звичайний регістр назви вулиці зоивається з капслоком. Якщо в іншому записі відключення довше
    (інший datePlanIn / dateEvent) — ключ інший, сповіщення йде окремо.
    """
    street = outage.get("street") or {}
    sname = _norm_street(street.get("name") or "")
    city = (outage.get("city") or {}).get("id") or (outage.get("city") or {}).get("name") or ""
    start = (outage.get("dateEvent") or "")[:16]
    end = (outage.get("datePlanIn") or "")[:16]
    hse = (house or "").strip().lower().lstrip("0") or "0"
    return f"{city}|{sname}|{hse}|{start}|{end}"


def _build_notify_blocks(
    db,
    items: List[Tuple[dict, dict]],
    *,
    with_schedule: bool = True,
) -> List[str]:
    """Збирає HTML-блоки подій (+ графіків) без заголовка."""
    blocks: List[str] = []
    seen_sched: set = set()
    grouped: List[Tuple[dict, dict]] = []
    group_at: Dict[str, int] = {}
    for outage, addr in items:
        user_house = str(addr.get("house") or addr.get("building") or "").strip()
        if outage.get("_actual_start_only"):
            kind = "actual"
        elif outage.get("_finished_early"):
            kind = "done"
        elif outage.get("_plan_updated"):
            kind = "plan"
        else:
            kind = "event"
        ev_key = f"{kind}|" + _msg_dedup_key(outage, user_house)
        # Той самий інтервал — не друге повідомлення, а допис номерів будинків.
        if ev_key in group_at:
            prev, _prev_addr = grouped[group_at[ev_key]]
            prev["buildingNames"] = merge_building_names(
                prev.get("buildingNames") or "",
                outage.get("buildingNames") or "",
            )
            if not (prev.get("koment") or "").strip() and (outage.get("koment") or "").strip():
                prev["koment"] = outage.get("koment")
            continue
        group_at[ev_key] = len(grouped)
        grouped.append((dict(outage), addr))

    today_ymd = datetime.now(timezone(timedelta(hours=2))).strftime("%Y-%m-%d")
    for outage, addr in grouped:
        user_house = str(addr.get("house") or addr.get("building") or "").strip()
        blocks.append(format_outage(outage, user_house=user_house))
        if not with_schedule:
            continue
        ymds = _outage_span_ymds(outage, from_ymd=today_ymd)
        if not ymds:
            ymds = [_outage_day_ymd(outage)]
        lab = user_queue_key(addr) or ""
        city = (addr.get("city_name") or "")
        street = (addr.get("street_name") or "")
        for ymd in ymds:
            skey = f"{lab}|{ymd}|{city}|{street}"
            if skey in seen_sched:
                continue
            seen_sched.add(skey)
            try:
                block = format_schedule_block_for_notify(db, addr, outage, ymd=ymd)
                if block:
                    blocks.append(block)
            except Exception as e:
                log.warning("schedule block: %s", e)
    return blocks


def build_notify_message(db, items: List[Tuple[dict, dict]]) -> List[str]:
    """
    items: [(outage, addr), ...]

    Окремі заголовки без дублювання типу в тілі:
      • нові → «Нові відключення…»
      • зміна інтервалу → «Зміна періоду запланованих робіт…»
      • кінець урізали і він уже минув → «Роботи … завершено»
      • лише фактичний старт → «Планові роботи … розпочато»
    """
    if not items:
        return []
    actual_items = [
        (o, a) for o, a in items if isinstance(o, dict) and o.get("_actual_start_only")
    ]
    done_items = [
        (o, a)
        for o, a in items
        if isinstance(o, dict) and o.get("_finished_early") and not o.get("_actual_start_only")
    ]
    plan_items = [
        (o, a)
        for o, a in items
        if isinstance(o, dict)
        and o.get("_plan_updated")
        and not o.get("_actual_start_only")
        and not o.get("_finished_early")
    ]
    new_items = [
        (o, a)
        for o, a in items
        if isinstance(o, dict)
        and not o.get("_actual_start_only")
        and not o.get("_plan_updated")
        and not o.get("_finished_early")
    ]
    msgs: List[str] = []
    if new_items:
        msgs.extend(
            pack_html_messages(
                _build_notify_blocks(db, new_items),
                "⚠️ <b>Нові відключення по твоїх адресах:</b>",
            )
        )
    if plan_items:
        msgs.extend(
            pack_html_messages(
                _build_notify_blocks(db, plan_items),
                "⚠️ <b>Зміна періоду запланованих робіт по твоїх адресах:</b>",
            )
        )
    if done_items:
        msgs.extend(
            pack_html_messages(
                _build_notify_blocks(db, done_items, with_schedule=False),
                "✅ <b>Роботи по твоїх адресах завершено:</b>",
            )
        )
    if actual_items:
        msgs.extend(
            pack_html_messages(
                _build_notify_blocks(db, actual_items),
                "ℹ️ <b>Планові роботи по твоїх адресах розпочато:</b>",
            )
        )
    return msgs


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


def remember_user_notified(col, tid: int, outage: dict, addr: dict) -> None:
    """Фіксує дедуп лише після того, як Telegram прийняв повідомлення."""
    house = str(addr.get("house") or addr.get("building") or "")
    if outage.get("_cancelled"):
        dkey = cancel_notify_dedup_key(outage, house)
        # Запис повернувся в API — появу треба сказати знову, старий ключ не блокує.
        orig = notify_dedup_key(outage, house)
        try:
            col.delete_one({"_id": f"{int(tid)}:{orig}"})
        except Exception as e:
            log.warning("drop appearance dedup: %s", e)
    elif outage.get("_actual_start_only"):
        dkey = actual_start_notify_dedup_key(outage, house)
    else:
        dkey = notify_dedup_key(outage, house)
    otg_raw = addr.get("otg_id") or addr.get("otgId") or 0
    city_raw = addr.get("city_id") or addr.get("cityId") or 0
    try:
        otg_id = int(otg_raw)
    except (TypeError, ValueError):
        otg_id = 0
    try:
        city_id = int(city_raw) if city_raw else 0
    except (TypeError, ValueError):
        city_id = 0
    col.update_one(
        {"_id": f"{int(tid)}:{dkey}"},
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


def _kyiv_zone():
    """Місцевий час ЖОЕ. API часто віддає naive ISO — це київський час, не UTC."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("Europe/Kyiv")
    except Exception:
        return timezone(timedelta(hours=2))


def _to_utc(dt: datetime) -> datetime:
    """Цифри з API — київський час, навіть якщо в ISO стоїть +00:00.

    Інакше 18:00+00:00 стає 20:00 за Києвом і після 18:00 план ще «відкритий».
    """
    return _kyiv_wall(dt).astimezone(timezone.utc)


def plan_still_open(outage: dict, now: Optional[datetime] = None) -> bool:
    """True, якщо datePlanIn ще попереду.

    Немає розбірного кінця — не можна стверджувати, що запис зняли рано.
    """
    end = _parse_iso_dt(str(outage.get("datePlanIn") or ""))
    if end is None:
        return False
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=_kyiv_zone())
    return _to_utc(end) > now.astimezone(timezone.utc)


def _kyiv_wall(dt: datetime) -> datetime:
    """Годинник з API як київський, навіть якщо в ISO стоїть +00:00."""
    return dt.replace(tzinfo=None).replace(tzinfo=_kyiv_zone())


def kyiv_moment_passed(iso: str, now: Optional[datetime] = None) -> bool:
    """True, якщо цей момент (як місцевий час ЖОЕ) уже настав."""
    moment = _parse_iso_dt(iso or "")
    if moment is None:
        return False
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=_kyiv_zone())
    return _kyiv_wall(moment).astimezone(timezone.utc) <= now.astimezone(timezone.utc)


def period_has_started(outage: dict, now: Optional[datetime] = None) -> bool:
    """True, якщо dateEvent уже настав (або немає старту — вважаємо, що період «йде»).

    Потрібно, щоб відрізнити «скасували до початку» від «полагодили раніше кінця».
    """
    start = _parse_iso_dt(str(outage.get("dateEvent") or ""))
    if start is None:
        # без старту не стверджуємо «ще не почалось»
        return True
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=_kyiv_zone())
    return _to_utc(start) <= now.astimezone(timezone.utc)


def _outage_interval_utc(outage: dict) -> Optional[Tuple[datetime, datetime]]:
    start = _parse_iso_dt(str(outage.get("dateEvent") or ""))
    end = _parse_iso_dt(str(outage.get("datePlanIn") or ""))
    if start is None and end is None:
        return None
    if start is None:
        start = end
    if end is None:
        end = start
    start_utc, end_utc = _to_utc(start), _to_utc(end)
    if end_utc < start_utc:
        start_utc, end_utc = end_utc, start_utc
    return start_utc, end_utc


def _intervals_overlap(
    a: Optional[Tuple[datetime, datetime]],
    b: Optional[Tuple[datetime, datetime]],
) -> bool:
    if not a or not b:
        return False
    return a[0] < b[1] and b[0] < a[1]


def _brief_place(obj) -> dict:
    if not isinstance(obj, dict):
        return {}
    return {"id": obj.get("id"), "name": obj.get("name")}


def snapshot_from_outage(outage: dict) -> dict:
    """Компактний запис знімка: достатньо, щоб наступного разу порівняти і зібрати текст."""
    source = outage.get("_source") or outage.get("source") or "x"
    oid = outage.get("id")
    if oid is None:
        oid = outage.get("source_id")
    key = outage.get("key") or f"{source}:{oid}"
    return {
        "key": key,
        "id": oid,
        "_source": source,
        "typeOff": outage.get("typeOff"),
        "switchSubReason": outage.get("switchSubReason"),
        "city": _brief_place(outage.get("city")),
        "street": _brief_place(outage.get("street")),
        "otg": _brief_place(outage.get("otg")),
        "buildingNames": outage.get("buildingNames") or "",
        "dateEvent": outage.get("dateEvent") or "",
        "datePlanIn": outage.get("datePlanIn") or "",
        "koment": outage.get("koment") or "",
    }


def mongo_doc_to_outage(doc: dict) -> dict:
    """Старий документ колекції outages → форма, з якою працюють матч і текст."""
    return {
        "id": doc.get("source_id"),
        "_source": doc.get("source") or doc.get("_source"),
        "typeOff": doc.get("typeOff"),
        "switchSubReason": doc.get("switchSubReason"),
        "city": {"id": doc.get("city_id"), "name": doc.get("city_name")},
        "street": {"id": doc.get("street_id"), "name": doc.get("street_name")},
        "otg": {"id": doc.get("otg_id"), "name": doc.get("otg_name")},
        "buildingNames": doc.get("buildingNames") or "",
        "dateEvent": doc.get("dateEvent") or "",
        "datePlanIn": doc.get("datePlanIn") or "",
        "koment": doc.get("koment") or "",
    }


def bootstrap_snapshot_from_ids(ids: List[str], outages_full) -> List[dict]:
    """Перший прогін після деплою: повного snapshot ще нема, але ids + outages є.

    Порожній snapshot у кеші (API реально нічого не віддав) сюди не потрапляє.
    """
    if outages_full is None:
        return []
    records: List[dict] = []
    seen = set()
    for key in ids or []:
        key = str(key or "")
        if ":" not in key or key in seen:
            continue
        seen.add(key)
        source, sid = key.split(":", 1)
        ors: List[dict] = [{"source": source, "source_id": sid}]
        try:
            ors.append({"source": source, "source_id": int(sid)})
        except ValueError:
            pass
        try:
            doc = outages_full.find_one({"$or": ors})
        except Exception as e:
            log.warning("snapshot bootstrap %s: %s", key, e)
            continue
        if not doc:
            continue
        rec = snapshot_from_outage(mongo_doc_to_outage(doc))
        rec["key"] = key
        records.append(rec)
    return records


def previous_snapshot_records(cache: dict, outages_full) -> List[dict]:
    """Минулий знімок + ще не догаслі ранні скасування (для повтору після збою розсилки)."""
    cache = cache or {}
    if "snapshot" in cache and isinstance(cache.get("snapshot"), list):
        prev = list(cache.get("snapshot") or [])
    else:
        prev = bootstrap_snapshot_from_ids(cache.get("ids") or [], outages_full)
    pending = list(cache.get("pending_cancels") or [])
    merged: List[dict] = []
    seen = set()
    for rec in pending + prev:
        if not isinstance(rec, dict):
            continue
        key = str(rec.get("key") or "") or make_outage_key(rec)
        if not key or key in seen:
            continue
        seen.add(key)
        item = dict(rec)
        item["key"] = key
        merged.append(item)
    return merged


def select_early_cancellations(
    prev_records: List[dict],
    current_outages: List[dict],
    now: Optional[datetime] = None,
) -> List[dict]:
    """Записи, які були в попередньому знімку і зникли з API до datePlanIn.

    Не скасовуємо, якщо на ту саму адресу+день API вже віддав інший id
    (ЖОЕ так «продовжує» аварію). Точне покриття будинку перевіряється
    пізніше, окремо для кожного підписника.
    """
    current_keys = {make_outage_key(o) for o in current_outages}
    # ГПВ не маскує скасування аварії/ремонту: інший id погодинного графіка
    # на тій самій вулиці — не «продовження» запису.
    current_days = {
        addr_day_key(o) for o in current_outages if should_notify_outage(o)
    }
    current_days.discard("")

    early: List[dict] = []
    seen = set()
    for rec in prev_records or []:
        if not isinstance(rec, dict):
            continue
        key = str(rec.get("key") or "") or make_outage_key(rec)
        if not key or key in seen or key in current_keys:
            continue
        seen.add(key)
        if not should_notify_outage(rec):
            continue
        adk = addr_day_key(rec)
        if adk and adk in current_days:
            continue
        if not plan_still_open(rec, now=now):
            continue
        item = dict(rec)
        item["key"] = key
        item["_cancelled"] = True
        item["_cancel_reason"] = "gone_before_plan_end"
        # Період уже йшов → світло повернули раніше; інакше — скасували до старту
        if period_has_started(rec, now=now):
            item["_fixed_early"] = True
        early.append(item)
    return early


def still_covered_for_addr(
    gone: dict,
    current_outages: List[dict],
    street_id,
    street_name: str,
    house: str,
) -> bool:
    """Чи лишилось для цього будинку інше відключення на той самий день / інтервал.

    Тоді старий id просто змінили, а не зняли. ГПВ покриттям не вважається.
    """
    gone_iv = _outage_interval_utc(gone)
    gone_day = (gone.get("dateEvent") or gone.get("datePlanIn") or "")[:10]
    for current in current_outages:
        if not should_notify_outage(current):
            continue
        if not matches_street(current, street_id, street_name):
            continue
        if not matches_building(current, house):
            continue
        cur_day = (current.get("dateEvent") or current.get("datePlanIn") or "")[:10]
        if gone_day and cur_day and gone_day == cur_day:
            return True
        if _intervals_overlap(gone_iv, _outage_interval_utc(current)):
            return True
    return False


def pending_cancel_doc(item: dict) -> dict:
    """Знімок зниклого запису + лічильник пропусків (скільки успішних fetch його не було)."""
    doc = snapshot_from_outage(item)
    doc["key"] = str(item.get("key") or doc.get("key") or "")
    try:
        doc["miss"] = int(item.get("miss") or 1)
    except (TypeError, ValueError):
        doc["miss"] = 1
    return doc


def apply_cancel_miss_streak(gone: List[dict], need: int = CANCEL_MISS_RUNS) -> Tuple[List[dict], List[dict]]:
    """Перший пропуск лише запам'ятовуємо. Сповіщення — з N-го підряд.

    Повертає (confirmed, pending). pending пишеться в кеш навіть для ще
    не підтверджених, щоб наступний успішний прогін збільшив лічильник.
    Якщо запис повернувся в API, його вже немає в gone — streak обривається.
    """
    confirmed: List[dict] = []
    pending: List[dict] = []
    for item in gone or []:
        try:
            miss = int(item.get("miss") or 0) + 1
        except (TypeError, ValueError):
            miss = 1
        item = dict(item)
        item["miss"] = miss
        pending.append(item)
        if miss >= need:
            confirmed.append(item)
    return confirmed, pending


def cancel_notify_dedup_key(outage: dict, house: str = "") -> str:
    """Окремий від появи ключ, інакше «вже слали про відключення» з’їсть скасування."""
    return "gone|" + notify_dedup_key(outage, house)


def actual_start_notify_dedup_key(outage: dict, house: str = "") -> str:
    """Окремий ключ: поява фактичного старту не блокується старим «план на 08:00–18:00»."""
    actual = _fmt_hhmm_from_iso(str(outage.get("_actual_start") or ""))
    return "actual|" + notify_dedup_key(outage, house) + f"|{actual}"


def outage_notify_dedup_key(outage: dict, house: str = "") -> str:
    """Єдиний вхід для дедупу залежно від типу події."""
    if outage.get("_cancelled"):
        return cancel_notify_dedup_key(outage, house)
    if outage.get("_actual_start_only"):
        return actual_start_notify_dedup_key(outage, house)
    return notify_dedup_key(outage, house)


def _fetch_pair(
    client: httpx.Client,
    *,
    city_id: Optional[int] = None,
    otg_id: Optional[int] = None,
) -> Tuple[List[dict], bool]:
    """Обидва ендпоінти мають успішно прочитатись.

    Порожня відповідь 200 — це справжнє «записів немає».
    Помилка аварій або планів — неповний знімок: кеш не чіпаємо
    і не розсилаємо «усе скасовано».
    """
    try:
        if city_id:
            accidents = _fetch_pages_by_city(client, "/pw_accidents", int(city_id))
            planned = _fetch_pages_by_city(client, "/pw_accident_planeds", int(city_id))
        else:
            accidents = _fetch_all_pages(client, "/pw_accidents", int(otg_id), None)
            planned = _fetch_all_pages(client, "/pw_accident_planeds", int(otg_id), None)
    except Exception as e:
        log.error(
            "Fetch incomplete city=%s otg=%s: %s",
            city_id,
            otg_id,
            e,
        )
        return [], False
    for acc in accidents:
        acc["_source"] = "accidents"
    for plan in planned:
        plan["_source"] = "planeds"
    return accidents + planned, True


def build_cancel_message(items: List[Tuple[dict, dict]]) -> List[str]:
    """Зняття з API. Без блока «світла не буде» — запису вже немає.

    Якщо період уже почався — окремий заголовок і час проходу скрипта.
    До старту — «скасовано».
    """
    early = [
        pair for pair in items
        if isinstance(pair[0], dict) and pair[0].get("_fixed_early")
    ]
    rest = [
        pair for pair in items
        if not (isinstance(pair[0], dict) and pair[0].get("_fixed_early"))
    ]
    msgs: List[str] = []
    if rest:
        msgs.extend(_pack_cancel_items(
            rest,
            "✅ <b>Скасовані відключення по твоїх адресах:</b>",
        ))
    if early:
        msgs.extend(_pack_cancel_items(
            early,
            "✅ <b>Аварійні роботи завершили раніше запланованого періоду по твоїх адресах:</b>",
        ))
    return msgs


def _pack_cancel_items(items: List[Tuple[dict, dict]], header: str) -> List[str]:
    grouped: List[Tuple[dict, dict]] = []
    group_at: Dict[str, int] = {}
    for outage, addr in items:
        user_house = str(addr.get("house") or addr.get("building") or "").strip()
        ev_key = "gone|" + _msg_dedup_key(outage, user_house)
        if ev_key in group_at:
            prev, _prev_addr = grouped[group_at[ev_key]]
            prev["buildingNames"] = merge_building_names(
                prev.get("buildingNames") or "",
                outage.get("buildingNames") or "",
            )
            if not (prev.get("koment") or "").strip() and (outage.get("koment") or "").strip():
                prev["koment"] = outage.get("koment")
            continue
        group_at[ev_key] = len(grouped)
        item = dict(outage)
        item["_cancelled"] = True
        grouped.append((item, addr))
    blocks = [
        format_outage(outage, user_house=str(addr.get("house") or addr.get("building") or "").strip())
        for outage, addr in grouped
    ]
    return pack_html_messages(blocks, header)


def _group_city_name(group_pairs, outages) -> str:
    for _user, addr in group_pairs or []:
        name = (addr.get("city_name") or addr.get("city") or "").strip()
        if isinstance(name, str) and name:
            return name
    for outage in outages or []:
        name = ((outage.get("city") or {}).get("name") or "").strip()
        if name:
            return name
    return ""


def _human_group(rep: dict) -> str:
    title = (rep.get("city_name") or "").strip() or f"місто {rep.get('city_id')}"
    if not rep.get("fetch_ok"):
        return (
            f"• {title}: ЖОЕ не відповіло, кеш не змінював. "
            f"Адрес підписників: {rep.get('addrs', 0)}."
        )
    quiet = not any(
        (
            rep.get("new"),
            rep.get("plan_upd"),
            rep.get("finished_early"),
            rep.get("actual_start"),
            rep.get("gone_early"),
            rep.get("matched"),
            rep.get("cancel_matched"),
            rep.get("unmatched"),
            rep.get("matched_addrs"),
        )
    )
    if quiet:
        return (
            f"• {title}: без змін. "
            f"Записів у відповіді {rep['fetched']}, адрес {rep['addrs']}."
        )
    waiting = max(0, int(rep.get("gone_early") or 0) - int(rep.get("cancel_ready") or 0))
    lines = [
        f"• {title}",
        (
            f"У відповіді {rep['fetched']} записів, "
            f"для сповіщень {rep['active']}. Адрес підписників: {rep['addrs']}."
        ),
        (
            f"Нових: {rep['new']}. Оновлень часу: {rep['plan_upd']}. "
            f"Завершено: {rep.get('finished_early', 0)}. "
            f"Фактичний старт: {rep.get('actual_start', 0)}."
        ),
        (
            f"Зникли з видачі: {rep['gone_early']}. "
            f"Скасування підтверджено: {rep.get('cancel_ready', 0)}. "
            f"Ще чекають повторної перевірки: {waiting}."
        ),
    ]
    if rep.get("skipped_planned") or rep.get("skipped_gpv"):
        lines.append(
            f"Пропущено вже відомих планових: {rep['skipped_planned']}, "
            f"погодинних графіків: {rep['skipped_gpv']}."
        )
    lines.append(
        f"Збіглося з адресами: {rep['matched']}. "
        f"Скасувань у розсилку: {rep['cancel_matched']}."
    )
    matched_addrs = rep.get("matched_addrs") or []
    if matched_addrs:
        lines.append(
            "Збіг по адресах: " + "; ".join(matched_addrs) + "."
        )
    if rep.get("covered_skip"):
        lines.append(
            "Не слав скасування, бо цю адресу вже покриває інший запис: "
            f"{rep['covered_skip']}."
        )
    if rep.get("unmatched"):
        lines.append(
            "Зараз відключень на цих адресах немає: "
            + "; ".join(rep["unmatched"])
            + "."
        )
    return "\n".join(lines)


def _human_run_summary(
    reports: List[dict],
    *,
    users: int,
    groups_checked: int,
    n_events: int,
    n_new: int,
    n_cancels: int,
    sent: int,
    sent_sound: int,
    sent_quiet: int,
    deliveries: Optional[List[str]] = None,
) -> str:
    body = "\n\n".join(_human_group(rep) for rep in reports) or "Груп немає."
    who = ""
    if deliveries:
        who = "Кому надіслано:\n" + "\n".join(deliveries)
    text = (
        "✅ Перевірка відключень\n"
        f"Підписників: {users}. Локацій: {groups_checked}.\n\n"
        f"{body}\n\n"
        f"У розсилку пішло {n_events}: нових {n_new}, скасувань {n_cancels}.\n"
        f"Повідомлення отримали {sent} "
        f"(зі звуком {sent_sound}, тихо {sent_quiet})."
    )
    if who:
        text = text.rstrip() + "\n\n" + who
    if len(text) <= TG_TEXT_LIMIT:
        return text, None
    short = (
        "✅ Перевірка відключень\n"
        f"Підписників: {users}. Локацій: {groups_checked}.\n"
        f"У розсилку пішло {n_events}: нових {n_new}, скасувань {n_cancels}.\n"
        f"Повідомлення отримали {sent} "
        f"(зі звуком {sent_sound}, тихо {sent_quiet}).\n\n"
        "Детальний звіт у файлі.\n\n"
        + (who or "Кому надіслано:\nнемає")
    )
    if len(short) > TG_TEXT_LIMIT:
        short = short[: TG_TEXT_LIMIT - 20].rstrip() + "\n… решта у файлі"
    return short, text


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
    # Групуємо по city_id (коли є) — інакше один і той самий населений пункт
    # з різними otg_id у довіднику (типовий баг ЖОЕ для Коростеня тощо)
    # дає два однакові звіти. Без city_id — по otg_id.
    # Ключ: ("c", city_id) | ("o", otg_id)
    groups: Dict[Tuple[str, int], List[Tuple[dict, dict]]] = {}
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
            if city_id:
                gkey: Tuple[str, int] = ("c", city_id)
            else:
                gkey = ("o", otg_id)
            groups.setdefault(gkey, []).append((u, addr))

    log.info("users=%s locations=%s", len(users), len(groups))

    notified: Dict[int, list] = {}
    cancelled: Dict[int, list] = {}
    groups_checked = 0
    group_reports: List[dict] = []

    # Один HTTP-клієнт на весь прогін
    http = httpx.Client(
        timeout=30,
        headers={
            "Accept": "application/ld+json",
            "User-Agent": "ztoe-outages-monitor/1.5",
        },
    )

    # Кеш fetch:
    # - по city_id (надійніше: обходить криві otg у довіднику міст)
    # - по otg_id (для адрес без city_id)
    city_outages_cache: Dict[int, List[dict]] = {}
    otg_outages_cache: Dict[int, List[dict]] = {}
    city_fetch_ok: Dict[int, bool] = {}
    otg_fetch_ok: Dict[int, bool] = {}

    unique_cities = sorted({cid for kind, cid in groups if kind == "c"})
    try:
        for city_id in unique_cities:
            items, ok = _fetch_pair(http, city_id=city_id)
            city_outages_cache[city_id] = items
            city_fetch_ok[city_id] = ok
            if ok:
                log.info("city=%s fetched_total=%s", city_id, len(items))
            else:
                log.error("city=%s fetch failed, cache kept", city_id)

        # otg-фетч лише для груп без city_id
        otgs_without_city = sorted({oid for kind, oid in groups if kind == "o"})
        for otg_id in otgs_without_city:
            items, ok = _fetch_pair(http, otg_id=otg_id)
            otg_outages_cache[otg_id] = items
            otg_fetch_ok[otg_id] = ok
            if ok:
                log.info("otg=%s (no city) fetched_total=%s", otg_id, len(items))
            else:
                log.error("otg=%s fetch failed, cache kept", otg_id)

        # Сортуємо локації за назвою міста (для звіту в лог-групі)
        def _group_sort_key(item: Tuple[Tuple[str, int], list]) -> str:
            gkey, pairs = item
            name = _group_city_name(pairs, [])
            return (name or "").lower()

        for gkey, group_pairs in sorted(groups.items(), key=_group_sort_key):
            kind, loc_id = gkey
            if kind == "c":
                city_id = loc_id
                # otg_id — лише для логів/старих кеш-ключів; fetch уже по city
                try:
                    otg_id = int(
                        (group_pairs[0][1].get("otg_id")
                         or group_pairs[0][1].get("otgId")
                         or 0)
                    )
                except (TypeError, ValueError, IndexError):
                    otg_id = 0
                fetch_ok = bool(city_fetch_ok.get(city_id))
                outages = list(city_outages_cache.get(city_id) or [])
                # Кеш знімка — один на city_id (не дробимо по кривому otg)
                cache_key = f"c_{city_id}"
            else:
                otg_id = loc_id
                city_id = 0
                fetch_ok = bool(otg_fetch_ok.get(otg_id))
                outages = list(otg_outages_cache.get(otg_id) or [])
                cache_key = f"o_{otg_id}"

            groups_checked += 1
            if not fetch_ok:
                # Порожній список через помилку HTTP — не «усе зникло».
                log.error(
                    "otg=%s city=%s: неповний fetch, кеш не оновлюю і скасування не рахую",
                    otg_id,
                    city_id,
                )
                group_reports.append({
                    "otg_id": otg_id,
                    "city_id": city_id,
                    "city_name": _group_city_name(group_pairs, []),
                    "fetch_ok": False,
                    "addrs": len(group_pairs),
                })
                continue
            current_ids = {make_outage_key(o) for o in outages}
            current_fps = {content_fingerprint(o) for o in outages}
            current_soft = {soft_fingerprint(o) for o in outages}

            # Міграція зі старого ключа otg_city → city/otg
            cache = outages_col.find_one({"_id": cache_key}) or {}
            if not cache and kind == "c":
                # спробувати старі ключі з будь-яким otg для цього city
                for legacy_otg in {
                    int(a.get("otg_id") or a.get("otgId") or 0)
                    for _, a in group_pairs
                    if (a.get("otg_id") or a.get("otgId"))
                }:
                    legacy = outages_col.find_one({"_id": f"{legacy_otg}_{city_id}"})
                    if legacy:
                        cache = legacy
                        break
            elif not cache and kind == "o":
                legacy = outages_col.find_one({"_id": f"{otg_id}_0"})
                if legacy:
                    cache = legacy
            prev_fps = set(cache.get("fps") or [])
            prev_soft = set(cache.get("soft_fps") or [])

            # --- Агрегація по addr_day: один логічний інтервал на адресу+день ---
            # ЖОЕ часто тримає план 08:00–17:00 і accident 09:28–18:00 одночасно,
            # або міняє id. Порівнюємо min(start)/max(end), не окремі fingerprint.
            skipped_gpv = sum(1 for o in outages if is_gpv_outage(o))
            skipped_planned = 0

            prev_records_early = previous_snapshot_records(cache, outages_full)
            prev_iv = prev_intervals_by_addr_day(prev_records_early)
            # fallback з plan_by_addr_day, якщо snapshot ще порожній
            for k, v in (cache.get("plan_by_addr_day") or {}).items():
                adk = str(k)
                plan = _iso_sort_key(str(v))
                if adk and plan and adk not in prev_iv:
                    prev_iv[adk] = ("", plan)

            cur_agg = aggregate_by_addr_day(outages)
            new_outages: list = []
            plan_updates: list = []
            actual_start_notices: list = []
            finished_notices: list = []
            seen_adk: set = set()

            for adk, agg in cur_agg.items():
                if adk in seen_adk:
                    continue
                seen_adk.add(adk)
                cur_start = _iso_sort_key(agg.get("dateEvent") or "")
                cur_end = _iso_sort_key(agg.get("datePlanIn") or "")
                old_start, old_end = prev_iv.get(adk, ("", ""))

                if not old_start and not old_end:
                    # Новий addr_day — але плановий soft-дубль не шлемо повторно
                    soft = soft_fingerprint(agg)
                    if type_family(agg.get("typeOff")) == "planned" and soft in prev_soft:
                        skipped_planned += 1
                        continue
                    new_outages.append(agg)
                    continue

                interval_changed = (
                    (old_start and cur_start and old_start[:16] != cur_start[:16])
                    or (old_end and cur_end and old_end[:16] != cur_end[:16])
                )

                shortened = bool(old_end and cur_end and cur_end[:16] < old_end[:16])
                new_end_passed = shortened and kyiv_moment_passed(cur_end)
                old_end_passed = shortened and kyiv_moment_passed(old_end)

                if new_end_passed and not old_end_passed:
                    # 18:00 → 17:00, зараз між 17:00 і 18:00: роботи закінчили раніше плану
                    oo = dict(agg)
                    oo["_finished_early"] = True
                    oo["_was_dateEvent"] = old_start
                    oo["_was_datePlanIn"] = old_end
                    finished_notices.append(oo)
                elif new_end_passed and old_end_passed:
                    # Обидва кінці вже в минулому (зараз 20:00, було до 18:00, стало до 17:00).
                    # Задня правка — не «зміна періоду» і не «завершили».
                    pass
                elif interval_changed:
                    # Реальний зсув min(start)/max(end) → «зміна періоду»
                    oo = dict(agg)
                    oo["_plan_updated"] = True
                    oo["_plan_was"] = old_end
                    oo["_was_dateEvent"] = old_start
                    oo["_was_datePlanIn"] = old_end
                    oo["_plan_reason"] = "addr_day_agg"
                    plan_updates.append(oo)
                elif agg.get("_actual_start"):
                    # Інтервал той самий, але з’явився/є фактичний старт з accidents
                    # (план 08:00–18:00 уже був → окремо кажемо «почали о 09:28»)
                    oo = dict(agg)
                    oo["_actual_start_only"] = True
                    actual_start_notices.append(oo)
                # інтервал без змін і без фактичного старту — мовчки

            if plan_updates or actual_start_notices or finished_notices:
                new_outages = (
                    list(new_outages) + plan_updates + actual_start_notices + finished_notices
                )

            prev_records = prev_records_early
            gone = select_early_cancellations(prev_records, outages)
            early, pending_gone = apply_cancel_miss_streak(gone)

            log.info(
                "otg=%s city=%s fetched=%s new=%s plan_upd=%s finished=%s actual_start=%s "
                "gone_early=%s cancel_ready=%s skipped_planned=%s skipped_gpv=%s",
                otg_id,
                city_id,
                len(outages),
                max(
                    0,
                    len(new_outages)
                    - len(plan_updates)
                    - len(actual_start_notices)
                    - len(finished_notices),
                ),
                len(plan_updates),
                len(finished_notices),
                len(actual_start_notices),
                len(gone),
                len(early),
                skipped_planned,
                skipped_gpv,
            )

            # Bulk upsert full docs (для /check) + prune зниклих з API
            if outages:
                ops = []
                current_src_ids = set()
                for o in outages:
                    doc = outage_to_mongo_doc(o)
                    current_src_ids.add((doc["source"], doc["source_id"]))
                    # також int/str варіанти id
                    try:
                        current_src_ids.add((doc["source"], int(doc["source_id"])))
                    except (TypeError, ValueError):
                        pass
                    ops.append(
                        UpdateOne(
                            {"source": doc["source"], "source_id": doc["source_id"]},
                            {"$set": doc},
                            upsert=True,
                        )
                    )
                if ops:
                    for i in range(0, len(ops), 500):
                        outages_full.bulk_write(ops[i : i + 500], ordered=False)

                # Видалити з Mongo записи цієї локації, яких більше немає в API.
                # Лише після успішного повного fetch (ми вже в гілці fetch_ok).
                prune_q: dict = {}
                if city_id:
                    prune_q["city_id"] = int(city_id)
                else:
                    prune_q["otg_id"] = int(otg_id)
                try:
                    stale_ids = []
                    for doc in outages_full.find(prune_q, projection={"_id": 1, "source": 1, "source_id": 1}):
                        key = (doc.get("source"), doc.get("source_id"))
                        if key not in current_src_ids:
                            # перевірка str/int
                            alt = (doc.get("source"), str(doc.get("source_id")))
                            try:
                                alt2 = (doc.get("source"), int(doc.get("source_id")))
                            except (TypeError, ValueError):
                                alt2 = None
                            if alt not in current_src_ids and (alt2 is None or alt2 not in current_src_ids):
                                stale_ids.append(doc["_id"])
                    if stale_ids:
                        for i in range(0, len(stale_ids), 500):
                            outages_full.delete_many({"_id": {"$in": stale_ids[i : i + 500]}})
                        log.info(
                            "otg=%s city=%s pruned_stale_outages=%s",
                            otg_id, city_id, len(stale_ids),
                        )
                except Exception as e:
                    log.warning("prune stale outages: %s", e)

            matched_fps: set = set()
            matched_addrs: List[str] = []
            matched_addrs_seen: set = set()

            # --- Per-user нотифікації по АГРЕГОВАНИХ активних відключеннях ---
            # Один логічний інтервал на addr_day — не шлемо окремо план і accident.
            # new_outages вже містить нові + plan_updates з агрегованими датами.
            # Для користувача, який додав адресу пізніше: шлемо поточний agg,
            # якщо його notify_dedup_key ще немає в user_outage_notified.
            active_by_adk = dict(cur_agg)  # addr_day → aggregated
            # Підмішати plan_updates/new (з прапорцями _plan_updated)
            for o in new_outages:
                adk = o.get("_addr_day") or addr_day_key(o)
                if adk:
                    active_by_adk[adk] = o
            active_outages = list(active_by_adk.values())

            if active_outages:
                for user, addr in group_pairs:
                    tid = user.get("telegram_id")
                    if not tid:
                        continue
                    street_id = addr.get("street_id") or addr.get("streetId")
                    street_name = (addr.get("street_name") or addr.get("street") or "").strip().lower()
                    house = str(addr.get("house") or addr.get("building") or "")
                    uqk = user_queue_key(addr)
                    addr_label = (
                        f"{(addr.get('street_name') or addr.get('street') or '?').strip()} "
                        f"{house}"
                    ).strip()

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

                        # «Лише фактичний старт» — тільки якщо користувач уже знає
                        # про цей інтервал (base dedup є). Інакше — повне нове
                        # сповіщення з рядком «Фактичний старт» у звичайному тексті.
                        outage_for_user = outage
                        if outage.get("_actual_start_only"):
                            base_dkey = notify_dedup_key(outage, house)
                            if base_dkey not in already_keys:
                                outage_for_user = dict(outage)
                                outage_for_user.pop("_actual_start_only", None)

                        dkey = outage_notify_dedup_key(outage_for_user, house)
                        if dkey in already_keys:
                            continue

                        # внутрішній дедуп у межах одного прогону
                        fp = content_fingerprint(outage_for_user)
                        already_run = False
                        for x in notified.get(tid, []):
                            prev_o = x[0] if isinstance(x, tuple) else x
                            if content_fingerprint(prev_o) == fp and not (
                                outage_for_user.get("_actual_start_only")
                                or prev_o.get("_actual_start_only")
                            ):
                                already_run = True
                                break
                            if outage_notify_dedup_key(prev_o, house) == dkey:
                                already_run = True
                                break
                        if already_run:
                            continue

                        outage_copy = dict(outage_for_user)
                        if uqk:
                            outage_copy["_user_queue"] = uqk

                        notified.setdefault(tid, []).append((outage_copy, addr))
                        matched_fps.add(fp)
                        already_keys.add(dkey)
                        if addr_label and addr_label not in matched_addrs_seen:
                            matched_addrs_seen.add(addr_label)
                            matched_addrs.append(addr_label)

                log.info(
                    "otg=%s city=%s matched_active=%s (addrs=%s active=%s)",
                    otg_id,
                    city_id,
                    len(matched_fps),
                    len(group_pairs),
                    len(active_outages),
                )

            # Запис був у попередньому знімку, з API зник, а datePlanIn ще попереду.
            matched_cancel = 0
            covered_skip = 0
            if early:
                for user, addr in group_pairs:
                    tid = user.get("telegram_id")
                    if not tid:
                        continue
                    street_id = addr.get("street_id") or addr.get("streetId")
                    street_name = (addr.get("street_name") or addr.get("street") or "").strip().lower()
                    house = str(addr.get("house") or addr.get("building") or "")
                    already_keys = {
                        d["dedup_key"]
                        for d in notified_col.find(
                            {"telegram_id": int(tid)},
                            projection={"dedup_key": 1, "_id": 0},
                        )
                    }
                    for outage in early:
                        if not matches_street(outage, street_id, street_name):
                            continue
                        if not matches_building(outage, house):
                            continue
                        if still_covered_for_addr(outage, outages, street_id, street_name, house):
                            covered_skip += 1
                            continue
                        dkey = cancel_notify_dedup_key(outage, house)
                        if dkey in already_keys:
                            continue
                        already_run = False
                        for x in cancelled.get(tid, []):
                            prev_o = x[0] if isinstance(x, tuple) else x
                            if cancel_notify_dedup_key(prev_o, house) == dkey:
                                already_run = True
                                break
                        if already_run:
                            continue
                        outage_copy = dict(outage)
                        outage_copy["_cancelled"] = True
                        cancelled.setdefault(tid, []).append((outage_copy, addr))
                        already_keys.add(dkey)
                        matched_cancel += 1
                log.info(
                    "otg=%s city=%s cancel_matched=%s gone_early=%s covered_skip=%s",
                    otg_id,
                    city_id,
                    matched_cancel,
                    len(early),
                    covered_skip,
                )

            unmatched: List[str] = []
            if active_outages and not matched_fps:
                seen_addr = set()
                for _user, addr in group_pairs:
                    street = (addr.get("street_name") or addr.get("street") or "?").strip()
                    house = str(addr.get("house") or addr.get("building") or "").strip()
                    label = f"{street} {house}".strip()
                    if label in seen_addr:
                        continue
                    seen_addr.add(label)
                    unmatched.append(label)
                    if len(unmatched) >= 8:
                        break

            group_reports.append({
                "otg_id": otg_id,
                "city_id": city_id,
                "city_name": _group_city_name(group_pairs, outages),
                "fetch_ok": True,
                "fetched": len(outages),
                "active": len(active_outages),
                "addrs": len(group_pairs),
                "new": max(
                    0,
                    len(new_outages)
                    - len(plan_updates)
                    - len(actual_start_notices)
                    - len(finished_notices),
                ),
                "plan_upd": len(plan_updates),
                "finished_early": len(finished_notices),
                "actual_start": len(actual_start_notices),
                "gone_early": len(gone),
                "cancel_ready": len(early),
                "skipped_planned": skipped_planned,
                "skipped_gpv": skipped_gpv,
                "matched": len(matched_fps),
                "matched_addrs": matched_addrs,
                "cancel_matched": matched_cancel,
                "covered_skip": covered_skip,
                "unmatched": unmatched,
            })

            # Кеш оновлюємо лише після повного fetch.
            # snapshot — те, що API віддав зараз; pending_cancels — зниклі до кінця,
            # щоб повторити розсилку, якщо Telegram не прийняв повідомлення.
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
                        "snapshot": [snapshot_from_outage(o) for o in outages],
                        "pending_cancels": [pending_cancel_doc(o) for o in pending_gone],
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

    n_new = sum(len(v) for v in notified.values())
    n_cancels = sum(len(v) for v in cancelled.values())
    n_events = n_new + n_cancels
    log_chat = (
        os.getenv("TELEGRAM_LOG_CHANNEL_ID")
        or os.getenv("ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
    )

    async def _dispatch() -> None:
        """Один event loop на всю розсилку.

        Кілька asyncio.run() закривають loop, а HTTPXRequest бота лишається
        прив’язаним до першого — звідси 'Event loop is closed' і Pool timeout.
        """
        nonlocal sent, sent_quiet, sent_sound
        request = HTTPXRequest(
            connection_pool_size=8,
            pool_timeout=10.0,
            connect_timeout=10.0,
            read_timeout=20.0,
            write_timeout=20.0,
        )
        bot = Bot(token=bot_token, request=request)

        async def _send_parts(tid, texts: List[str], silent: bool) -> bool:
            for part_i, text in enumerate(texts):
                try:
                    await bot.send_message(
                        chat_id=tid,
                        text=fit_telegram_html(text),
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        disable_notification=silent,
                    )
                except Exception as e:
                    log.error(
                        "Send fail part %s/%s: %s",
                        part_i + 1,
                        len(texts),
                        e,
                    )
                    return False
                if part_i + 1 < len(texts):
                    await asyncio.sleep(0.25)
            return True

        def _mark(tid, item_list) -> None:
            for outage, addr in item_list:
                if not isinstance(addr, dict):
                    continue
                try:
                    remember_user_notified(notified_col, int(tid), outage, addr)
                except Exception as e:
                    log.error("mark notified fail: %s", e)

        try:
            tids = list(dict.fromkeys([*notified.keys(), *cancelled.keys()]))
            deliveries: List[str] = []
            for tid in tids:
                new_items = notified.get(tid) or []
                gone_items = cancelled.get(tid) or []
                u_doc = _user_by_tid.get(int(tid)) or {}
                silent = is_quiet_now(u_doc)
                new_ok = False
                cancel_ok = False
                if new_items and await _send_parts(tid, build_notify_message(db, new_items), silent):
                    new_ok = True
                    _mark(tid, new_items)
                if gone_items:
                    if new_items:
                        await asyncio.sleep(0.25)
                    if await _send_parts(tid, build_cancel_message(gone_items), silent):
                        cancel_ok = True
                        _mark(tid, gone_items)
                mode = "тихо" if silent else "зі звуком"
                # У повідомленні лог-групи: id + @username (якщо є).
                # У stdout/GitHub Actions — без персональних даних.
                uname = (
                    (u_doc.get("username") or u_doc.get("user_name") or "")
                    .strip()
                    .lstrip("@")
                )
                who = f"{tid}" + (f" @{uname}" if uname else "")
                if new_ok or cancel_ok:
                    deliveries.append(
                        f"{who} — нові {len(new_items)}, скасування {len(gone_items)}, {mode}"
                    )
                else:
                    deliveries.append(
                        f"{who} — не дійшло (нові {len(new_items)}, скасування {len(gone_items)})"
                    )
                if not (new_ok or cancel_ok):
                    continue

                sent += 1
                if silent:
                    sent_quiet += 1
                else:
                    sent_sound += 1
                log.info(
                    "Надіслано new=%s cancel=%s silent=%s",
                    len(new_items),
                    len(gone_items),
                    silent,
                )
                await asyncio.sleep(0.35)

            summary, attachment = _human_run_summary(
                group_reports,
                users=len(users),
                groups_checked=groups_checked,
                n_events=n_events,
                n_new=n_new,
                n_cancels=n_cancels,
                sent=sent,
                sent_sound=sent_sound,
                sent_quiet=sent_quiet,
                deliveries=deliveries,
            )
            log.info(
                "підсумок subscribers=%s locations=%s events=%s sent=%s sound=%s quiet=%s",
                len(users),
                groups_checked,
                n_events,
                sent,
                sent_sound,
                sent_quiet,
            )

            if log_chat:
                try:
                    # 153 — звичайна гілка «Аварії, ремонти крон»
                    # 17725 — коли є що розсилати (нові / скасування)
                    thread_id = 17725 if n_events > 0 else 153
                    await bot.send_message(
                        chat_id=int(log_chat),
                        text=summary,
                        message_thread_id=thread_id,
                    )
                    if attachment:
                        doc = BytesIO(attachment.encode("utf-8"))
                        doc.name = "outages-log.txt"
                        await bot.send_document(
                            chat_id=int(log_chat),
                            document=doc,
                            message_thread_id=thread_id,
                        )
                except Exception as e:
                    log.warning("log channel: %s", e)
        finally:
            await bot.shutdown()

    asyncio.run(_dispatch())

    log.info(
        "Готово groups=%s sent=%s events=%s cancels=%s",
        groups_checked,
        sent,
        n_events,
        n_cancels,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
