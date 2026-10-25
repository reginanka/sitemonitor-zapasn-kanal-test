#!/usr/bin/env python3
"""
Перегенеровує PNG-графіки черг з даних у Mongo.

Джерела:
  - queue_schedules     — слоти / events по чергах
  - ukrenergo2_states   — поточний статус області (title, period) для шапки картинки

Результат:
  - PNG у GridFS (bucket schedule_images)
  - оновлені image_* поля в queue_schedules

Запуск:
  python regenerate_images.py
  python regenerate_images.py --queue 4 --subqueue 2
  python regenerate_images.py --dry-run
  python regenerate_images.py --only-missing

Env (як у інших скриптах репо):
  MAYACHOK_MONGO_URI | MONGO_URI
  ZTOE_MONGO_DB | MONGO_DB | MAYACHOK_MONGO_DB   (default: vidkluchennya)
  UKRENERGO2_REGION_SLUG                         (default: zhytomyr)
  REGENERATE_OPERATOR                            (default: Житомиробленерго)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from mongo_store import get_db, save_schedule_png, _fp
from render_schedule import (
    build_data,
    events_from_slots,
    render_to_png,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("regenerate_images")

REGION_SLUG = (os.getenv("UKRENERGO2_REGION_SLUG") or "zhytomyr").strip().lower()
OPERATOR = (os.getenv("REGENERATE_OPERATOR") or "Житомиробленерго").strip()


# ---------- Ukrenergo status → підписи на картинці ----------

STATUS_FALLBACK = {
    3: ("Електроенергії достатньо", "Споживання в межах можливостей енергосистеми."),
    4: (
        "Споживайте електроенергію ощадливо",
        "Енергосистема працює з дефіцитом.",
    ),
    9: (
        "За розпорядженням НЕК Укренерго застосовано графіки погодинних відключень",
        "Діють графіки обмеження споживання.",
    ),
    10: ("Застосовані аварійні відключення", "Аварійні обмеження споживання."),
    11: ("Пошкодження мереж / генерації", "Є пошкодження об'єктів енергосистеми."),
}


def _parse_iso_kyiv(iso: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        try:
            import pytz

            return dt.astimezone(pytz.timezone("Europe/Kyiv"))
        except Exception:
            return dt.astimezone(timezone(timedelta(hours=3)))
    except Exception:
        return None


def _fmt_period_str(period: Any) -> str:
    """Короткий рядок періоду (для логів / legacy)."""
    if not isinstance(period, dict):
        return str(period or "")
    start = period.get("start") or period.get("from")
    end = period.get("end") or period.get("to")
    if not start and not end:
        return ""

    def _hm(iso: str) -> str:
        dt = _parse_iso_kyiv(iso)
        if dt:
            return dt.strftime("%H:%M")
        s = str(iso)
        if ":" in s and len(s) <= 5:
            return s[:5]
        return s[:16]

    if start and end:
        return f"{_hm(start)}–{_hm(end)}"
    if start:
        return f"з {_hm(start)}"
    return f"до {_hm(end)}"


def _period_for_template(period: Any) -> dict:
    """
    Об'єкт period для нового outage-schedule.html:
      { from: 'HH:MM', to: 'HH:MM' }
      або з fromDate/toDate якщо інтервал через північ / кілька днів.
    """
    if isinstance(period, dict):
        # вже у форматі шаблону
        if period.get("from") and period.get("to") and "start" not in period:
            return {
                "from": str(period["from"])[:5],
                "to": str(period["to"])[:5],
                **(
                    {"fromDate": period["fromDate"], "toDate": period["toDate"]}
                    if period.get("fromDate") and period.get("toDate")
                    else {}
                ),
            }

        start = period.get("start")
        end = period.get("end")
        if start or end:
            ds = _parse_iso_kyiv(start) if start else None
            de = _parse_iso_kyiv(end) if end else None
            out: dict[str, str] = {
                "from": ds.strftime("%H:%M") if ds else "06:00",
                "to": de.strftime("%H:%M") if de else "23:59",
            }
            if ds and de and ds.date() != de.date():
                out["fromDate"] = ds.strftime("%d.%m")
                out["toDate"] = de.strftime("%d.%m")
            return out

    if isinstance(period, str) and period.strip():
        # "06:00–23:59" або подібне
        s = period.strip()
        for sep in ("–", "—", " - ", "-"):
            if sep in s:
                a, b = s.split(sep, 1)
                a, b = a.strip()[:5], b.strip()[:5]
                if ":" in a and ":" in b:
                    return {"from": a, "to": b}
    return {"from": "06:00", "to": "23:59"}


def load_ukrenergo_status(db) -> dict:
    """
    Повертає dict:
      status_title, status_sub, period (dict для шаблону), period_str,
      status_code, emoji, name_uk, raw_title, raw_sub
    """
    doc = db["ukrenergo2_states"].find_one({"slug": REGION_SLUG}) or {}
    code = doc.get("status_code")
    try:
        code_i = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_i = None

    title = (doc.get("title_uk") or "").strip()
    name_uk = (doc.get("name_uk") or "Житомирська область").strip()
    emoji = (doc.get("emoji") or "").strip()

    if not title and code_i in STATUS_FALLBACK:
        title = STATUS_FALLBACK[code_i][0]

    if not title:
        title = "Статус енергосистеми"

    sub = ""
    if code_i in STATUS_FALLBACK:
        sub = STATUS_FALLBACK[code_i][1]
    elif doc.get("status"):
        sub = str(doc.get("status"))

    raw_period = doc.get("period")
    period_str = _fmt_period_str(raw_period)
    period_obj = _period_for_template(raw_period)

    # Для нового шаблону: statusTitle = "Статус Житомирської області"
    # statusSub = довгий текст від Укренерго
    status_title = f"Статус {name_uk}" if name_uk else "Статус області"
    if emoji:
        status_title = f"{emoji} {status_title}".strip()

    # statusSub — основний текст (title з API / fallback)
    status_sub = title

    return {
        "status_title": status_title,
        "status_sub": status_sub,
        "period": period_obj,
        "period_str": period_str,
        "status_code": code_i,
        "emoji": emoji,
        "name_uk": name_uk,
        "raw_title": title,
        "raw_sub": sub,
    }


# ---------- черги ----------

def iter_queue_docs(
    db,
    *,
    only_queue: Optional[str] = None,
    only_subqueue: Optional[str] = None,
):
    q: dict = {}
    if only_queue is not None:
        q["queue"] = str(only_queue).strip()
    if only_subqueue is not None:
        q["subqueue"] = str(only_subqueue).strip()
    cursor = db["queue_schedules"].find(q).sort([("queue", 1), ("subqueue", 1)])
    for doc in cursor:
        yield doc


def _normalize_events(events: list) -> list:
    """text → note, гарантує type/from/to."""
    out = []
    for e in events or []:
        if not isinstance(e, dict):
            continue
        fr = e.get("from") or e.get("start")
        to = e.get("to") or e.get("end")
        if not fr or not to:
            continue
        note = e.get("note") or e.get("text") or ""
        out.append(
            {
                "type": e.get("type") or "schedule",
                "from": str(fr)[:5],
                "to": str(to)[:5],
                "note": note,
            }
        )
    return out


def build_days_from_doc(doc: dict) -> list:
    """Збирає days[] для шаблону з slots / events документа."""
    slots = doc.get("slots") or []
    events = doc.get("events") or []
    known = bool(doc.get("known", True))

    if not events and slots:
        events = events_from_slots(slots)
    events = _normalize_events(events)

    # якщо в документі вже є структуровані days — беремо їх (з нормалізацією)
    if isinstance(doc.get("days"), list) and doc["days"]:
        days = []
        for d in doc["days"]:
            if not isinstance(d, dict):
                continue
            days.append(
                {
                    **d,
                    "events": _normalize_events(d.get("events") or []),
                }
            )
        if days:
            return days

    tomorrow_events = _normalize_events(
        doc.get("events_tomorrow")
        or doc.get("extra", {}).get("slots_tomorrow")
        or []
    )
    # slots_tomorrow може бути списком слотів без type
    if not tomorrow_events:
        raw_tm = (doc.get("extra") or {}).get("slots_tomorrow") or []
        if raw_tm:
            tomorrow_events = events_from_slots(raw_tm)

    tomorrow_known = bool(tomorrow_events) or bool(doc.get("known_tomorrow"))

    day_today = {
        "label": "Сьогодні",
        "offset": 0,
        "known": known,
        "events": events,
    }
    # дата з документа, якщо є
    if doc.get("date"):
        try:
            # YYYY-MM-DD → DD.MM
            ds = str(doc["date"])
            if len(ds) >= 10 and ds[4] == "-":
                day_today["date"] = f"{ds[8:10]}.{ds[5:7]}"
        except Exception:
            pass

    day_tomorrow = {
        "label": "Завтра",
        "offset": 1,
        "known": tomorrow_known,
        "events": tomorrow_events,
    }
    if not tomorrow_known:
        day_tomorrow["waitNote"] = (
            "Розпорядження НЕК Укренерго очікується після 18:00"
        )

    return [day_today, day_tomorrow]


def regenerate_one(
    doc: dict,
    ukr: dict,
    *,
    dry_run: bool = False,
    only_missing: bool = False,
) -> str:
    """
    Повертає статус: ok | skip | dry | error
    """
    queue = str(doc.get("queue") or "").strip()
    subqueue = str(doc.get("subqueue") or "").strip()
    if not queue:
        log.warning("пропуск документа без queue: %s", doc.get("_id"))
        return "skip"

    qlabel = f"{queue}.{subqueue}" if subqueue else queue

    if only_missing and doc.get("image_file_id"):
        log.info("skip (є image_file_id): %s", qlabel)
        return "skip"

    days = build_days_from_doc(doc)
    # планові/аварійні з outages (ГПВ у koment) на шкалу
    try:
        from outage_events import load_outage_events_for_queue, merge_events
        extra = load_outage_events_for_queue(qlabel)
        for i, day in enumerate(days):
            if i in extra and extra[i]:
                day["events"] = merge_events(day.get("events") or [], extra[i])
                day["known"] = True
    except Exception as e:
        log.warning("outage events %s: %s", qlabel, e)

    data = build_data(
        queue=qlabel,
        status_title=ukr["status_title"],
        status_sub=ukr.get("status_sub") or ukr.get("raw_title") or "",
        period=ukr.get("period") or {"from": "06:00", "to": "23:59"},
        operator=OPERATOR,
        days=days,
    )

    log.info(
        "рендер %s | status=%s | events_today=%s | period=%s",
        qlabel,
        ukr.get("status_code"),
        len(days[0].get("events") or []) if days else 0,
        ukr.get("period_str") or ukr.get("period"),
    )

    if dry_run:
        return "dry"

    try:
        png = render_to_png(data)
    except Exception as e:
        log.error("render failed %s: %s", qlabel, e)
        return "error"

    fp = _fp(
        queue,
        subqueue,
        str(doc.get("date") or ""),
        str(doc.get("slots") or doc.get("events") or ""),
        str(ukr.get("status_code")),
        str(len(png)),
    )

    try:
        file_id = save_schedule_png(queue, subqueue, png, fingerprint=fp)
        log.info("✓ %s → GridFS %s (%d bytes)", qlabel, file_id, len(png))
        return "ok"
    except Exception as e:
        log.error("save failed %s: %s", qlabel, e)
        return "error"


def main() -> int:
    ap = argparse.ArgumentParser(description="Перегенерація PNG графіків черг з Mongo")
    ap.add_argument("--queue", default=None, help="лише ця черга")
    ap.add_argument("--subqueue", default=None, help="лише ця підчерга")
    ap.add_argument("--dry-run", action="store_true", help="не писати в Mongo/GridFS")
    ap.add_argument(
        "--only-missing",
        action="store_true",
        help="лише документи без image_file_id",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="макс. кількість черг (0 = усі)",
    )
    args = ap.parse_args()

    log.info("=" * 40)
    log.info("REGENERATE IMAGES · slug=%s", REGION_SLUG)

    try:
        db = get_db()
    except Exception as e:
        log.error("Mongo: %s", e)
        return 1

    ukr = load_ukrenergo_status(db)
    log.info(
        "Укренерго: code=%s title=%r period=%r",
        ukr.get("status_code"),
        ukr.get("raw_title"),
        ukr.get("period_str") or ukr.get("period"),
    )

    stats = {"ok": 0, "skip": 0, "dry": 0, "error": 0, "total": 0}
    for doc in iter_queue_docs(
        db, only_queue=args.queue, only_subqueue=args.subqueue
    ):
        stats["total"] += 1
        if args.limit and stats["ok"] + stats["dry"] + stats["error"] >= args.limit:
            log.info("досягнуто --limit=%s", args.limit)
            break
        status = regenerate_one(
            doc,
            ukr,
            dry_run=args.dry_run,
            only_missing=args.only_missing,
        )
        stats[status] = stats.get(status, 0) + 1

    log.info(
        "готово: total=%s ok=%s skip=%s dry=%s error=%s",
        stats["total"],
        stats["ok"],
        stats["skip"],
        stats["dry"],
        stats["error"],
    )
    return 1 if stats["error"] else 0


if __name__ == "__main__":
    sys.exit(main())
