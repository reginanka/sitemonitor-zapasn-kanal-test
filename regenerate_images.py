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
        "Застосовані графіки погодинних відключень",
        "Діють графіки обмеження споживання.",
    ),
    10: ("Застосовані аварійні відключення", "Аварійні обмеження споживання."),
    11: ("Пошкодження мереж / генерації", "Є пошкодження об'єктів енергосистеми."),
}


def _fmt_period(period: Any) -> str:
    """Короткий рядок періоду для шапки картинки."""
    if not isinstance(period, dict):
        return ""
    start = period.get("start")
    end = period.get("end")
    if not start and not end:
        return ""

    def _hm(iso: str) -> str:
        try:
            dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            try:
                import pytz

                dt = dt.astimezone(pytz.timezone("Europe/Kyiv"))
            except Exception:
                dt = dt.astimezone(timezone(timedelta(hours=3)))
            return dt.strftime("%H:%M")
        except Exception:
            return str(iso)[:16]

    if start and end:
        return f"{_hm(start)}–{_hm(end)}"
    if start:
        return f"з {_hm(start)}"
    return f"до {_hm(end)}"


def load_ukrenergo_status(db) -> dict:
    """
    Повертає dict:
      status_title, status_sub, period, status_code, emoji, name_uk
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

    # підзаголовок: body з fallback або status
    sub = ""
    if code_i in STATUS_FALLBACK:
        sub = STATUS_FALLBACK[code_i][1]
    elif doc.get("status"):
        sub = str(doc.get("status"))

    period_str = _fmt_period(doc.get("period"))
    if period_str:
        sub = f"{sub} · {period_str}".strip(" ·")

    status_title = f"{emoji} {name_uk}".strip() if emoji else name_uk

    return {
        "status_title": status_title,
        "status_sub": title if not sub else f"{title}",
        "period": period_str,
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


def build_days_from_doc(doc: dict) -> list:
    """Збирає days[] для шаблону з slots / events документа."""
    slots = doc.get("slots") or []
    events = doc.get("events") or []
    known = bool(doc.get("known", True))

    if not events and slots:
        events = events_from_slots(slots)

    # якщо в документі вже є структуровані days — беремо їх
    if isinstance(doc.get("days"), list) and doc["days"]:
        return doc["days"]

    return [
        {
            "label": "Сьогодні",
            "offset": 0,
            "known": known,
            "events": events,
        },
        {
            "label": "Завтра",
            "offset": 1,
            "known": False,
            "events": doc.get("events_tomorrow") or [],
        },
    ]


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
    _id = doc.get("_id") or (f"{queue}|{subqueue}" if subqueue else queue)

    if only_missing and doc.get("image_file_id"):
        log.info("skip (є image_file_id): %s", qlabel)
        return "skip"

    days = build_days_from_doc(doc)
    data = build_data(
        queue=qlabel,
        status_title=ukr["status_title"],
        status_sub=ukr.get("raw_title") or ukr["status_sub"],
        period=ukr.get("period") or "",
        operator=OPERATOR,
        days=days,
    )

    log.info(
        "рендер %s | status=%s | events_today=%s",
        qlabel,
        ukr.get("status_code"),
        len(days[0].get("events") or []) if days else 0,
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
        ukr.get("period"),
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
