"""Планові/аварійні з Mongo outages → events для PNG-графіка черги.

Шаблон already supports:
  type: schedule | planned | emerg
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

_GPV_RE = re.compile(
    r"(?:ГПВ|черг[аи]?)\s*[-–]?\s*(\d+)\s*[.,]\s*(\d+)",
    re.IGNORECASE,
)


def parse_queue_from_koment(koment: str) -> Optional[str]:
    if not koment:
        return None
    m = _GPV_RE.search(koment)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return None


def _queue_parts(label: str) -> Tuple[str, str]:
    s = (label or "").strip().replace("|", ".")
    if "." in s:
        a, b = s.split(".", 1)
        return a.strip(), b.strip()
    return s, ""


def _hhmm(iso: str) -> str:
    s = (iso or "").strip()
    if not s:
        return ""
    m = re.search(r"T(\d{2}:\d{2})", s)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{2}:\d{2})\b", s)
    return m.group(1) if m else ""


def _day_offset_for_iso(iso: str, now: datetime | None = None) -> Optional[int]:
    if not iso:
        return None
    now = now or datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(timezone(timedelta(hours=3)))
        today = now.astimezone(timezone(timedelta(hours=3))).date()
        d = local.date()
        if d == today:
            return 0
        if d == today + timedelta(days=1):
            return 1
    except Exception:
        return None
    return None


def type_to_event_type(type_off) -> str:
    try:
        t = int(type_off)
    except (TypeError, ValueError):
        return "planned"
    if t == 1:
        return "emerg"
    return "planned"


def load_outage_events_for_queue(queue_label: str, *, db=None) -> dict:
    """
    {0: [events...], 1: [events...]} — сьогодні / завтра.
    Шукає outages з ГПВ у koment або з полем queue_key.
    """
    result = {0: [], 1: []}
    ch, pid = _queue_parts(queue_label)
    label = f"{ch}.{pid}" if pid else ch
    if not ch:
        return result

    try:
        if db is None:
            from mongo_store import get_db
            db = get_db()
        ors = [
            {
                "koment": {
                    "$regex": rf"ГПВ\s*[-–]?\s*{re.escape(ch)}\s*[.,]\s*{re.escape(pid or '')}",
                    "$options": "i",
                }
            },
            {"queue_key": label},
            {"queue_key": f"{ch}|{pid}" if pid else ch},
        ]
        if pid:
            ors.append({"queue": ch, "subqueue": pid})
        cursor = db["outages"].find({"$or": ors}).limit(200)
    except Exception:
        return result

    seen = set()
    for doc in cursor:
        fr = _hhmm(doc.get("dateEvent") or "")
        to = _hhmm(doc.get("datePlanIn") or "") or fr
        if not fr:
            continue
        off = _day_offset_for_iso(doc.get("dateEvent") or "")
        if off is None:
            continue
        et = type_to_event_type(doc.get("typeOff"))
        key = (off, fr, to, et)
        if key in seen:
            continue
        seen.add(key)
        note = (doc.get("koment") or "").strip()[:40]
        result[off].append({"type": et, "from": fr, "to": to, "note": note})
    return result


def merge_events(base: list, extra: list) -> list:
    out = list(base or [])
    have = {(e.get("from"), e.get("to"), e.get("type")) for e in out}
    for e in extra or []:
        k = (e.get("from"), e.get("to"), e.get("type"))
        if k not in have and e.get("from") and e.get("to"):
            out.append(e)
            have.add(k)
    return out
