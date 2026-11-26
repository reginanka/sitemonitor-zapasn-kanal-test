#!/usr/bin/env python3
"""
ГПВ ЖОЕ проти графіка черги підписника.

Окремо від ztoe_outages_monitor.py (той файл не імпортується і не змінюється):
там switchSubReason=2 свідомо викидається, бо вікно ГПВ часто збігається
з чергою, і людям із графіком його вже шле monitor.py.

Тут:
  1. Тягнемо /pw_accidents по city.id (fallback otg.id, якщо міста немає).
  2. Лишаємо лише ГПВ: switchSubReason=2, або «ГПВ» у koment і це не ГАВ/СГАВ.
  3. Місто + вулиця + будинок, як у моніторі аварій (033 ≡ 33, 33а ≡ 33А).
     Порожній buildingNames на конкретний будинок не розмазуємо.
  4. Далі:
     - черга є, кожне вікно ГПВ входить у slots цієї черги на цей день
       (до хвилини, не «списки рівні») → тиша;
     - черга є, день графіка відомий, а вікна в слотах немає
       (інші години або слотів на день порожньо) → «не збігається»;
     - no_gpv / «не вимикають, крім аварійних» → «з’явилось погодинне»;
     - черга ще порожня (довідник не проставив) → пропуск, це не no_gpv.
  5. Графік у queue_schedules є лише на дати з документа (сьогодні і, якщо
     є, завтра). Інший день, немає документа або date не той → пропуск,
     не тривога.
  6. Час з API (+00:00 / Z) — київський годинник, без зсуву +2.
     Секунди відрізаються: 13:00:29 і 13:00 — одне й те саме.
  7. Дедуп — колекція user_gpv_notified, не user_outage_notified.
     Ключ пишеться лише після успішної відправки.

Env:
  ZTOE_API_BASE / API_BASE, MONGO_URI, MONGO_DB, ZTOE_BOT_TOKEN,
  TELEGRAM_LOG_CHANNEL_ID,
  GPV_LOG_THREAD_ID (тихий підсумок, default 155 — гілка черг),
  GPV_LOG_THREAD_ALERT_ID (є розсилка, default 17725),
  GPV_HORIZON_DAYS (default 1 = сьогодні й завтра; <0 без обмеження),
  GPV_NOTIFIED_TTL_DAYS (default 7),
  GPV_DRY_RUN=1 — нічого не слати й не писати дедуп.
"""

from __future__ import annotations

import argparse
import asyncio
import html
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Set, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_gpv_monitor")

Window = Tuple[str, str, str]  # ymd, HH:MM, HH:MM
Slot = Tuple[str, str]

SUBREASON_GPV = 2
SUBREASON_GAV = 1
SUBREASON_SGAV = 3

NO_GPV_MARKERS = (
    "не вимикається",
    "не вимикають",
    "не відключається",
    "не відключають",
    "без відключень",
)

GPV_HORIZON_DAYS = int(os.getenv("GPV_HORIZON_DAYS", "1"))
GPV_NOTIFIED_TTL_DAYS = max(1, int(os.getenv("GPV_NOTIFIED_TTL_DAYS", "7")))
GPV_COLLECTION = os.getenv("GPV_NOTIFIED_COLLECTION", "user_gpv_notified")
GPV_LOG_THREAD_ID = int(os.getenv("GPV_LOG_THREAD_ID", "155"))
GPV_LOG_THREAD_ALERT_ID = int(os.getenv("GPV_LOG_THREAD_ALERT_ID", "17725"))
TG_TEXT_LIMIT = 4096


def _api_base() -> str:
    return (os.getenv("ZTOE_API_BASE") or os.getenv("API_BASE") or "").rstrip("/")


def _kyiv_zone():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("Europe/Kyiv")
    except Exception:
        return timezone(timedelta(hours=2))


def kyiv_today() -> datetime:
    return datetime.now(_kyiv_zone()).replace(tzinfo=None)


# ----- час: цифри API це київська стіна, секунди не рахуємо -----

def wall_naive(iso: str) -> Optional[datetime]:
    """13:00:29+00:00 → 13:00, не 15:00. Офсет відкидаємо, не конвертуємо."""
    raw = (iso or "").strip()
    if not raw:
        return None
    raw = raw.replace("Z", "")
    raw = re.sub(r"[+-]\d{2}:?\d{2}$", "", raw).strip()
    raw = raw.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt.replace(tzinfo=None, second=0, microsecond=0)


def windows_from_interval(date_event: str, date_plan: str) -> List[Window]:
    """Ріже [start, end) по календарних днях. Кінець рівно опівночі — це 24:00 попереднього дня."""
    start = wall_naive(date_event)
    end = wall_naive(date_plan)
    if start is None or end is None or end <= start:
        return []
    out: List[Window] = []
    cur = start
    for _ in range(4):
        next_midnight = datetime.combine(cur.date() + timedelta(days=1), datetime.min.time())
        piece_end = min(end, next_midnight)
        if piece_end > cur:
            ymd = cur.strftime("%Y-%m-%d")
            a = cur.strftime("%H:%M")
            b = "24:00" if piece_end >= next_midnight else piece_end.strftime("%H:%M")
            if a != b:
                out.append((ymd, a, b))
        if piece_end >= end:
            break
        cur = piece_end
    return out


def _parse_hhmm(t: str) -> Optional[int]:
    m = re.match(r"^(\d{1,2}):(\d{2})", (t or "").strip())
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if mi > 59 or h > 24 or (h == 24 and mi != 0):
        return None
    if h == 24:
        return 24 * 60
    return h * 60 + mi


def _minutes_to_hhmm(m: int) -> str:
    if m >= 24 * 60:
        return "24:00"
    return f"{m // 60:02d}:{m % 60:02d}"


def _as_minutes(fr: str, to: str) -> Optional[Tuple[int, int]]:
    a, b = _parse_hhmm(fr), _parse_hhmm(to)
    if a is None or b is None:
        return None
    if b <= a:
        b += 24 * 60
    return a, b


def merge_slots(slots: Sequence[Slot]) -> List[Slot]:
    parsed: List[Tuple[int, int]] = []
    for fr, to in slots:
        pair = _as_minutes(fr, to)
        if pair:
            parsed.append(pair)
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


def subtract_slots(windows: Sequence[Slot], covers: Sequence[Slot]) -> List[Slot]:
    """Що від вікон лишилось після зняття слотів. Порожньо → вікно повністю всередині."""
    cover_m = []
    for fr, to in merge_slots(covers):
        pair = _as_minutes(fr, to)
        if pair:
            cover_m.append(pair)
    result: List[Tuple[int, int]] = []
    for fr, to in windows:
        pair = _as_minutes(fr, to)
        if not pair:
            continue
        pieces = [pair]
        for ca, cb in cover_m:
            nxt = []
            for pa, pb in pieces:
                if cb <= pa or ca >= pb:
                    nxt.append((pa, pb))
                    continue
                if pa < ca:
                    nxt.append((pa, ca))
                if pb > cb:
                    nxt.append((cb, pb))
            pieces = nxt
        result.extend(pieces)
    return [(_minutes_to_hhmm(a), _minutes_to_hhmm(b)) for a, b in result if b > a]


def window_inside(fr: str, to: str, slots: Sequence[Slot]) -> bool:
    if not slots:
        return False
    return not subtract_slots([(fr, to)], slots)


def normalize_ymd(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    head = raw[:10]
    for fmt, chunk in (("%Y-%m-%d", head), ("%d.%m.%Y", head), ("%d.%m.%y", head)):
        try:
            return datetime.strptime(chunk, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def within_horizon(ymd: str, today: Optional[datetime] = None) -> bool:
    """Минулі дні — ні. Далі ніж GPV_HORIZON_DAYS від сьогодні — ні. <0 = без краю."""
    try:
        day = datetime.strptime(ymd[:10], "%Y-%m-%d").date()
    except ValueError:
        return False
    base = (today or kyiv_today()).date()
    if day < base:
        return False
    if GPV_HORIZON_DAYS < 0:
        return True
    return day <= base + timedelta(days=GPV_HORIZON_DAYS)


def _fmt_ddmmyyyy(ymd: str) -> str:
    try:
        return datetime.strptime(ymd[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return ymd


def _fmt_ranges(slots: Sequence[Slot]) -> str:
    merged = merge_slots(slots)
    if not merged:
        return ""
    return ", ".join(f"{a}-{b}" for a, b in merged)


# ----- вулиця / будинок (копія логіки монітора аварій, без імпорту) -----

def _norm_street(name: str) -> str:
    s = (name or "").strip().lower()
    for pfx in (
        "вул.", "вул ", "вулиця ", "пров.", "провулок ",
        "проспект ", "пр.", "бульвар ", "б-р ", "пл.", "площа ",
        "м.", "м ",
    ):
        if s.startswith(pfx):
            s = s[len(pfx):].strip()
    s = s.replace("'", "").replace("’", "").replace("`", "")
    return " ".join(s.split())


def matches_street(outage: dict, street_id, street_name: str) -> bool:
    o_street = outage.get("street") or {}
    o_street_id = o_street.get("id")
    if street_id and o_street_id is not None and str(o_street_id) == str(street_id):
        return True
    a = _norm_street(street_name)
    b = _norm_street(o_street.get("name") or "")
    if not a or not b:
        return False
    return a == b or a in b or b in a


def _parse_house(h: str) -> Tuple[str, str]:
    h = (h or "").strip().lower().replace(" ", "").replace("-", "")
    m = re.match(r"^0*(\d+)([а-яa-z]*)$", h, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)
    return h, ""


def matches_building(outage: dict, house: str) -> bool:
    """
    033 ≡ 33, 33-А ≡ 33а. 33 не збігається з 330.
    Без літери в адресі підходить і 33а (як у моніторі аварій).
    Порожній список будинків у ЖОЕ на конкретний дім не чіпаємо:
    інакше одне вікно вулиці розійдеться по всіх домах.
    """
    house = (house or "").strip()
    buildings = (outage.get("buildingNames") or "").strip()
    if not house:
        return True
    if not buildings:
        return False
    tokens = {
        p.strip().lower()
        for p in buildings.replace(";", ",").split(",")
        if p.strip()
    }
    user_num, user_suf = _parse_house(house)
    if not user_num:
        return False
    for t in tokens:
        tok_num, tok_suf = _parse_house(t)
        if tok_num != user_num:
            continue
        if not user_suf or user_suf == tok_suf:
            return True
    return False


def _is_no_gpv_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t or re.match(r"^[\d.|]+$", t):
        return False
    return any(m in t for m in NO_GPV_MARKERS)


def is_no_gpv_addr(addr: dict) -> bool:
    if addr.get("no_gpv") is True:
        return True
    return _is_no_gpv_text(str(addr.get("queue") or "")) or _is_no_gpv_text(
        str(addr.get("subqueue") or "")
    )


def queue_label(addr: dict) -> str:
    """'3'+'1' → '3.1'. Текст «не вимикають…» і порожня черга → ''."""
    if is_no_gpv_addr(addr):
        return ""
    q = str(addr.get("queue") or "").strip()
    sq = str(addr.get("subqueue") or "").strip()
    if not q:
        return ""
    if "." in q or "|" in q:
        label = q.replace("|", ".")
        return label if re.fullmatch(r"\d+(\.\d+)?", label) else ""
    if not re.fullmatch(r"\d+", q):
        return ""
    if sq and re.fullmatch(r"\d+", sq):
        return f"{q}.{sq}"
    return q


def _switch_sub_reason(outage: dict) -> int:
    try:
        return int(outage.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        return 0


def is_gpv_record(outage: dict) -> bool:
    sub = _switch_sub_reason(outage)
    if sub == SUBREASON_GPV:
        return True
    if sub in (SUBREASON_GAV, SUBREASON_SGAV):
        return False
    return "ГПВ" in (outage.get("koment") or "").upper()


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def user_wants_hourly(user: dict) -> bool:
    if not user or user.get("notify", True) is False:
        return False
    prefs = user.get("notify_prefs")
    if isinstance(prefs, dict) and "hourly" in prefs:
        return bool(prefs.get("hourly"))
    return True


def is_quiet_now(user: dict) -> bool:
    if not user or not user.get("quiet_mode"):
        return False
    def clamp_h(v, default):
        try:
            return int(v) % 24
        except (TypeError, ValueError):
            return default
    def clamp_m(v, default):
        try:
            m = int(v)
        except (TypeError, ValueError):
            return default
        return 0 if m < 0 else 59 if m > 59 else m
    start = clamp_h(user.get("quiet_from", 23), 23) * 60 + clamp_m(user.get("quiet_from_min", 0), 0)
    end = clamp_h(user.get("quiet_to", 7), 7) * 60 + clamp_m(user.get("quiet_to_min", 0), 0)
    now = datetime.now(_kyiv_zone())
    cur = now.hour * 60 + now.minute
    if start == end:
        return True
    if start < end:
        return start <= cur < end
    return cur >= start or cur < end


def h(value) -> str:
    return html.escape("" if value is None else str(value), quote=False)


def addr_line(addr: dict) -> str:
    city = (addr.get("city_name") or addr.get("city") or "").strip()
    street = (addr.get("street_name") or addr.get("street") or "").strip()
    house = str(addr.get("house") or addr.get("building") or "").strip()
    parts = [p for p in (city, street, house) if p]
    return ", ".join(parts) if parts else "адреса"


# ----- графік черги -----

def _queue_ids(label: str) -> List[str]:
    s = (label or "").replace("|", ".").strip()
    if not s:
        return []
    parts = s.split(".")
    ch, pid = parts[0], (parts[1] if len(parts) > 1 else "")
    ids = []
    for item in (
        f"{ch}|{pid}" if pid else ch,
        f"{ch}.{pid}" if pid else ch,
        label,
        ch,
    ):
        if item and item not in ids:
            ids.append(item)
    return ids


def load_queue_doc(db, label: str) -> Optional[dict]:
    if db is None or not label:
        return None
    col = db["queue_schedules"]
    for _id in _queue_ids(label):
        doc = col.find_one({"_id": _id})
        if doc:
            return doc
    parts = label.replace("|", ".").split(".")
    ch, pid = parts[0], (parts[1] if len(parts) > 1 else "")
    if pid:
        doc = col.find_one({"queue": ch, "subqueue": pid})
        if doc:
            return doc
    return None


def _doc_slots(doc: dict, day: int) -> List[Slot]:
    extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
    if day == 1:
        raw = (
            doc.get("slots_tomorrow")
            or extra.get("slots_tomorrow")
            or doc.get("events_tomorrow")
            or []
        )
    else:
        raw = doc.get("slots") or doc.get("events") or extra.get("slots") or []
    out: List[Slot] = []
    for s in raw or []:
        if not isinstance(s, dict):
            continue
        fr = (s.get("from") or s.get("start") or "").strip()[:5]
        to = (s.get("to") or s.get("end") or "").strip()[:5]
        if fr and to:
            out.append((fr, to))
    return merge_slots(out)


def explicit_schedule_days(doc: dict) -> List[str]:
    extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
    raw = doc.get("dates_in_source")
    if not raw:
        raw = extra.get("dates_in_source") or []
    days = sorted({normalize_ymd(str(d)) for d in raw if normalize_ymd(str(d))})
    return days


def schedule_day_slots(doc: Optional[dict], ymd: str) -> Tuple[int, List[Slot]]:
    """
    0/1 і слоти, якщо цей ymd реально є в графіку.
    -1 → дня в документі немає (не тривожити).

    Поле date і перша дата з dates_in_source — день слотів `slots`
    (monitor.py кладе туди dates[0]). Друга дата списку — `slots_tomorrow`,
    навіть якщо слоти порожні: день опублікований без відключень.
    Дати, яких у списку немає, не звіряємо. Без dates_in_source віримо
    лише полю date; порожній slots_tomorrow сам по собі завтра не доводить.
    """
    if not doc or not ymd:
        return -1, []
    explicit = set(explicit_schedule_days(doc))
    anchor = normalize_ymd(str(doc.get("date") or ""))
    ordered = sorted(explicit)
    # monitor.py: date = dates[0] (slots), slots_tomorrow = dates[1], дати вже відсортовані.
    day0 = anchor or (ordered[0] if ordered else "")
    day1 = ordered[1] if len(ordered) >= 2 and ordered[1] != day0 else ""
    if day0 and ymd == day0:
        return 0, _doc_slots(doc, 0)
    if day1 and ymd == day1:
        return 1, _doc_slots(doc, 1)
    return -1, []


# ----- рішення по адресі -----

def gpv_windows(outage: dict) -> List[Window]:
    return windows_from_interval(
        str(outage.get("dateEvent") or ""),
        str(outage.get("datePlanIn") or ""),
    )


def collect_addr_windows(outages: Sequence[dict], addr: dict) -> List[Window]:
    street_id = addr.get("street_id") or addr.get("streetId")
    street_name = (addr.get("street_name") or addr.get("street") or "").strip()
    house = str(addr.get("house") or addr.get("building") or "")
    seen: Set[Window] = set()
    ordered: List[Window] = []
    for outage in outages:
        if not is_gpv_record(outage):
            continue
        if not matches_street(outage, street_id, street_name):
            continue
        if not matches_building(outage, house):
            continue
        for w in gpv_windows(outage):
            if w not in seen:
                seen.add(w)
                ordered.append(w)
    ordered.sort()
    return ordered


def dedup_key(kind: str, addr: dict, window: Window) -> str:
    city = addr.get("city_id") or addr.get("cityId") or (addr.get("city_name") or "")
    street = _norm_street(addr.get("street_name") or addr.get("street") or "")
    num, suf = _parse_house(str(addr.get("house") or addr.get("building") or ""))
    house = f"{num}{suf}"
    ymd, fr, to = window
    q = queue_label(addr) or ("nogpv" if is_no_gpv_addr(addr) else "")
    return f"{kind}|{city}|{street}|{house}|{q}|{ymd}|{fr}-{to}"


class Decision:
    __slots__ = ("kind", "addr", "ymd", "windows", "slots", "queue")

    def __init__(self, kind: str, addr: dict, ymd: str, windows: List[Window], slots: List[Slot], queue: str):
        self.kind = kind
        self.addr = addr
        self.ymd = ymd
        self.windows = windows
        self.slots = slots
        self.queue = queue


def decide_address(
    db,
    addr: dict,
    outages: Sequence[dict],
    *,
    sched_cache: Optional[Dict[str, Optional[dict]]] = None,
    today: Optional[datetime] = None,
) -> Tuple[List[Decision], Dict[str, int]]:
    """Повертає лише те, про що треба писати (mismatch / appeared) + лічильники."""
    stats = {
        "match_windows": 0,
        "mismatch_windows": 0,
        "appeared_windows": 0,
        "skip_no_queue": 0,
        "skip_no_sched": 0,
        "skip_horizon": 0,
        "skip_past": 0,
    }
    windows = collect_addr_windows(outages, addr)
    if not windows:
        return [], stats

    by_day: Dict[str, List[Window]] = {}
    for w in windows:
        by_day.setdefault(w[0], []).append(w)

    no_gpv = is_no_gpv_addr(addr)
    label = queue_label(addr)
    alerts: List[Decision] = []

    doc = None
    if not no_gpv and label:
        if sched_cache is not None and label in sched_cache:
            doc = sched_cache[label]
        else:
            doc = load_queue_doc(db, label)
            if sched_cache is not None:
                sched_cache[label] = doc

    for ymd, day_windows in sorted(by_day.items()):
        base = (today or kyiv_today()).date()
        try:
            day = datetime.strptime(ymd, "%Y-%m-%d").date()
        except ValueError:
            stats["skip_horizon"] += len(day_windows)
            continue
        if day < base:
            stats["skip_past"] += len(day_windows)
            continue
        if not within_horizon(ymd, today):
            stats["skip_horizon"] += len(day_windows)
            continue

        if no_gpv:
            stats["appeared_windows"] += len(day_windows)
            alerts.append(Decision("appeared", addr, ymd, day_windows, [], ""))
            continue
        if not label:
            stats["skip_no_queue"] += len(day_windows)
            continue
        if doc is None:
            stats["skip_no_sched"] += len(day_windows)
            continue
        idx, slots = schedule_day_slots(doc, ymd)
        if idx < 0:
            stats["skip_no_sched"] += len(day_windows)
            continue
        bad = [w for w in day_windows if not window_inside(w[1], w[2], slots)]
        good = len(day_windows) - len(bad)
        stats["match_windows"] += good
        if bad:
            stats["mismatch_windows"] += len(bad)
            alerts.append(Decision("mismatch", addr, ymd, bad, slots, label))
    return alerts, stats


def format_decision(d: Decision) -> str:
    lines = [f"📍 {h(addr_line(d.addr))}"]
    if d.kind == "mismatch" and d.queue:
        lines.append(f"🔢 Черга {h(d.queue)}")
    lines.append(f"📅 {h(_fmt_ddmmyyyy(d.ymd))}")
    hours = _fmt_ranges([(w[1], w[2]) for w in d.windows])
    if d.kind == "appeared":
        lines.append(f"⏰ ГПВ: <b>{h(hours)}</b>")
        lines.append("ℹ️ За цією адресою відключень за графіком не мало бути (лише аварійні).")
    else:
        lines.append(f"⏰ ЖОЕ: <b>{h(hours)}</b>")
        slot_txt = _fmt_ranges(d.slots)
        if slot_txt:
            lines.append(f"🪫 Графік черги: {h(slot_txt)}")
        else:
            lines.append("🪫 Графік черги: відключень на цей день немає")
    return "\n".join(lines)


def pack_messages(blocks: Sequence[str], header: str, limit: int = TG_TEXT_LIMIT) -> List[str]:
    msgs: List[str] = []
    cur = header.strip()
    cont = "⚠️ <b>Продовження:</b>"
    for block in blocks:
        block = (block or "").strip()
        if not block:
            continue
        cand = f"{cur}\n\n{block}" if cur else block
        if len(cand) <= limit:
            cur = cand
            continue
        if cur:
            msgs.append(cur)
        cur = f"{cont}\n\n{block}"
        if len(cur) > limit:
            cur = block[: limit - 1]
    if cur:
        msgs.append(cur)
    return msgs


def build_user_messages(decisions: Sequence[Decision]) -> List[Tuple[List[str], List[Decision]]]:
    """(тексти, decisions у пакеті). Дедуп пишемо лише якщо пішли всі частини пакета."""
    grouped: List[Tuple[List[str], List[Decision]]] = []
    for kind, header in (
        ("mismatch", "⚠️ <b>ГПВ не збігається з графіком черги по твоїх адресах:</b>"),
        ("appeared", "⚠️ <b>По адресах без графіка з’явилось погодинне відключення:</b>"),
    ):
        items = [d for d in decisions if d.kind == kind]
        if not items:
            continue
        grouped.append((pack_messages([format_decision(d) for d in items], header), items))
    return grouped


# ----- HTTP / mongo -----

def _fetch_pages(client, path: str, extra: dict) -> List[dict]:
    items: List[dict] = []
    page = 1
    while page <= 40:
        params = {"page": page, "itemsPerPage": 100, **extra}
        r = client.get(path, params=params)
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


def fetch_accidents(client, *, city_id: Optional[int] = None, otg_id: Optional[int] = None) -> Tuple[List[dict], bool]:
    base = _api_base()
    try:
        if city_id:
            items = _fetch_pages(client, f"{base}/pw_accidents", {"city.id": int(city_id)})
        else:
            items = _fetch_pages(client, f"{base}/pw_accidents", {"otg.id": int(otg_id)})
    except Exception as e:
        log.error("fetch failed city=%s otg=%s: %s", city_id, otg_id, e)
        return [], False
    for item in items:
        item["_source"] = "accidents"
    return items, True


def _sum_stats(into: Dict[str, int], part: Dict[str, int]) -> None:
    for k, v in part.items():
        into[k] = into.get(k, 0) + int(v)


def _fresh_decisions(decisions: Sequence[Decision], already: Set[str]) -> List[Decision]:
    fresh: List[Decision] = []
    for d in decisions:
        windows = [w for w in d.windows if dedup_key(d.kind, d.addr, w) not in already]
        if not windows:
            continue
        fresh.append(Decision(d.kind, d.addr, d.ymd, windows, d.slots, d.queue))
    return fresh


def remember_notified(col, tid: int, decisions: Sequence[Decision]) -> None:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(days=GPV_NOTIFIED_TTL_DAYS)
    for d in decisions:
        for w in d.windows:
            key = dedup_key(d.kind, d.addr, w)
            col.update_one(
                {"_id": f"{int(tid)}:{key}"},
                {"$set": {
                    "telegram_id": int(tid),
                    "dedup_key": key,
                    "kind": d.kind,
                    "notified_at": now,
                    "expires_at": exp,
                }},
                upsert=True,
            )


def _human_summary(
    *,
    users: int,
    locations: int,
    fetch_fail: int,
    gpv_records: int,
    stats: Dict[str, int],
    sent: int,
    sent_sound: int,
    sent_quiet: int,
    dry: bool,
    deliveries: Sequence[str],
) -> str:
    lines = [
        "✅ <b>Перевірка ГПВ проти черг</b>",
        f"Підписників: <b>{users}</b> · Локацій: <b>{locations}</b>",
        (
            f"Записів ГПВ: <b>{gpv_records}</b>"
            + (f" · ЖОЕ не відповіло: <b>{fetch_fail}</b>" if fetch_fail else "")
        ),
        (
            f"Збіглось із графіком (тиша): <b>{stats.get('match_windows', 0)}</b> · "
            f"Розбіжностей: <b>{stats.get('mismatch_windows', 0)}</b> · "
            f"Нових на адресах без графіка: <b>{stats.get('appeared_windows', 0)}</b>"
        ),
        (
            f"Пропуск: немає черги {stats.get('skip_no_queue', 0)}, "
            f"немає графіка на день {stats.get('skip_no_sched', 0)}, "
            f"поза вікном {stats.get('skip_horizon', 0)}, "
            f"минулі {stats.get('skip_past', 0)}"
        ),
        f"Отримали: <b>{sent}</b> (звук {sent_sound} / тихо {sent_quiet})"
        + (" · dry-run" if dry else ""),
    ]
    if deliveries:
        lines.append("")
        lines.append("<b>Кому:</b>")
        for row in deliveries[:40]:
            lines.append(h(row))
        if len(deliveries) > 40:
            lines.append(h(f"… ще {len(deliveries) - 40}"))
    return "\n".join(lines)


def main() -> int:
    api = _api_base()
    if not api:
        log.error("Немає ZTOE_API_BASE")
        return 1
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        log.error("Немає MONGO_URI")
        return 1
    bot_token = os.getenv("ZTOE_BOT_TOKEN")
    if not bot_token and os.getenv("GPV_DRY_RUN") != "1":
        log.error("Немає ZTOE_BOT_TOKEN")
        return 1

    import httpx
    from pymongo import MongoClient

    db_name = os.getenv("MONGO_DB") or "vidkluchennya"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    users_col = db["users"]
    notified_col = db[GPV_COLLECTION]

    users = [u for u in users_col.find({"notify": True}) if user_wants_hourly(u)]
    groups: Dict[Tuple[str, int], List[Tuple[dict, dict]]] = {}
    for user in users:
        for addr in get_user_addresses(user):
            if not isinstance(addr, dict):
                continue
            try:
                city_id = int(addr.get("city_id") or addr.get("cityId") or 0)
            except (TypeError, ValueError):
                city_id = 0
            try:
                otg_id = int(addr.get("otg_id") or addr.get("otgId") or 0)
            except (TypeError, ValueError):
                otg_id = 0
            if city_id:
                gkey: Tuple[str, int] = ("c", city_id)
            elif otg_id:
                gkey = ("o", otg_id)
            else:
                continue
            groups.setdefault(gkey, []).append((user, addr))

    log.info("users=%s locations=%s horizon_days=%s", len(users), len(groups), GPV_HORIZON_DAYS)

    http = httpx.Client(
        timeout=30,
        headers={
            "Accept": "application/ld+json",
            "User-Agent": "ztoe-gpv-monitor/1.0",
        },
    )
    pending: Dict[int, List[Decision]] = {}
    user_by_tid: Dict[int, dict] = {}
    stats: Dict[str, int] = {}
    gpv_records = 0
    fetch_fail = 0
    sched_cache: Dict[str, Optional[dict]] = {}

    try:
        for (kind, loc_id), pairs in groups.items():
            if kind == "c":
                outages, ok = fetch_accidents(http, city_id=loc_id)
            else:
                outages, ok = fetch_accidents(http, otg_id=loc_id)
            if not ok:
                fetch_fail += 1
                continue
            gpv = [o for o in outages if is_gpv_record(o)]
            gpv_records += len(gpv)
            log.info(
                "%s=%s accidents=%s gpv=%s addrs=%s",
                "city" if kind == "c" else "otg",
                loc_id,
                len(outages),
                len(gpv),
                len(pairs),
            )
            seen_addr = set()
            for user, addr in pairs:
                tid = user.get("telegram_id")
                if not tid:
                    continue
                tid = int(tid)
                user_by_tid[tid] = user
                house = str(addr.get("house") or addr.get("building") or "")
                street = _norm_street(addr.get("street_name") or addr.get("street") or "")
                marker = (tid, street, house, queue_label(addr), is_no_gpv_addr(addr))
                if marker in seen_addr:
                    continue
                seen_addr.add(marker)
                alerts, part = decide_address(db, addr, gpv, sched_cache=sched_cache)
                _sum_stats(stats, part)
                if not alerts:
                    continue
                already = {
                    d.get("dedup_key")
                    for d in notified_col.find(
                        {"telegram_id": tid},
                        projection={"dedup_key": 1, "_id": 0},
                    )
                    if d.get("dedup_key")
                }
                fresh = _fresh_decisions(alerts, already)
                if fresh:
                    pending.setdefault(tid, []).extend(fresh)
    finally:
        http.close()

    try:
        deleted = notified_col.delete_many(
            {"expires_at": {"$lt": datetime.now(timezone.utc)}}
        )
        log.info("user_gpv_notified cleanup deleted=%s", deleted.deleted_count)
    except Exception as e:
        log.warning("cleanup failed: %s", e)

    dry = os.getenv("GPV_DRY_RUN") == "1"
    sent = sent_quiet = sent_sound = 0
    deliveries: List[str] = []

    async def _dispatch() -> None:
        nonlocal sent, sent_quiet, sent_sound
        bot = None
        if not dry:
            from telegram import Bot
            from telegram.request import HTTPXRequest
            request = HTTPXRequest(
                connection_pool_size=8,
                pool_timeout=10.0,
                connect_timeout=10.0,
                read_timeout=20.0,
                write_timeout=20.0,
            )
            bot = Bot(token=bot_token, request=request)

        async def _send_parts(tid: int, texts: List[str], silent: bool) -> bool:
            assert bot is not None
            for i, text in enumerate(texts):
                try:
                    await bot.send_message(
                        chat_id=tid,
                        text=text if len(text) <= TG_TEXT_LIMIT else text[:TG_TEXT_LIMIT],
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        disable_notification=silent,
                    )
                except Exception as e:
                    log.error("send fail %s part %s/%s: %s", tid, i + 1, len(texts), e)
                    return False
                if i + 1 < len(texts):
                    await asyncio.sleep(0.25)
            return True

        try:
            for tid, decisions in pending.items():
                user = user_by_tid.get(tid) or {}
                silent = is_quiet_now(user)
                packets = build_user_messages(decisions)
                ok_any = False
                failed = False
                for texts, items in packets:
                    if dry:
                        ok_any = True
                        log.info("dry-run tid=%s parts=%s kind=%s", tid, len(texts), items[0].kind)
                        continue
                    if await _send_parts(tid, texts, silent):
                        ok_any = True
                        remember_notified(notified_col, tid, items)
                    else:
                        failed = True
                n_mis = sum(len(d.windows) for d in decisions if d.kind == "mismatch")
                n_app = sum(len(d.windows) for d in decisions if d.kind == "appeared")
                uname = (user.get("username") or user.get("user_name") or "").strip().lstrip("@")
                who = f"{tid}" + (f" @{uname}" if uname else "")
                mode = "тихо" if silent else "зі звуком"
                if ok_any and not failed:
                    deliveries.append(f"{who} — розбіжність {n_mis}, нове ГПВ {n_app}, {mode}")
                    sent += 1
                    if silent:
                        sent_quiet += 1
                    else:
                        sent_sound += 1
                elif ok_any and failed:
                    deliveries.append(f"{who} — частково (розбіжність {n_mis}, нове ГПВ {n_app})")
                    sent += 1
                elif decisions:
                    deliveries.append(f"{who} — не дійшло (розбіжність {n_mis}, нове ГПВ {n_app})")
                if ok_any and not dry:
                    await asyncio.sleep(0.35)

            summary = _human_summary(
                users=len(users),
                locations=len(groups),
                fetch_fail=fetch_fail,
                gpv_records=gpv_records,
                stats=stats,
                sent=sent,
                sent_sound=sent_sound,
                sent_quiet=sent_quiet,
                dry=dry,
                deliveries=deliveries,
            )
            log.info(
                "підсумок users=%s locations=%s gpv=%s match=%s mismatch=%s appeared=%s sent=%s",
                len(users),
                len(groups),
                gpv_records,
                stats.get("match_windows", 0),
                stats.get("mismatch_windows", 0),
                stats.get("appeared_windows", 0),
                sent,
            )
            log_chat = (
                os.getenv("TELEGRAM_LOG_CHANNEL_ID")
                or os.getenv("ADMIN_CHAT_ID")
                or os.getenv("TELEGRAM_CHAT_ID")
            )
            if log_chat and bot is not None and not dry:
                thread = GPV_LOG_THREAD_ALERT_ID if sent else GPV_LOG_THREAD_ID
                try:
                    await bot.send_message(
                        chat_id=int(log_chat),
                        text=summary if len(summary) <= TG_TEXT_LIMIT else summary[:TG_TEXT_LIMIT],
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        message_thread_id=thread,
                    )
                except Exception as e:
                    log.warning("log channel fail thread=%s: %s", thread, e)
            elif dry:
                log.info("dry summary:\n%s", re.sub("<[^>]+>", "", summary))
        finally:
            if bot is not None:
                await bot.shutdown()

    asyncio.run(_dispatch())
    return 0


def _self_check() -> None:
    """Без мережі й Mongo. Запуск: python ztoe_gpv_monitor.py --self-check"""
    assert wall_naive("2026-11-26T13:00:29+00:00") == datetime(2026, 11, 26, 13, 0)
    assert wall_naive("2026-11-26T13:00:29Z") == datetime(2026, 11, 26, 13, 0)
    assert wall_naive("2026-11-26 16:00:01+00:00") == datetime(2026, 11, 26, 16, 0)
    # опівніч наступного дня → 24:00 цього, а не зсув +2
    overnight = windows_from_interval(
        "2026-11-26T22:00:10+00:00",
        "2026-11-27T01:00:40+00:00",
    )
    assert overnight == [("2026-11-26", "22:00", "24:00"), ("2026-11-27", "00:00", "01:00")], overnight

    slots = [("13:00", "16:00"), ("16:00", "20:00")]
    assert window_inside("13:00", "16:00", slots)
    assert window_inside("13:00", "20:00", slots)  # об’єднання, не рівність списків
    assert window_inside("16:00", "18:00", [("08:00", "12:00"), ("13:00", "20:00")])
    assert not window_inside("12:30", "16:00", [("13:00", "20:00")])
    assert not window_inside("13:00", "16:00", [])
    assert not window_inside("15:00", "18:00", [("13:00", "16:00")])

    # секунди не роблять розбіжність
    wins = windows_from_interval("2026-11-26T13:00:29+00:00", "2026-11-26T16:00:01+00:00")
    assert wins == [("2026-11-26", "13:00", "16:00")], wins
    assert window_inside(wins[0][1], wins[0][2], [("13:00", "20:00")])

    doc_today = {
        "date": "2026-11-26",
        "dates_in_source": ["2026-11-26", "27.11.2026"],
        "slots": [{"from": "13:00", "to": "20:00"}],
        "slots_tomorrow": [],
    }
    idx, got = schedule_day_slots(doc_today, "2026-11-26")
    assert idx == 0 and got == [("13:00", "20:00")], (idx, got)
    idx, got = schedule_day_slots(doc_today, "2026-11-27")
    assert idx == 1 and got == [], (idx, got)  # день відомий, слотів немає → розбіжність
    idx, _got = schedule_day_slots(doc_today, "2026-11-28")
    assert idx == -1

    doc_no_dates = {"date": "2026-11-26", "slots": [{"from": "08:00", "to": "12:00"}], "slots_tomorrow": []}
    assert schedule_day_slots(doc_no_dates, "2026-11-26")[0] == 0
    assert schedule_day_slots(doc_no_dates, "2026-11-27")[0] == -1  # завтра не доведене

    assert _parse_house("033") == ("33", "")
    assert _parse_house("33-А") == ("33", "а")
    assert _parse_house("33 а") == ("33", "а")
    gpv = {
        "switchSubReason": 2,
        "street": {"id": 5, "name": "СЕРГІЯ КЕМСЬКОГО"},
        "buildingNames": "1, 033, 15",
        "dateEvent": "2026-11-26T13:00:29+00:00",
        "datePlanIn": "2026-11-26T16:00:00+00:00",
    }
    addr = {
        "city_id": 430,
        "city_name": "Коростень",
        "street_id": 5,
        "street_name": "вул. Сергія Кемського",
        "house": "33",
        "queue": "3",
        "subqueue": "1",
    }
    assert matches_street(gpv, 5, addr["street_name"])
    assert matches_building(gpv, "33")
    assert matches_building(gpv, "033")
    assert not matches_building(gpv, "330")
    assert not matches_building({"buildingNames": ""}, "33")
    assert queue_label(addr) == "3.1"
    assert not is_no_gpv_addr(addr)
    assert not is_no_gpv_addr({"queue": "", "subqueue": ""})
    assert is_no_gpv_addr({"no_gpv": True, "queue": ""})
    assert is_no_gpv_addr({"queue": "не вимикають, крім аварійних"})
    assert queue_label({"queue": "не вимикають, крім аварійних"}) == ""

    other = dict(gpv)
    other["buildingNames"] = "12, 14"
    other["dateEvent"] = "2026-11-26T16:00:00+00:00"
    other["datePlanIn"] = "2026-11-26T20:00:00+00:00"
    # 33 не чіпає чужі будинки тієї ж вулиці
    assert collect_addr_windows([other], addr) == []

    covered = {
        "date": "2026-11-26",
        "dates_in_source": ["2026-11-26"],
        "slots": [
            {"from": "13:00", "to": "16:00"},
            {"from": "16:00", "to": "20:00"},
        ],
    }
    alerts, st = decide_address(
        None, addr, [gpv],
        sched_cache={"3.1": covered},
        today=datetime(2026, 11, 26, 9, 0),
    )
    assert alerts == [], alerts
    assert st["match_windows"] == 1

    short = dict(covered)
    short["slots"] = [{"from": "18:00", "to": "21:00"}]
    alerts, st = decide_address(
        None, addr, [gpv],
        sched_cache={"3.1": short},
        today=datetime(2026, 11, 26, 9, 0),
    )
    assert len(alerts) == 1 and alerts[0].kind == "mismatch", alerts
    assert alerts[0].windows == [("2026-11-26", "13:00", "16:00")]

    empty_day = dict(covered)
    empty_day["slots"] = []
    alerts, _st = decide_address(
        None, addr, [gpv],
        sched_cache={"3.1": empty_day},
        today=datetime(2026, 11, 26, 9, 0),
    )
    assert alerts and alerts[0].kind == "mismatch" and alerts[0].slots == []

    # немає документа черги — тиша, не «додали ГПВ»
    alerts, st = decide_address(
        None, addr, [gpv],
        sched_cache={"3.1": None},
        today=datetime(2026, 11, 26, 9, 0),
    )
    assert alerts == [] and st["skip_no_sched"] == 1

    # порожня черга — теж тиша
    bare = dict(addr)
    bare["queue"] = ""
    bare["subqueue"] = ""
    alerts, st = decide_address(
        None, bare, [gpv], today=datetime(2026, 11, 26, 9, 0),
    )
    assert alerts == [] and st["skip_no_queue"] == 1

    # no_gpv — сповіщення з годинами API
    quiet = dict(addr)
    quiet["no_gpv"] = True
    quiet["queue"] = "не вимикають, крім аварійних"
    quiet["subqueue"] = ""
    alerts, st = decide_address(
        None, quiet, [gpv], today=datetime(2026, 11, 26, 9, 0),
    )
    assert len(alerts) == 1 and alerts[0].kind == "appeared"
    assert "13:00-16:00" in format_decision(alerts[0])
    assert st["appeared_windows"] == 1

    # післязавтра для no_gpv теж поза горизонтом (default 1)
    far = dict(gpv)
    far["dateEvent"] = "2026-11-28T13:00:00+00:00"
    far["datePlanIn"] = "2026-11-28T16:00:00+00:00"
    alerts, st = decide_address(
        None, quiet, [far], today=datetime(2026, 11, 26, 9, 0),
    )
    assert alerts == [] and st["skip_horizon"] == 1

    # ГАВ не є ГПВ, навіть якщо в коментарі згадка
    assert not is_gpv_record({"switchSubReason": 1, "koment": "ГПВ щось"})
    assert is_gpv_record({"switchSubReason": 2, "koment": ""})
    assert is_gpv_record({"switchSubReason": 0, "koment": "Графік ГПВ"})
    assert not is_gpv_record({"switchSubReason": 0, "typeOff": 1, "koment": "аварія"})

    key = dedup_key("mismatch", addr, ("2026-11-26", "13:00", "16:00"))
    assert key.startswith("mismatch|430|")
    assert "сергія кемського" in key
    sample = Decision("mismatch", addr, "2026-11-26", [("2026-11-26", "13:00", "16:00")], [], "3.1")
    assert _fresh_decisions([sample], {key}) == []
    again = _fresh_decisions([sample], set())
    assert len(again) == 1 and again[0].windows == sample.windows
    print("self-check ok")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ГПВ ЖОЕ проти графіка черги")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        _self_check()
        sys.exit(0)
    sys.exit(main())
