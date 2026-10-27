"""
Спільна розсилка підписникам бота ztoe:
  - 1 адреса → короткий формат
  - кілька адрес/черг → скрін + текст одним повідомленням (стиль /check)

Погодинні = queue_schedules (ГПВ API).
Power-on (outages), пріоритет для ОДНОГО інтервалу часу:
  1) Погодинні (вже в графіку / switchSubReason=2)
  2) Аварійні  (typeOff=1 & switchSubReason=0, або switchSubReason 1/3)
  3) Ремонтні  (typeOff 0/3)
  4) Інші      (typeOff=2)
Той самий час не дублюється в двох категоріях.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from mongo_store import get_db, get_site_screenshot
from telegram_handler import send_photos_sync

def _html_esc(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


try:
    from log_utils import log_to_buffer
except Exception:  # pragma: no cover

    def log_to_buffer(msg: str) -> None:
        print(msg)


# ---------- helpers: queues / addresses ----------

def _queue_parts(queue_key: str) -> Tuple[str, str]:
    s = (queue_key or "").strip()
    if "|" in s:
        a, b = s.split("|", 1)
        return a.strip(), b.strip()
    if "." in s:
        a, b = s.split(".", 1)
        return a.strip(), b.strip()
    return s, ""


def addr_matches_queue(addr: dict, queue_key: str) -> bool:
    if not isinstance(addr, dict):
        return False
    q = (addr.get("queue") or "").strip()
    sq = (addr.get("subqueue") or "").strip()
    if not q:
        return False
    ch, pid = _queue_parts(queue_key)
    variants = {
        queue_key,
        f"{ch}.{pid}" if pid else ch,
        f"{ch}|{pid}" if pid else ch,
        ch,
    }
    if q in variants:
        if not sq or not pid:
            return True
        return sq == pid or sq == queue_key
    if sq and f"{q}.{sq}" == queue_key:
        return True
    if sq and f"{q}|{sq}" == queue_key:
        return True
    return False


def user_addresses(u: dict) -> list:
    if u.get("addresses"):
        return u["addresses"]
    if u.get("address"):
        return [u["address"]]
    return []


_NO_GPV_MARKERS = (
    "не вимикається",
    "не вимикають",
    "не відключається",
    "не відключають",
    "без відключень",
)


def _is_no_gpv_queue(text: str) -> bool:
    """Черга — не номер, а пояснення «в цей час не вимикається…»."""
    t = (text or "").strip().lower()
    if not t or re.match(r"^[\d.]+$", t):
        return False
    return any(m in t for m in _NO_GPV_MARKERS)


def queue_label(queue: str, subqueue: str = "") -> str:
    """Коротка мітка (6.1). Для спец-текстів «не вимикають» — порожньо."""
    q = (queue or "").strip()
    sq = (subqueue or "").strip()
    if not q:
        return ""
    if _is_no_gpv_queue(q) or _is_no_gpv_queue(sq):
        return ""
    if "." in q or "|" in q:
        if not re.match(r"^[\d.]+$", q.replace("|", ".")):
            return ""
        return q.replace("|", ".")
    if sq and sq != q and re.match(r"^[\d.]+$", sq):
        return f"{q}.{sq}"
    if re.match(r"^[\d.]+$", q):
        return q
    return ""


def format_address(addr: dict) -> str:
    if not isinstance(addr, dict):
        return "—"
    # Підписка лише на чергу (без вулиці) — sitemonitor все одно шле ГПВ по queue
    if addr.get("queue_only"):
        q = (addr.get("queue") or "").strip()
        sq = (addr.get("subqueue") or "").strip()
        if _is_no_gpv_queue(q) or _is_no_gpv_queue(sq):
            return "не вимикають, крім аварійних"
        if q and re.match(r"^[\d.]+$", q.replace("|", ".")):
            lab = q.replace("|", ".")
        elif q and sq and re.match(r"^[\d.]+$", sq):
            lab = f"{q}.{sq}"
        else:
            lab = q or "?"
        return f"Черга {lab}"
    city = (addr.get("city_name") or addr.get("city") or "").strip()
    street = (addr.get("street_name") or addr.get("street") or "").strip()
    house = (
        addr.get("house")
        or addr.get("building")
        or addr.get("house_number")
        or ""
    ).strip()
    parts = []
    if city:
        parts.append(city)
    if street:
        if not re.match(r"^(вул\.?|пров\.?|пр\.?|бульвар|площа)\s", street, re.I):
            street = f"вул. {street}"
        parts.append(street)
    if house:
        house_disp = house.lstrip("0") or house
        parts.append(f"буд. {house_disp}")
    return ", ".join(parts) if parts else "—"


def collect_notify_targets(
    queue_keys: Optional[Sequence[str]] = None,
) -> Dict[int, List[Tuple[str, dict]]]:
    bot_token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        log_to_buffer("⚠️ ZTOE_BOT_TOKEN / TELEGRAM_BOT_TOKEN немає — пропуск DM")
        return {}

    try:
        db = get_db()
        users = list(db["users"].find({"notify": True}))
    except Exception as e:
        log_to_buffer(f"❌ Mongo users: {e}")
        return {}

    keys = [str(k).strip() for k in (queue_keys or []) if str(k).strip()]
    targets: Dict[int, List[Tuple[str, dict]]] = {}
    all_labels: List[str] = []

    for u in users:
        tid = u.get("telegram_id")
        if tid is None:
            continue
        matched: List[Tuple[str, dict]] = []
        no_queue: List[Tuple[str, dict]] = []
        user_labs: List[str] = []
        for addr in user_addresses(u):
            if not isinstance(addr, dict):
                continue
            q = (addr.get("queue") or "").strip()
            sq = (addr.get("subqueue") or "").strip()
            if not q:
                # адреса без черги — покажемо з поміткою (не пропускаємо)
                no_queue.append(("", addr))
                continue
            label = queue_label(q, sq)
            if label:
                user_labs.append(label)
            if keys:
                for qk in keys:
                    if addr_matches_queue(addr, qk):
                        ch, pid = _queue_parts(qk)
                        lab = f"{ch}.{pid}" if pid else ch
                        if not any(
                            m[0] == lab
                            and format_address(m[1]) == format_address(addr)
                            for m in matched
                        ):
                            matched.append((lab, addr))
                        break
            else:
                if label and not any(
                    m[0] == label
                    and format_address(m[1]) == format_address(addr)
                    for m in matched
                ):
                    matched.append((label, addr))
        if user_labs:
            all_labels.append(f"tid={tid}:[{','.join(user_labs)}]")
        # якщо є збіг по чергах змін (або шлемо всім) — додаємо й адреси без черги
        if matched or (not keys and no_queue):
            combined = matched + no_queue
            # якщо фільтр по чергах і є збіг — теж підтягуємо no_queue
            if matched and no_queue:
                combined = matched + no_queue
            try:
                targets[int(tid)] = combined
            except (TypeError, ValueError):
                log_to_buffer(f"⚠️ bad telegram_id={tid!r}")

    log_to_buffer(
        f"📬 notify users={len(users)} keys={keys} targets={len(targets)}"
    )
    if keys and not targets and all_labels:
        log_to_buffer(
            "⚠️ жоден підписник не збігся з чергами змін; адреси: "
            + "; ".join(all_labels[:20])
        )
    return targets


# ---------- time / intervals ----------

def _parse_time(t: str) -> Optional[int]:
    t = (t or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})", t)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h == 24 and mi == 0:
        return 24 * 60
    return h * 60 + mi


def _minutes_to_str(m: int) -> str:
    if m >= 24 * 60:
        return "24:00"
    return f"{m // 60:02d}:{m % 60:02d}"


def _merge_intervals(intervals: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    parsed = []
    for fr, to in intervals:
        a = _parse_time(fr)
        b = _parse_time(to)
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
    return [(_minutes_to_str(a), _minutes_to_str(b)) for a, b in merged]


def _total_minutes(intervals: List[Tuple[str, str]]) -> int:
    merged = _merge_intervals(intervals)
    total_min = 0
    for fr, to in merged:
        a = _parse_time(fr)
        b = _parse_time(to)
        if a is None or b is None:
            continue
        if b <= a:
            b += 24 * 60
        total_min += b - a
    return total_min


def _fmt_duration(total_min: int) -> str:
    """2 год, 30хв або 2 год 30хв. Півгодини не округлюються."""
    if total_min < 0:
        total_min = 0
    h, m = divmod(int(total_min), 60)
    if h and m:
        return f"{h} год {m}хв"
    if h:
        return f"{h} год"
    return f"{m}хв"


def _interval_key(fr: str, to: str) -> Tuple[int, int]:
    a = _parse_time(fr) or -1
    b = _parse_time(to) or -1
    if a >= 0 and b >= 0 and b <= a:
        b += 24 * 60
    return (a, b)


def _interval_covered_by(
    slot: Tuple[str, str],
    covers: List[Tuple[str, str]],
) -> bool:
    """Повне входження slot у будь-який з covers (після merge)."""
    a, b = _interval_key(*slot)
    if a < 0 or b < 0:
        return False
    for fr, to in _merge_intervals(covers):
        ca, cb = _interval_key(fr, to)
        if ca < 0:
            continue
        if ca <= a and b <= cb:
            return True
    return False


def _same_interval(a: Tuple[str, str], b: Tuple[str, str]) -> bool:
    return _interval_key(*a) == _interval_key(*b)


# ---------- data loaders ----------

def load_queue_doc(label: str) -> dict | None:
    db = get_db()
    ch, pid = _queue_parts(label)
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


def _slots_from_doc(doc: dict | None, *, day: int = 0) -> List[Tuple[str, str]]:
    """day=0 → slots, day=1 → slots_tomorrow; day<0/>1 → немає ГПВ (далека дата)."""
    if not doc or day < 0 or day > 1:
        return []
    if day == 1:
        slots = doc.get("slots_tomorrow") or []
    else:
        slots = doc.get("slots") or doc.get("events") or []
    out = []
    for s in slots:
        if not isinstance(s, dict):
            continue
        fr = (s.get("from") or s.get("start") or "").strip()
        to = (s.get("to") or s.get("end") or "").strip()
        if fr and to:
            out.append((fr, to))
    return _merge_intervals(out)


def _fmt_date_display(date_str: str) -> str:
    """YYYY-MM-DD / DD.MM.YYYY → DD.MM.YYYY."""
    s = (date_str or "").strip()
    if not s:
        return datetime.now().strftime("%d.%m.%Y")
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return s


def _ymd(date_str: str) -> str:
    s = (date_str or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def _day_offset(date_str: str) -> int:
    """
    0 = сьогодні, 1 = завтра.
    -1 = дата поза відомим ГПВ (далекий ремонт) — погодинних не підставляти.
    """
    ymd = _ymd(date_str)
    if not ymd:
        return -1
    try:
        d = datetime.strptime(ymd, "%Y-%m-%d").date()
    except ValueError:
        return -1
    today = datetime.now().date()
    if d == today:
        return 0
    if d == today + timedelta(days=1):
        return 1
    return -1


def _dates_from_changes(
    changes: Optional[Dict[str, List[dict]]],
    fallback: str,
) -> List[str]:
    """Унікальні дати з diff, у форматі YYYY-MM-DD, за зростанням (спочатку раніша)."""
    seen = set()
    for items in (changes or {}).values():
        for c in items or []:
            d = _ymd(c.get("date") or "")
            if d:
                seen.add(d)
    if not seen:
        fb = _ymd(fallback) or datetime.now().strftime("%Y-%m-%d")
        return [fb]
    return sorted(seen)  # 27.10 перед 28.10


def _norm_street(name: str) -> str:
    s = (name or "").strip().lower()
    for pfx in (
        "вул.", "вул ", "пров.", "провулок ", "проспект ", "пр.",
        "бульвар ", "пл.", "площа ", "вулиця ",
    ):
        if s.startswith(pfx):
            s = s[len(pfx):].strip()
    return re.sub(r"\s+", " ", s)


def _date_on_day(iso_or_str: str, ymd: str) -> bool:
    s = (iso_or_str or "").strip()
    if not s:
        return False
    return s[:10] == ymd


def _classify_outage(doc: dict) -> Optional[str]:
    """
    Повертає: 'emergency' | 'repair' | 'other' | None (ігнорувати / ГПВ-дубль).

    Логіка як на сайті ztoe-poweron (JS):
      switchSubReason:
        1 → ГАВ (аварійний графік)
        2 → ГПВ (погодинні) → ігноруємо тут (є в queue_schedules)
        3 → СГАВ
      typeOff (якщо switchSubReason == 0 / немає):
        0 → планове
        1 → аварійне
        2 → оперативні / інші
        3 → планове (planeds)
    """
    koment = (doc.get("koment") or "").upper()
    # явний ГПВ у коментарі
    if "ГПВ" in koment or "ГПВ-" in koment:
        return None

    try:
        sub = int(doc.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        sub = 0
    try:
        t = int(doc.get("typeOff"))
    except (TypeError, ValueError):
        t = -1

    if sub == 2:
        return None  # графік погодинних — не дублюємо
    if sub in (1, 3):
        return "emergency"
    if sub > 0:
        # невідомий sub > 0 — обережно як other
        return "other"

    if t == 1:
        return "emergency"
    if t in (0, 3):
        return "repair"
    if t == 2:
        return "other"
    return None


def _outages_bucketed(
    addr: dict,
    date_str: str,
) -> Dict[str, List[Tuple[str, str]]]:
    """
    З Mongo outages → {'emergency': [...], 'repair': [...], 'other': [...]}
    з merge всередині категорії.
    """
    empty = {"emergency": [], "repair": [], "other": []}
    try:
        db = get_db()
    except Exception:
        return empty

    city = (addr.get("city_name") or addr.get("city") or "").strip()
    street = (addr.get("street_name") or addr.get("street") or "").strip()
    house = (addr.get("house") or addr.get("building") or "").strip()
    street_id = addr.get("street_id") or addr.get("streetId")

    if not city and not street and not street_id:
        return empty

    try:
        d = datetime.strptime(date_str, "%d.%m.%Y")
        ymd = d.strftime("%Y-%m-%d")
    except ValueError:
        ymd = date_str

    buckets: Dict[str, List[Tuple[str, str]]] = {
        "emergency": [],
        "repair": [],
        "other": [],
    }
    sn = _norm_street(street)

    cursor = db["outages"].find({}).limit(800)

    for doc in cursor:
        de = doc.get("dateEvent") or doc.get("date_from") or ""
        dp = doc.get("datePlanIn") or doc.get("date_to") or ""
        if not (_date_on_day(de, ymd) or _date_on_day(dp, ymd)):
            continue

        dc = (doc.get("city_name") or "").strip()
        ds = (doc.get("street_name") or "").strip()
        doc_street_id = doc.get("street_id")

        street_ok = False
        if street_id and doc_street_id is not None and str(street_id) == str(doc_street_id):
            street_ok = True
        else:
            dn = _norm_street(ds)
            if sn and dn and (sn == dn or sn in dn or dn in sn):
                street_ok = True
        if not street_ok:
            continue

        if city and dc:
            if city.lower() not in dc.lower() and dc.lower() not in city.lower():
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
        elif house and not buildings:
            continue

        kind = _classify_outage(doc)
        if not kind:
            continue

        start_raw = ""
        end_raw = ""
        if isinstance(de, str) and len(de) >= 16:
            start_raw = de[11:16]
        if isinstance(dp, str) and len(dp) >= 16:
            end_raw = dp[11:16]
        if not (start_raw and end_raw):
            continue

        buckets[kind].append((start_raw, end_raw))

    for k in buckets:
        buckets[k] = _merge_intervals(buckets[k])
    return buckets


def _dedupe_by_priority(
    scheduled: List[Tuple[str, str]],
    emergency: List[Tuple[str, str]],
    repair: List[Tuple[str, str]],
    other: List[Tuple[str, str]],
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]], List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Пріоритет на один інтервал часу:
      погодинні > аварійні > ремонтні > інші
    Якщо час уже зайнятий вищою категорією — нижчу прибираємо.
    """
    claimed: List[Tuple[str, str]] = list(scheduled)

    def take(slots: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        out = []
        for s in slots:
            if _interval_covered_by(s, claimed):
                continue
            # також точний збіг з уже взятим
            if any(_same_interval(s, c) for c in claimed):
                continue
            out.append(s)
            claimed.append(s)
        return _merge_intervals(out)

    em = take(emergency)
    rp = take(repair)
    ot = take(other)
    return scheduled, em, rp, ot


def _format_slots_line(prefix: str, slots: List[Tuple[str, str]]) -> str:
    """Рядок деталей; час підкреслений одним тегом (менше Telegram entities).
    Порожні слоти не показуємо (без спаму «…: немає»).
    """
    if not slots:
        return ""
    parts = [f"{a}-{b}" for a, b in slots]
    h = _total_minutes(slots)
    return f"{prefix}: <u>{', '.join(parts)}</u> ({_fmt_duration(h)})"


def _short_time(t: str) -> str:
    t = (t or "").strip()
    m = re.match(r"^0?(\d{1,2}):(\d{2})", t)
    if not m:
        return t
    return f"{int(m.group(1))}:{m.group(2)}"


def _schedule_block(lab: str, addr: dict, date_str: str) -> str:
    """
    Зовні: адреса + <b>Черга</b>.
    Згорнутий блок: Світла не буде + час <b><u>…</u></b> + Деталі.
    date_str — день, для якого брати слоти (сьогодні/завтра з Mongo).
    """
    day = _day_offset(date_str)
    scheduled = _slots_from_doc(load_queue_doc(lab), day=day)
    # outages у Mongo фільтруємо по цій даті
    buckets = _outages_bucketed(addr, _fmt_date_display(date_str))
    scheduled, emerg, repair, other = _dedupe_by_priority(
        scheduled,
        buckets.get("emergency") or [],
        buckets.get("repair") or [],
        buckets.get("other") or [],
    )

    all_merged = _merge_intervals(scheduled + emerg + repair + other)
    addr_str = _html_esc(format_address(addr))
    lab_esc = _html_esc(lab)
    q_raw = (addr.get("queue") or "").strip()
    sq_raw = (addr.get("subqueue") or "").strip()
    no_gpv = _is_no_gpv_queue(q_raw) or _is_no_gpv_queue(sq_raw) or not lab

    if addr.get("queue_only") and lab:
        head = f"<b>Черга {lab_esc}</b>"
    elif lab:
        head = f"{addr_str} (<b>Черга {lab_esc}</b>)"
    elif no_gpv and not addr.get("queue_only"):
        head = f"{addr_str} (<i>не вимикають, крім аварійних</i>)"
    elif addr.get("queue_only"):
        head = "<i>не вимикають, крім аварійних</i>"
    else:
        head = f"{addr_str} (<i>черга не визначена</i>)"

    # ГПВ є лише для day 0/1; day=-1 = далека дата без графіка
    has_gpv_data = day >= 0

    if all_merged:
        ranges = ", ".join(f"{a}-{b}" for a, b in all_merged)
        # один <u> на всі інтервали — інакше Telegram ріже entities з кінця повідомлення
        summary = (
            f"<b>🪫 Світла не буде ({_fmt_duration(_total_minutes(all_merged))}):</b> "
            f"<u>{ranges}</u>"
        )
    elif has_gpv_data:
        summary = "🔋 Світло буде увесь день"
    else:
        # ЖОЕ ще не передав ГПВ на цей день — не стверджуємо «світло весь день»
        summary = "🔄 Дані очікуємо"

    # Деталі лише якщо є аварійні / ремонтні / інші (не дублюємо чисті ГПВ)
    extra_lines = []
    if emerg:
        extra_lines.append(_format_slots_line("⚠️ Аварійні", emerg))
    if repair:
        extra_lines.append(_format_slots_line("🛠 Ремонтні", repair))
    if other:
        extra_lines.append(_format_slots_line("⚪ Інші", other))

    # Черга без адреси або лише ГПВ — підсумок у цитаті, без «Деталі»
    if addr.get("queue_only") or not extra_lines:
        return f"{head}\n<blockquote>{summary}</blockquote>"

    detail = [summary, "⬇️⬇️⬇️", "Деталі:"]
    if scheduled:
        detail.append(_format_slots_line("⏰ Погодинні", scheduled))
    detail.extend(extra_lines)
    body = "\n".join(detail)
    return (
        f"{head}\n"
        f"<blockquote expandable>{body}</blockquote>"
    )


def _change_line(c: dict) -> str:
    s = _html_esc(_short_time(c.get("start") or ""))
    e = _html_esc(_short_time(c.get("end") or ""))
    span = f"{s}-{e}" if s or e else ""
    ch = (c.get("change") or "").strip()
    if ch in ("added", "new"):
        return f"{span} 🪫 додали відключення" if span else "🪫 додали відключення / новий графік"
    if span:
        return f"<s>{span}</s> 🔋 скасували відключення"
    return "🔋 скасували відключення"


def _items_for_lab_day(
    changes: Dict[str, List[dict]],
    lab: str,
    d: str,
    dates: List[str],
) -> List[dict]:
    items = [
        c
        for c in (changes.get(lab) or [])
        if not c.get("date") or _ymd(c.get("date") or "") == d
    ]
    if not items and dates and d == dates[0]:
        items = [
            c
            for c in (changes.get(lab) or [])
            if not c.get("date")
        ]
    return items


def _is_new_schedule_only(items: List[dict]) -> bool:
    """Лише «новий графік» без конкретних інтервалів."""
    if not items:
        return False
    for c in items:
        ch = (c.get("change") or "").strip()
        if ch not in ("new", "added"):
            return False
        if (c.get("start") or "").strip() or (c.get("end") or "").strip():
            return False
    return True


def build_changes_text(
    queue_labels_with_addr: Sequence[Tuple[str, dict]],
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """
    Блок «що змінилось» — під скрін.
    Групує по датах; «новий графік» на день — одним рядком на кілька черг.
    """
    changes = changes or {}
    labs_order: List[str] = []
    for lab, _ in queue_labels_with_addr:
        if lab and lab not in labs_order:
            labs_order.append(lab)

    dates = _dates_from_changes(changes, date_str)
    labs_str = ", ".join(_html_esc(x) for x in labs_order) if labs_order else "—"
    lines = [
        f"Для черг {labs_str} 🔔 <b>ОНОВЛЕННЯ ГРАФІКА ВІДКЛЮЧЕНЬ!</b>",
        "",
    ]

    for d in dates:
        lines.append(f"📅 {_html_esc(_fmt_date_display(d))}")
        lines.append("")

        # черги, для яких на цей день лише «новий графік» без інтервалів
        new_sched_labs: List[str] = []
        detail_labs: List[Tuple[str, List[dict]]] = []
        for lab in labs_order:
            items = _items_for_lab_day(changes, lab, d, dates)
            if _is_new_schedule_only(items):
                new_sched_labs.append(lab)
            elif items:
                detail_labs.append((lab, items))
            else:
                detail_labs.append((lab, []))

        if new_sched_labs:
            labs_txt = ", ".join(_html_esc(x) for x in new_sched_labs)
            lines.append(
                f"🆕 Для черг <b>{labs_txt}</b> додали новий графік на цей день"
            )
            lines.append("")

        for lab, items in detail_labs:
            if lab in new_sched_labs:
                continue
            lines.append(f"▶️ Черга <b>{_html_esc(lab)}</b>:")
            if not items:
                lines.append("оновлення графіка")
            else:
                for c in items:
                    lines.append(_change_line(c))
            lines.append("")

    return "\n".join(lines).strip()


def _addr_head_line(lab: str, addr: dict) -> str:
    """Рядок адреси + черга для списків / груп."""
    addr_str = _html_esc(format_address(addr))
    lab_esc = _html_esc(lab)
    if addr.get("queue_only") and lab:
        return f"<b>Черга {lab_esc}</b>"
    if lab:
        return f"{addr_str} (<b>Черга {lab_esc}</b>)"
    q = (addr.get("queue") or "").strip()
    if not q:
        return (
            f"{addr_str} (<i>черга не обрана</i>)\n"
            f"<i>➡️ Передодай адресу в боті, щоб бачити графік</i>"
        )
    return addr_str


def _outage_under_header(
    lab: str,
    addr: dict,
    *,
    scheduled: List[Tuple[str, str]],
    emerg: List[Tuple[str, str]],
    repair: List[Tuple[str, str]],
    other: List[Tuple[str, str]],
    all_merged: List[Tuple[str, str]],
) -> str:
    """
    Під заголовком «Світла не буде по адресах»:
      адреса + звичайна цитата (лише ГПВ)
      або expandable з Деталями (ремонт/аварія).
    """
    head = _addr_head_line(lab, addr)
    ranges = ", ".join(f"{a}-{b}" for a, b in all_merged)
    summary = f"<u>{ranges}</u> ({_fmt_duration(_total_minutes(all_merged))})"

    extra_lines = []
    if emerg:
        extra_lines.append(_format_slots_line("⚠️ Аварійні", emerg))
    if repair:
        extra_lines.append(_format_slots_line("🛠 Ремонтні", repair))
    if other:
        extra_lines.append(_format_slots_line("⚪ Інші", other))

    if not extra_lines:
        return f"{head}\n<blockquote>{summary}</blockquote>"

    detail = [summary, "⬇️⬇️⬇️", "Деталі:"]
    if scheduled:
        detail.append(_format_slots_line("⏰ Погодинні", scheduled))
    detail.extend(extra_lines)
    return f"{head}\n<blockquote expandable>{chr(10).join(detail)}</blockquote>"


def _grouped_ok_blockquote(title: str, heads: List[str]) -> str:
    """Заголовок зовні, стрілки + список — у згортаємій цитаті."""
    body = "⬇️⬇️⬇️\n" + "\n".join(heads)
    return f"{title}\n<blockquote expandable>{body}</blockquote>"


def build_schedule_text(
    queue_labels_with_addr: Sequence[Tuple[str, dict]],
    *,
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """
    По днях:
      🪫 Світла не буде по адресах  (заголовок)
      адреса + цитата з інтервалами / деталями
      🔋 / 🔄 — згортаєма група адрес без відключень
    """
    dates = _dates_from_changes(changes, date_str)
    items = list(queue_labels_with_addr)
    if not items:
        return ""

    # адреси без черги — один блок на все повідомлення (не на кожен день)
    queued_items = [
        (lab, addr)
        for lab, addr in items
        if (lab or "").strip() or (addr.get("queue") or "").strip()
    ]
    no_queue_heads = [
        _addr_head_line(lab, addr)
        for lab, addr in items
        if not (lab or "").strip() and not (addr.get("queue") or "").strip()
    ]

    parts: List[str] = []
    for day_i, d in enumerate(dates):
        if day_i > 0:
            parts.append("")
        parts.append(f"📅 <b>{_html_esc(_fmt_date_display(d))}</b>")
        parts.append("")

        day = _day_offset(d)
        has_gpv = day >= 0
        disp = _fmt_date_display(d)
        outage_blocks: List[str] = []
        ok_or_wait: List[str] = []
        for lab, addr in queued_items:
            scheduled = _slots_from_doc(load_queue_doc(lab), day=day)
            buckets = _outages_bucketed(addr, disp)
            scheduled, emerg, repair, other = _dedupe_by_priority(
                scheduled,
                buckets.get("emergency") or [],
                buckets.get("repair") or [],
                buckets.get("other") or [],
            )
            all_merged = _merge_intervals(scheduled + emerg + repair + other)
            if all_merged:
                outage_blocks.append(
                    _outage_under_header(
                        lab,
                        addr,
                        scheduled=scheduled,
                        emerg=emerg,
                        repair=repair,
                        other=other,
                        all_merged=all_merged,
                    )
                )
            else:
                ok_or_wait.append(_addr_head_line(lab, addr))

        if outage_blocks:
            parts.append("🪫 <b>Світла не буде по адресах</b>")
            parts.append("⬇️⬇️⬇️")
            parts.append("")
            for block in outage_blocks:
                parts.append(block)
                parts.append("")
        if ok_or_wait:
            title = (
                "🔋 <b>Світло буде увесь день по адресах</b>"
                if has_gpv
                else "🔄 <b>Дані очікуємо по адресах</b>"
            )
            parts.append(_grouped_ok_blockquote(title, ok_or_wait))
            parts.append("")

    if no_queue_heads:
        parts.append("")
        parts.append(
            _grouped_ok_blockquote(
                "⚠️ <b>Адреси без черги</b>", no_queue_heads
            )
        )

    return "\n".join(parts).strip()


def build_personal_dm(
    queue_labels_with_addr: Sequence[Tuple[str, dict]],
    *,
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """Залишено для сумісності: зміни + розклад одним текстом."""
    if not date_str:
        date_str = datetime.now().strftime("%d.%m.%Y")
    a = build_changes_text(queue_labels_with_addr, changes, date_str)
    b = build_schedule_text(queue_labels_with_addr, changes=changes, date_str=date_str)
    if a and b:
        return f"{a}\n\n{b}"
    return a or b or "🔔 Оновлення графіка"



CAPTION_LIMIT = 1024
TEXT_LIMIT = 4096


def _split_header_and_days(text: str) -> tuple[str, list[str]]:
    """Шапка (зміни / статус) і блоки, що починаються з рядка 📅 — як у /check."""
    lines = (text or "").splitlines()
    first = None
    for i, line in enumerate(lines):
        if line.strip().startswith("📅"):
            first = i
            break
    if first is None:
        return (text or "").strip(), []
    header = "\n".join(lines[:first]).strip()
    days: list[str] = []
    buf: list[str] = []
    for line in lines[first:]:
        if line.strip().startswith("📅") and buf:
            days.append("\n".join(buf).strip())
            buf = [line]
        else:
            buf.append(line)
    if buf and "\n".join(buf).strip():
        days.append("\n".join(buf).strip())
    return header, days


def _text_chunks(text: str, limit: int) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    out: list[str] = []
    rest = text
    while rest:
        if len(rest) <= limit:
            out.append(rest)
            break
        cut = rest.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        piece = rest[:cut].strip()
        if piece:
            out.append(piece)
        rest = rest[cut:].strip()
    return out


def plan_dm_messages(text: str) -> tuple[str, list[str]]:
    """
    caption — підпис до фото (≤1024), follow — наступні повідомлення.

    Як у /check (handlers.plan_check_messages), з пріоритетом по днях:
      • є кілька 📅 днів → під фото шапка + ПЕРШИЙ день; решта днів окремо
        (навіть якщо все б вмістилось у 1024 — другий день не в caption)
      • один день і все ≤1024 → лише caption
      • шапка+перший день >1024 → під фото лише шапка; дні окремо
    """
    full = (text or "").strip()
    if not full:
        return "", []

    header, days = _split_header_and_days(full)

    # немає розбиття по 📅 — як раніше: все в caption, якщо влазить
    if not days:
        if len(full) <= CAPTION_LIMIT:
            return full, []
        parts = _text_chunks(full, TEXT_LIMIT)
        caption = parts[0] if len(parts[0]) <= CAPTION_LIMIT else parts[0][:CAPTION_LIMIT]
        return caption, parts[1:]

    # один день: можна все під фото, якщо ≤1024
    if len(days) == 1:
        first = f"{header}\n\n{days[0]}".strip() if header else days[0]
        if len(first) <= CAPTION_LIMIT:
            return first, []
        # не влазить — лише шапка під фото
        caption = header or "🔔 Оновлення графіка"
        if len(caption) > CAPTION_LIMIT:
            caption = caption[: CAPTION_LIMIT - 1] + "…"
        return caption, _text_chunks(days[0], TEXT_LIMIT)

    # кілька днів: під фото ТІЛЬКИ шапка + перший день; решта — окремо
    first = f"{header}\n\n{days[0]}".strip() if header else days[0]
    if len(first) <= CAPTION_LIMIT:
        caption = first
        rest_days = days[1:]
    else:
        caption = header or "🔔 Оновлення графіка"
        if len(caption) > CAPTION_LIMIT:
            caption = caption[: CAPTION_LIMIT - 1] + "…"
        rest_days = days  # перший день теж окремо

    follow: list[str] = []
    for day in rest_days:
        follow.extend(_text_chunks(day, TEXT_LIMIT))
    return caption, follow


def notify_bot_users_with_images(
    message: str = "",
    *,
    queue_keys: Optional[Sequence[str]] = None,
    all_user_queues: bool = False,
    header: str = "",
    date_str: str = "",
    personalize: bool = True,
    changes: Optional[Dict[str, List[dict]]] = None,
) -> None:
    """
    Формат DM як /check:
      фото + caption РАЗОМ (не окремий скрін).
      caption: зміни/шапка + перший день (якщо ≤1024);
      якщо не вміщається — під фото лише шапка, дні окремими повідомленнями.
      другий день — завжди окремо, якщо не вліз у caption.
    """
    bot_token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        log_to_buffer("⚠️ Немає токена бота — DM не шлемо")
        return

    if all_user_queues:
        targets = collect_notify_targets(None)
    else:
        if not queue_keys:
            log_to_buffer("ℹ️ Немає черг для DM")
            return
        targets = collect_notify_targets(queue_keys)

    if not targets:
        log_to_buffer(
            "ℹ️ Немає підписників бота для DM "
            "(перевір notify=true, queue/subqueue, ZTOE_BOT_TOKEN, /start бота)"
        )
        return

    if not date_str:
        date_str = datetime.now().strftime("%d.%m.%Y")

    site_png = get_site_screenshot()
    images = [site_png] if site_png else []
    if not images:
        log_to_buffer("⚠️ немає скріну ЖОЕ в Mongo — DM лише текстом")

    sent = 0
    failed = 0
    for tid, qs_with_addr in targets.items():
        labs = sorted({lab for lab, _ in qs_with_addr})

        if personalize:
            changes_text = build_changes_text(qs_with_addr, changes, date_str)
            schedule_text = build_schedule_text(
                qs_with_addr, changes=changes, date_str=date_str
            )
            if changes_text and schedule_text:
                full_text = f"{changes_text}\n\n{schedule_text}"
            else:
                full_text = changes_text or schedule_text or (
                    message or header or "🔔 Оновлення графіка"
                )
        else:
            full_text = (
                (message or header or "🔔 Оновлення")
                + f"\n\n📌 Твої черги: <b>{', '.join(_html_esc(x) for x in labs)}</b>"
            )

        caption, follow = plan_dm_messages(full_text)

        ok = False
        photo_ok = False

        if images:
            photo_ok = send_photos_sync(
                tid, images, caption or "🔔 Оновлення графіка", bot_token=bot_token
            )
            ok = photo_ok
            for part in follow:
                text_ok = send_photos_sync(tid, [], part[:TEXT_LIMIT], bot_token=bot_token)
                ok = ok or text_ok
            if not ok:
                log_to_buffer(f"⚠️ DM photo fail tid={tid}, шлю лише текст")
                for part in ([caption] + follow) if caption else follow:
                    if not part:
                        continue
                    ok = send_photos_sync(
                        tid, [], part[:TEXT_LIMIT], bot_token=bot_token
                    ) or ok
        else:
            pieces = [p for p in [caption, *follow] if p] or [full_text]
            for part in pieces:
                part_ok = send_photos_sync(
                    tid, [], part[:TEXT_LIMIT], bot_token=bot_token
                )
                ok = ok or part_ok

        if ok:
            sent += 1
            log_to_buffer(
                f"✓ DM tid={tid} queues={labs} photo={photo_ok} "
                f"caption_len={len(caption)} follow={len(follow)} "
                f"(фото+текст як /check)"
            )
        else:
            failed += 1
            log_to_buffer(
                f"❌ DM fail tid={tid} queues={labs} "
                f"(бот інший? користувач не /start? блок?)"
            )
        time.sleep(0.05)

    log_to_buffer(
        f"📬 DM бота: sent={sent} failed={failed} users={len(targets)}"
    )
