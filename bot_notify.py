"""
Спільна розсилка підписникам бота ztoe:
  - 1 адреса → короткий формат
  - кілька адрес/черг → одне зведене повідомлення + 1 скрін

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


def queue_label(queue: str, subqueue: str = "") -> str:
    q = (queue or "").strip()
    sq = (subqueue or "").strip()
    if not q:
        return ""
    if "." in q or "|" in q:
        return q.replace("|", ".")
    return f"{q}.{sq}" if sq else q


def format_address(addr: dict) -> str:
    if not isinstance(addr, dict):
        return "—"
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
        user_labs: List[str] = []
        for addr in user_addresses(u):
            if not isinstance(addr, dict):
                continue
            q = (addr.get("queue") or "").strip()
            sq = (addr.get("subqueue") or "").strip()
            if not q:
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
        if matched:
            try:
                targets[int(tid)] = matched
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
    """day=0 → slots (сьогодні), day=1 → slots_tomorrow."""
    if not doc:
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
    """0 = сьогодні, 1 = завтра, інакше 0 (fallback на сьогоднішні слоти)."""
    ymd = _ymd(date_str)
    if not ymd:
        return 0
    try:
        d = datetime.strptime(ymd, "%Y-%m-%d").date()
    except ValueError:
        return 0
    today = datetime.now().date()
    if d == today:
        return 0
    if d == today + timedelta(days=1):
        return 1
    return 0


def _dates_from_changes(
    changes: Optional[Dict[str, List[dict]]],
    fallback: str,
) -> List[str]:
    """Унікальні дати з diff (порядок збереження), у форматі YYYY-MM-DD."""
    seen = []
    for items in (changes or {}).values():
        for c in items or []:
            d = _ymd(c.get("date") or "")
            if d and d not in seen:
                seen.append(d)
    if not seen:
        seen.append(_ymd(fallback) or datetime.now().strftime("%Y-%m-%d"))
    return seen


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
    """Рядок деталей; час жирний+підкреслений."""
    if not slots:
        return f"{prefix}: немає"
    parts = [f"<b><u>{a}–{b}</u></b>" for a, b in slots]
    h = _total_minutes(slots)
    return f"{prefix}: {', '.join(parts)} ({_fmt_duration(h)})"


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

    if all_merged:
        ranges = ", ".join(f"<b><u>{a}–{b}</u></b>" for a, b in all_merged)
        summary = (
            f"Світла не буде ({_fmt_duration(_total_minutes(all_merged))}): "
            f"{ranges}"
        )
    else:
        summary = "Світла не буде: немає відключень"

    detail_lines = [
        summary,
        "⬇️⬇️⬇️",
        "Деталі:",
        _format_slots_line("⏰ Погодинні", scheduled),
        _format_slots_line("🔴 Аварійні", emerg),
        _format_slots_line("🟠 Ремонтні", repair),
    ]
    if other:
        detail_lines.append(_format_slots_line("⚪ Інші", other))

    body = "\n".join(detail_lines)
    return (
        f"{addr_str} (<b>Черга {lab_esc}</b>)\n"
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


def build_changes_text(
    queue_labels_with_addr: Sequence[Tuple[str, dict]],
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """
    Блок «що змінилось» — під скрін.
    Групує по датах з diff (не лише «сьогодні»).
    """
    changes = changes or {}
    labs_order: List[str] = []
    for lab, _ in queue_labels_with_addr:
        if lab not in labs_order:
            labs_order.append(lab)

    dates = _dates_from_changes(changes, date_str)
    lines = [
        f"Для черг {', '.join(_html_esc(x) for x in labs_order)} "
        f"🔔 ОНОВЛЕННЯ ГРАФІКА ВІДКЛЮЧЕНЬ!",
        "",
    ]

    for d in dates:
        lines.append(f"📅 {_html_esc(_fmt_date_display(d))}")
        lines.append("")
        for lab in labs_order:
            items = [
                c
                for c in (changes.get(lab) or [])
                if not c.get("date") or _ymd(c.get("date") or "") == d
            ]
            # якщо в diff дата порожня — показуємо всі items лише на першій даті
            if not items and d == dates[0]:
                items = [
                    c
                    for c in (changes.get(lab) or [])
                    if not c.get("date")
                ]
            lines.append(f"▶️ Черга <b>{_html_esc(lab)}</b>:")
            if not items:
                lines.append("оновлення графіка")
            else:
                for c in items:
                    lines.append(_change_line(c))
            lines.append("")
    return "\n".join(lines).strip()


def build_schedule_text(
    queue_labels_with_addr: Sequence[Tuple[str, dict]],
    *,
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """
    Наступне повідомлення: дата → адреси «Світла не буде…»,
    потім знову дата → знову адреси (якщо кілька днів).
    """
    dates = _dates_from_changes(changes, date_str)
    items = list(queue_labels_with_addr)
    if not items:
        return ""

    parts: List[str] = []
    for d in dates:
        parts.append(f"📅 <b>{_html_esc(_fmt_date_display(d))}</b>")
        parts.append("")
        for lab, addr in items:
            parts.append("—")
            parts.append(_schedule_block(lab, addr, d))
            parts.append("")
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
    Формат DM:
      1) скрін + caption = блок змін (як у каналі під фото)
         якщо caption > 1024 → фото з 📸, далі окремо текст змін
      2) наступне повідомлення: 📅 дата + «Світла не буде» по адресах
         (якщо кілька дат — дата, блоки, знов дата, блоки)
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
        else:
            changes_text = (
                (message or header or "🔔 Оновлення")
                + f"\n\n📌 Твої черги: <b>{', '.join(_html_esc(x) for x in labs)}</b>"
            )
            schedule_text = ""

        ok = False
        photo_ok = False

        # --- 1) скрін + блок змін ---
        if images:
            if len(changes_text) <= 1024:
                photo_ok = send_photos_sync(
                    tid, images, changes_text, bot_token=bot_token
                )
                ok = photo_ok
            else:
                # як у каналі: фото з 📸, потім текст змін
                photo_ok = send_photos_sync(tid, images, "📸", bot_token=bot_token)
                text_ok = send_photos_sync(
                    tid, [], changes_text[:4096], bot_token=bot_token
                )
                ok = photo_ok or text_ok
            if not ok:
                log_to_buffer(f"⚠️ DM photo/changes fail tid={tid}, шлю текст змін")
                ok = send_photos_sync(
                    tid, [], changes_text[:4096], bot_token=bot_token
                )
        else:
            ok = send_photos_sync(tid, [], changes_text[:4096], bot_token=bot_token)

        # --- 2) розклад по датах/адресах ---
        if schedule_text:
            sched_ok = send_photos_sync(
                tid, [], schedule_text[:4096], bot_token=bot_token
            )
            ok = ok or sched_ok

        if ok:
            sent += 1
            log_to_buffer(
                f"✓ DM tid={tid} queues={labs} photo={photo_ok} "
                f"changes_len={len(changes_text)} sched_len={len(schedule_text)}"
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
