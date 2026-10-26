"""
Спільна розсилка підписникам бота ztoe:
  - текст + один скрін ЖОЕ (site screenshot)
  - у тексті ЛИШЕ черги користувача + адреса + розбивка погодинних/аварійних

Використовується з monitor.py (зміна графіків) і ukrenergo2_monitor.py (зміна статусу).
PNG по чергах більше НЕ генеруються і НЕ відправляються.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from mongo_store import get_db, get_site_screenshot
from telegram_handler import send_photos_sync

try:
    from log_utils import log_to_buffer
except Exception:  # pragma: no cover

    def log_to_buffer(msg: str) -> None:
        print(msg)


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
    """Чи адреса користувача належить до черги 4.2 / queue=4 subqueue=2."""
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
    """Коротка адреса для повідомлення: Купеч, вул. ЦЕНТРАЛЬНА, буд. 120"""
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
        # якщо вже є «вул.» — не дублюємо
        if not re.match(r"^(вул\.?|пров\.?|пр\.?|бульвар|площа)\s", street, re.I):
            street = f"вул. {street}"
        parts.append(street)
    if house:
        parts.append(f"буд. {house}")
    return ", ".join(parts) if parts else "—"


def collect_notify_targets(
    queue_keys: Optional[Sequence[str]] = None,
) -> Dict[int, List[Tuple[str, dict]]]:
    """
    telegram_id → список (queue_label, addr_dict).
    Якщо queue_keys=None — усі черги з адрес користувача.
    """
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

    for u in users:
        tid = u.get("telegram_id")
        if tid is None:
            continue
        matched: List[Tuple[str, dict]] = []
        for addr in user_addresses(u):
            if not isinstance(addr, dict):
                continue
            q = (addr.get("queue") or "").strip()
            sq = (addr.get("subqueue") or "").strip()
            if not q:
                continue
            label = queue_label(q, sq)
            if keys:
                for qk in keys:
                    if addr_matches_queue(addr, qk):
                        ch, pid = _queue_parts(qk)
                        lab = f"{ch}.{pid}" if pid else ch
                        if not any(m[0] == lab for m in matched):
                            matched.append((lab, addr))
            else:
                if label and not any(m[0] == label for m in matched):
                    matched.append((label, addr))
        if matched:
            targets[int(tid)] = matched

    return targets


def _parse_time(t: str) -> Optional[int]:
    """'12:00' / '12:00:00' → хвилини від опівночі."""
    t = (t or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})", t)
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))


def _duration_hours(start: str, end: str) -> float:
    a = _parse_time(start)
    b = _parse_time(end)
    if a is None or b is None:
        return 0.0
    if b <= a:
        b += 24 * 60
    return round((b - a) / 60.0, 1)


def _fmt_hours(h: float) -> str:
    if h == int(h):
        return str(int(h))
    return f"{h:.1f}".rstrip("0").rstrip(".")


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


def _slots_from_doc(doc: dict | None) -> List[Tuple[str, str]]:
    """Список (from, to) з queue_schedules."""
    if not doc:
        return []
    slots = doc.get("slots") or doc.get("events") or []
    out = []
    for s in slots:
        if not isinstance(s, dict):
            continue
        fr = (s.get("from") or s.get("start") or "").strip()
        to = (s.get("to") or s.get("end") or "").strip()
        if fr and to:
            out.append((fr, to))
    return out


def _emergency_slots_for_addr(addr: dict, date_str: str) -> List[Tuple[str, str]]:
    """
    Аварійні інтервали на дату для адреси з колекції outages.
    date_str: DD.MM.YYYY
    Повертає список (HH:MM, HH:MM).
    """
    try:
        db = get_db()
    except Exception:
        return []

    city = (addr.get("city_name") or addr.get("city") or "").strip()
    street = (addr.get("street_name") or addr.get("street") or "").strip()
    house = (addr.get("house") or addr.get("building") or "").strip()

    if not city and not street:
        return []

    # date_str → YYYY-MM-DD для порівняння
    try:
        d = datetime.strptime(date_str, "%d.%m.%Y")
        ymd = d.strftime("%Y-%m-%d")
    except ValueError:
        ymd = date_str

    query = {"typeOff": 1}  # аварійне
    # м'який пошук по місту/вулиці
    cursor = db["outages"].find(query).limit(200)
    result = []
    for doc in cursor:
        dc = (doc.get("city_name") or "").strip()
        ds = (doc.get("street_name") or "").strip()
        if city and city.lower() not in dc.lower() and dc.lower() not in city.lower():
            continue
        if street:
            sn = street.lower().replace("вул.", "").replace("вул ", "").strip()
            dn = ds.lower().replace("вул.", "").replace("вул ", "").strip()
            if sn and dn and sn not in dn and dn not in sn:
                continue
        # дата
        de = (doc.get("dateEvent") or doc.get("date_from") or "")[:10]
        dp = (doc.get("datePlanIn") or doc.get("date_to") or "")[:10]
        if de and de[:10] != ymd and (not dp or dp[:10] != ymd):
            # якщо дата не збігається — пропускаємо
            if ymd not in (de, dp):
                continue
        # будинок (якщо вказано)
        buildings = (doc.get("buildingNames") or doc.get("buildings") or "").strip()
        if house and buildings:
            tokens = {p.strip().lower() for p in buildings.replace(";", ",").split(",") if p.strip()}
            h = house.lower().lstrip("0") or "0"
            if h not in tokens and not any(t.lstrip("0") == h for t in tokens):
                continue

        # інтервал
        start_raw = (doc.get("dateEvent") or "")[11:16] or "00:00"
        end_raw = (doc.get("datePlanIn") or "")[11:16] or ""
        if start_raw and end_raw:
            result.append((start_raw, end_raw))
    return result


def build_personal_dm(
    queue_labels_with_addr: Sequence[Tuple[str, dict]],
    *,
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """
    Новий короткий формат для DM.

    changes: { "6.1": [ {"change": "added"|"removed", "start": "12:00", "end": "13:00"}, ... ] }
    """
    if not date_str:
        date_str = datetime.now().strftime("%d.%m.%Y")

    changes = changes or {}

    # --- рядок змін ---
    change_bits = []
    for lab, _ in queue_labels_with_addr:
        for c in changes.get(lab, []):
            action = "🪫 додали відключення" if c.get("change") == "added" else "🔋 скасували відключення"
            start = c.get("start") or ""
            end = c.get("end") or ""
            change_bits.append(f"{lab} {action} ▶️ {start}-{end}")

    if change_bits:
        header = "🔔 Для черг " + "; ".join(change_bits)
    else:
        labs = ", ".join(lab for lab, _ in queue_labels_with_addr)
        header = f"🔔 Оновлення графіка для черг {labs}"

    # беремо першу адресу (зазвичай одна черга на повідомлення)
    addr = queue_labels_with_addr[0][1] if queue_labels_with_addr else {}
    addr_str = format_address(addr)

    lines = [
        header,
        "____",
        f"Графік на {date_str} за адресою → {addr_str}",
        "⬇️⬇️⬇️",
        "",
    ]

    # --- погодинні ---
    scheduled: List[Tuple[str, str]] = []
    for lab, _ in queue_labels_with_addr:
        doc = load_queue_doc(lab)
        scheduled.extend(_slots_from_doc(doc))

    # унікальні + сортування
    seen = set()
    scheduled_unique = []
    for fr, to in sorted(scheduled, key=lambda x: x[0]):
        key = (fr, to)
        if key not in seen:
            seen.add(key)
            scheduled_unique.append((fr, to))

    # --- аварійні ---
    emergency = _emergency_slots_for_addr(addr, date_str)

    # загальний час без світла (просте об'єднання інтервалів)
    all_intervals = scheduled_unique + emergency
    total_h = sum(_duration_hours(a, b) for a, b in all_intervals)

    if all_intervals:
        # межі від найранішого до найпізнішого (спрощено)
        starts = [a for a, _ in all_intervals]
        ends = [b for _, b in all_intervals]
        overall_start = min(starts)
        overall_end = max(ends)
        lines.append(
            f"Світла не буде ({_fmt_hours(total_h)} год): {overall_start}–{overall_end}"
        )
        lines.append("")
    else:
        lines.append("Світла не буде: немає відключень")
        lines.append("")

    if scheduled_unique:
        sch_parts = [f"{a}–{b}" for a, b in scheduled_unique]
        sch_h = sum(_duration_hours(a, b) for a, b in scheduled_unique)
        lines.append(f"⏰ Погодинні: {', '.join(sch_parts)} ({_fmt_hours(sch_h)} год)")
    else:
        lines.append("⏰ Погодинні: немає")

    if emergency:
        em_parts = [f"{a}–{b}" for a, b in emergency]
        em_h = sum(_duration_hours(a, b) for a, b in emergency)
        lines.append(f"🔴 Аварійні: {', '.join(em_parts)} ({_fmt_hours(em_h)} год)")
    else:
        lines.append("🔴 Аварійні: немає")

    return "\n".join(lines).strip()


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
    DM підписникам: новий короткий текст + один скрін ЖОЕ.

    changes — деталі diff з monitor.py:
      { "6.1": [ {"change": "added", "start": "12:00", "end": "13:00"}, ... ] }
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
        log_to_buffer("ℹ️ Немає підписників бота для DM")
        return

    if not date_str:
        date_str = datetime.now().strftime("%d.%m.%Y")

    site_png = get_site_screenshot()
    images = [site_png] if site_png else []

    sent = 0
    failed = 0
    for tid, qs_with_addr in targets.items():
        if personalize:
            body = build_personal_dm(
                qs_with_addr,
                changes=changes,
                date_str=date_str,
            )
        else:
            labs = [lab for lab, _ in qs_with_addr]
            body = (message or header or "🔔 Оновлення") + f"\n\n📌 Твої черги: <b>{', '.join(labs)}</b>"

        caption = body if len(body) <= 1024 else (body[:1000] + "\n…")

        ok = send_photos_sync(tid, images, caption, bot_token=bot_token)
        if ok:
            sent += 1
            if len(body) > 1024:
                try:
                    send_photos_sync(tid, [], body[:4096], bot_token=bot_token)
                except Exception:
                    pass
        else:
            failed += 1
        time.sleep(0.05)

    log_to_buffer(
        f"📬 DM бота: sent={sent} failed={failed} users={len(targets)}"
    )
