"""
Спільна розсилка підписникам бота ztoe:
  - текст + один скрін ЖОЕ (site screenshot)
  - у тексті ЛИШЕ черга цієї адреси + розбивка погодинних/аварійних

Використовується з monitor.py (зміна графіків) і ukrenergo2_monitor.py (зміна статусу).
PNG по чергах більше НЕ генеруються і НЕ відправляються.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime
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
    """Коротка адреса: Купеч, вул. ЦЕНТРАЛЬНА, буд. 120"""
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
        # прибрати ведучі нулі в номері будинку для відображення
        house_disp = house.lstrip("0") or house
        parts.append(f"буд. {house_disp}")
    return ", ".join(parts) if parts else "—"


def collect_notify_targets(
    queue_keys: Optional[Sequence[str]] = None,
) -> Dict[int, List[Tuple[str, dict]]]:
    """
    telegram_id → список (queue_label, addr_dict).
    Кожна адреса окремо (не зливаємо черги).
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
                        # одна адреса = один запис (не дублюємо)
                        if not any(m[1] is addr or (
                            m[0] == lab
                            and format_address(m[1]) == format_address(addr)
                        ) for m in matched):
                            matched.append((lab, addr))
                        break
            else:
                if label and not any(
                    m[0] == label and format_address(m[1]) == format_address(addr)
                    for m in matched
                ):
                    matched.append((label, addr))
        if matched:
            targets[int(tid)] = matched

    return targets


# ---------- час / інтервали ----------

def _parse_time(t: str) -> Optional[int]:
    """'12:00' / '12:00:00' / '24:00' → хвилини від опівночі."""
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
    """Злити перекриття/сусідні інтервали, прибрати дублікати."""
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
        if a <= merged[-1][1]:  # overlap or touch
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return [(_minutes_to_str(a), _minutes_to_str(b)) for a, b in merged]


def _total_hours(intervals: List[Tuple[str, str]]) -> float:
    """Сумарні години після merge (без подвійного рахунку)."""
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
    h = total_min / 60.0
    return round(h, 1) if h != int(h) else float(int(h))


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
    return _merge_intervals(out)


def _norm_street(name: str) -> str:
    s = (name or "").strip().lower()
    for pfx in (
        "вул.", "вул ", "пров.", "провулок ", "проспект ", "пр.",
        "бульвар ", "пл.", "площа ", "вулиця ",
    ):
        if s.startswith(pfx):
            s = s[len(pfx):].strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _date_on_day(iso_or_str: str, ymd: str) -> bool:
    """Чи дата (dateEvent / datePlanIn) припадає на ymd (YYYY-MM-DD)."""
    s = (iso_or_str or "").strip()
    if not s:
        return False
    # 2026-10-26T13:00:00 або 2026-10-26
    day = s[:10]
    return day == ymd


def _emergency_slots_for_addr(addr: dict, date_str: str) -> List[Tuple[str, str]]:
    """
    Унікальні аварійні інтервали на дату для КОНКРЕТНОЇ адреси.
    Джерело: Mongo outages (typeOff=1).
    """
    try:
        db = get_db()
    except Exception:
        return []

    city = (addr.get("city_name") or addr.get("city") or "").strip()
    street = (addr.get("street_name") or addr.get("street") or "").strip()
    house = (addr.get("house") or addr.get("building") or "").strip()
    street_id = addr.get("street_id") or addr.get("streetId")

    if not city and not street and not street_id:
        return []

    try:
        d = datetime.strptime(date_str, "%d.%m.%Y")
        ymd = d.strftime("%Y-%m-%d")
    except ValueError:
        ymd = date_str

    # тільки аварійні
    cursor = db["outages"].find({"typeOff": 1}).limit(500)
    raw: List[Tuple[str, str]] = []
    sn = _norm_street(street)

    for doc in cursor:
        # --- дата: обов'язково сьогодні ---
        de = doc.get("dateEvent") or doc.get("date_from") or ""
        dp = doc.get("datePlanIn") or doc.get("date_to") or ""
        if not (_date_on_day(de, ymd) or _date_on_day(dp, ymd)):
            continue

        # --- вулиця ---
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

        # --- місто (м'яко) ---
        if city and dc:
            if city.lower() not in dc.lower() and dc.lower() not in city.lower():
                continue

        # --- будинок ---
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
            # немає списку будинків — пропускаємо (занадто широко)
            continue

        # інтервал з часу
        start_raw = ""
        end_raw = ""
        if isinstance(de, str) and len(de) >= 16:
            start_raw = de[11:16]
        if isinstance(dp, str) and len(dp) >= 16:
            end_raw = dp[11:16]
        if start_raw and end_raw:
            raw.append((start_raw, end_raw))

    return _merge_intervals(raw)


def build_personal_dm_for_one(
    lab: str,
    addr: dict,
    *,
    changes: Optional[Dict[str, List[dict]]] = None,
    date_str: str = "",
) -> str:
    """
    Одне повідомлення на ОДНУ чергу + ОДНУ адресу.
    """
    if not date_str:
        date_str = datetime.now().strftime("%d.%m.%Y")
    changes = changes or {}

    # --- рядок змін лише по цій черзі ---
    change_bits = []
    for c in changes.get(lab, []):
        action = (
            "🪫 додали відключення"
            if c.get("change") == "added"
            else "🔋 скасували відключення"
        )
        start = c.get("start") or ""
        end = c.get("end") or ""
        if start and end:
            change_bits.append(f"{action} ▶️ {start}-{end}")
        else:
            change_bits.append(action)

    if change_bits:
        header = f"🔔 Для черг {lab} " + "; ".join(change_bits)
    else:
        header = f"🔔 Оновлення графіка для черг {lab}"

    addr_str = format_address(addr)
    lines = [
        header,
        "____",
        f"Графік на {date_str} за адресою → {addr_str}",
        "⬇️⬇️⬇️",
        "",
    ]

    # --- погодинні (лише ця черга) ---
    scheduled = _slots_from_doc(load_queue_doc(lab))

    # --- аварійні (лише ця адреса) ---
    emergency = _emergency_slots_for_addr(addr, date_str)

    all_merged = _merge_intervals(scheduled + emergency)
    total_h = _total_hours(all_merged)

    if all_merged:
        overall_start = all_merged[0][0]
        overall_end = all_merged[-1][1]
        lines.append(
            f"Світла не буде ({_fmt_hours(total_h)} год): {overall_start}–{overall_end}"
        )
        lines.append("")
    else:
        lines.append("Світла не буде: немає відключень")
        lines.append("")

    if scheduled:
        sch_parts = [f"{a}–{b}" for a, b in scheduled]
        sch_h = _total_hours(scheduled)
        lines.append(
            f"⏰ Погодинні: {', '.join(sch_parts)} ({_fmt_hours(sch_h)} год)"
        )
    else:
        lines.append("⏰ Погодинні: немає")

    if emergency:
        em_parts = [f"{a}–{b}" for a, b in emergency]
        em_h = _total_hours(emergency)
        lines.append(
            f"🔴 Аварійні: {', '.join(em_parts)} ({_fmt_hours(em_h)} год)"
        )
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
    DM підписникам: короткий текст (окремо на кожну адресу) + один скрін ЖОЕ.

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
        # окреме повідомлення на кожну адресу/чергу
        for lab, addr in qs_with_addr:
            if personalize:
                body = build_personal_dm_for_one(
                    lab, addr, changes=changes, date_str=date_str
                )
            else:
                body = (
                    (message or header or "🔔 Оновлення")
                    + f"\n\n📌 Твоя черга: <b>{lab}</b>"
                )

            caption = body if len(body) <= 1024 else (body[:1000] + "\n…")
            ok = send_photos_sync(tid, images, caption, bot_token=bot_token)
            if ok:
                sent += 1
                if len(body) > 1024:
                    try:
                        send_photos_sync(
                            tid, [], body[:4096], bot_token=bot_token
                        )
                    except Exception:
                        pass
            else:
                failed += 1
            time.sleep(0.05)

    log_to_buffer(
        f"📬 DM бота: sent={sent} failed={failed} users={len(targets)}"
    )
