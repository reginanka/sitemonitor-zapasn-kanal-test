"""
Спільна розсилка підписникам бота ztoe:
  - текст під картинкою (caption)
  - кілька PNG одним повідомленням (media group)
  - у тексті ЛИШЕ черги користувача (не всі з каналу)

Використовується з monitor.py (зміна графіків) і ukrenergo2_monitor.py (зміна статусу).
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

from mongo_store import get_db, get_schedule_png
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


def collect_notify_targets(
    queue_keys: Optional[Sequence[str]] = None,
) -> Dict[int, List[str]]:
    """
    telegram_id → список queue keys (4.2).
    Якщо queue_keys=None — усі черги з адрес користувача (для зміни статусу області).
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
    targets: Dict[int, List[str]] = {}

    for u in users:
        tid = u.get("telegram_id")
        if tid is None:
            continue
        matched: List[str] = []
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
                        if lab not in matched:
                            matched.append(lab)
            else:
                if label and label not in matched:
                    matched.append(label)
        if matched:
            targets[int(tid)] = matched

    return targets


def pngs_for_queues(queue_labels: Sequence[str]) -> List[bytes]:
    """PNG з GridFS у тому ж порядку, що queue_labels (пропуск якщо немає)."""
    images: List[bytes] = []
    for lab in queue_labels:
        ch, pid = _queue_parts(lab)
        png = get_schedule_png(ch, pid)
        if not png and pid:
            png = get_schedule_png(f"{ch}.{pid}", "")
        if not png and pid:
            png = get_schedule_png(f"{ch}|{pid}", "")
        if png:
            images.append(png)
    return images


def _slots_line_from_doc(doc: dict | None) -> str:
    """🪫12:00-14:00, 18:00-20:00 або «немає відключень»."""
    if not doc:
        return "немає даних"
    slots = doc.get("slots") or doc.get("events") or []
    parts = []
    for s in slots:
        if not isinstance(s, dict):
            continue
        fr = s.get("from") or s.get("start") or ""
        to = s.get("to") or s.get("end") or ""
        if fr and to:
            parts.append(f"{fr}-{to}")
    if parts:
        return "🪫" + ", ".join(parts)
    text = (doc.get("text") or "").strip()
    if text and text.lower() not in ("немає відключень",):
        return text
    return "немає відключень"


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


def build_personal_schedule_message(
    queue_labels: Sequence[str],
    *,
    header: str = "🔔 Оновлення графіка відключень",
    date_str: str = "",
) -> str:
    """Текст ТІЛЬКИ по чергах користувача (не весь канал)."""
    lines = [header, ""]
    if date_str:
        lines.append(f"🗓 {date_str}")
        lines.append("")
    for lab in queue_labels:
        doc = load_queue_doc(lab)
        line = _slots_line_from_doc(doc)
        lines.append(f"Черга {lab}:")
        lines.append(line)
        lines.append("")
    return "\n".join(lines).strip()


def notify_bot_users_with_images(
    message: str = "",
    *,
    queue_keys: Optional[Sequence[str]] = None,
    all_user_queues: bool = False,
    header: str = "",
    date_str: str = "",
    personalize: bool = True,
) -> None:
    """
    DM підписникам: картинки їхніх черг + caption одним повідомленням.

    personalize=True: текст збирається ЛИШЕ по чергах юзера з Mongo,
    а не весь текст каналу (де всі 1.1–6.2).
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
    if not header:
        # якщо передали повний текст каналу — беремо лише перший рядок як заголовок
        if message and message.strip().startswith("🔔"):
            header = message.strip().split("\n")[0].strip()
        else:
            header = "🔔 Оновлення графіка відключень"

    sent = 0
    failed = 0
    for tid, qs in targets.items():
        if personalize:
            body = build_personal_schedule_message(
                qs, header=header, date_str=date_str
            )
        else:
            body = (message or header) + f"\n\n📌 Твої черги: <b>{', '.join(qs)}</b>"

        caption = body if len(body) <= 1024 else (body[:1000] + "\n…")
        images = pngs_for_queues(qs)

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


def regenerate_queues_for_labels(labels: Sequence[str]) -> int:
    """Перегенерувати PNG для списку черг (4.2). Повертає кількість успішних."""
    try:
        from regenerate_images import load_ukrenergo_status, regenerate_one
        from mongo_store import get_db
    except Exception as e:
        log_to_buffer(f"⚠️ regenerate недоступний: {e}")
        return 0

    try:
        db = get_db()
        ukr = load_ukrenergo_status(db)
    except Exception as e:
        log_to_buffer(f"❌ Mongo/ukr status: {e}")
        return 0

    ok_n = 0
    for lab in labels:
        ch, pid = _queue_parts(lab)
        doc = None
        for _id in (
            f"{ch}|{pid}" if pid else ch,
            f"{ch}.{pid}" if pid else ch,
            ch,
        ):
            doc = db["queue_schedules"].find_one({"_id": _id})
            if doc:
                break
        if not doc:
            doc = {
                "queue": ch,
                "subqueue": pid,
                "slots": [],
                "events": [],
                "known": True,
            }
        status = regenerate_one(doc, ukr, dry_run=False, only_missing=False)
        if status == "ok":
            ok_n += 1
    return ok_n
