"""
Спільна розсилка підписникам бота ztoe:
  - текст під картинкою (caption)
  - кілька PNG одним повідомленням (media group)

Використовується з monitor.py (зміна графіків) і ukrenergo2_monitor.py (зміна статусу).
"""

from __future__ import annotations

import os
import time
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
                    if addr_matches_queue(addr, qk) and qk not in matched:
                        # нормалізований ключ
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


def notify_bot_users_with_images(
    message: str,
    *,
    queue_keys: Optional[Sequence[str]] = None,
    all_user_queues: bool = False,
) -> None:
    """
    DM підписникам: картинки черг + caption (текст під першим фото).
    Кілька картинок — одне media group повідомлення.

    queue_keys — лише ці черги (зміна графіка).
    all_user_queues=True — усі черги з адрес користувача (зміна статусу Укренерго).
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

    body = message or "🔔 Оновлення"
    # caption media group ≤ 1024; якщо довший — обрізаємо (повний текст і так у каналі)
    caption = body if len(body) <= 1024 else (body[:1000] + "\n…")

    sent = 0
    failed = 0
    for tid, qs in targets.items():
        images = pngs_for_queues(qs)
        # підпис: текст + які черги
        q_note = ", ".join(qs)
        cap = caption
        if q_note and "Черг" not in caption and "черг" not in caption.lower():
            extra = f"\n\n📌 Твої черги: <b>{q_note}</b>"
            if len(cap) + len(extra) <= 1024:
                cap = cap + extra

        ok = send_photos_sync(tid, images, cap, bot_token=bot_token)
        if ok:
            sent += 1
        else:
            failed += 1
        time.sleep(0.05)  # м’який rate-limit

    log_to_buffer(
        f"📬 DM бота: sent={sent} failed={failed} users={len(targets)} "
        f"(з картинками={'так' if any(True for _ in targets) else 'ні'})"
    )


def regenerate_queues_for_labels(labels: Sequence[str]) -> int:
    """
    Перегенерувати PNG для списку черг (4.2).
    Повертає кількість успішних.
    """
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
        # шукаємо документ
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
            # мінімальний doc для рендеру порожнього графіка
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
