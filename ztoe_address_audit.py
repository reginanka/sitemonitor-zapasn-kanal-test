#!/usr/bin/env python3
"""
Аудит адрес підписників: які поля є і чи слались сповіщення.

Пише в лог-канал (TELEGRAM_LOG_CHANNEL_ID / ADMIN_CHAT_ID / TELEGRAM_CHAT_ID)
короткий звіт по кожному користувачу з notify=True:

  - telegram_id
  - кожна адреса: city / street / house + otg_id, city_id, street_id
  - прапорці MISS_OTG / MISS_CITY (без них крон адресу не бачить або фетчить криво)
  - скільки записів у user_outage_notified і останні dedup_key

Env (ті самі, що в ztoe_outages_monitor):
  MONGO_URI
  MONGO_DB                  (default vidkluchennya)
  ZTOE_BOT_TOKEN
  TELEGRAM_LOG_CHANNEL_ID | ADMIN_CHAT_ID | TELEGRAM_CHAT_ID
  TELEGRAM_LOG_THREAD_ID    (default 153, 0 = без гілки)
  AUDIT_NOTIFIED_LIMIT      (скільки ключів показувати на юзера, default 8)
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient
from telegram import Bot


def _s(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _addr_lines(addr: dict) -> list[str]:
    if not isinstance(addr, dict):
        return ["  • адреса не dict"]
    otg = addr.get("otg_id") if addr.get("otg_id") is not None else addr.get("otgId")
    city_id = addr.get("city_id") if addr.get("city_id") is not None else addr.get("cityId")
    street_id = addr.get("street_id") if addr.get("street_id") is not None else addr.get("streetId")
    city = _s(addr.get("city_name") or addr.get("city"))
    street = _s(addr.get("street_name") or addr.get("street"))
    house = _s(addr.get("house") or addr.get("building"))
    flags = []
    if not otg:
        flags.append("MISS_OTG")
    if not city_id:
        flags.append("MISS_CITY")
    if not street and not street_id:
        flags.append("MISS_STREET")
    flag_s = ("  ⚠ " + ",".join(flags)) if flags else ""
    return [
        (
            f"  • {city or '?'} | {street or '?'} | буд {house or '—'}"
            f"{flag_s}"
        ),
        (
            f"    otg_id={otg or '—'} city_id={city_id or '—'} "
            f"street_id={street_id or '—'}"
        ),
    ]


def _fmt_dt(v) -> str:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).strftime("%d.%m %H:%M")
    return ""


def build_reports(db) -> tuple[str, list[str]]:
    users_col = db["users"]
    notified_col = db["user_outage_notified"]
    limit_keys = int(os.getenv("AUDIT_NOTIFIED_LIMIT", "8"))

    users = list(users_col.find({"notify": True}))
    n_users = 0
    n_addr = 0
    n_miss_otg = 0
    n_miss_city = 0
    n_no_notified = 0
    blocks: list[str] = []

    for u in users:
        tid = u.get("telegram_id")
        if tid is None:
            continue
        n_users += 1
        addrs = u.get("addresses") or []
        if not addrs and u.get("address"):
            addrs = [u["address"]]
        if not isinstance(addrs, list):
            addrs = [addrs]

        lines = [f"👤 tid={tid}  адрес={len(addrs)}"]
        user_miss = False
        for addr in addrs:
            n_addr += 1
            if isinstance(addr, dict):
                otg = addr.get("otg_id") if addr.get("otg_id") is not None else addr.get("otgId")
                city_id = addr.get("city_id") if addr.get("city_id") is not None else addr.get("cityId")
                if not otg:
                    n_miss_otg += 1
                    user_miss = True
                if not city_id:
                    n_miss_city += 1
                    user_miss = True
            lines.extend(_addr_lines(addr if isinstance(addr, dict) else {"raw": addr}))

        try:
            tid_i = int(tid)
        except (TypeError, ValueError):
            tid_i = None

        docs = []
        if tid_i is not None:
            docs = list(
                notified_col.find({"telegram_id": tid_i})
                .sort("notified_at", -1)
                .limit(limit_keys)
            )
        if not docs:
            n_no_notified += 1
            lines.append("  сповіщення: немає записів у user_outage_notified")
        else:
            total = notified_col.count_documents({"telegram_id": tid_i})
            lines.append(f"  сповіщення: {total} (показано {len(docs)})")
            for d in docs:
                when = _fmt_dt(d.get("notified_at"))
                key = _s(d.get("dedup_key"))
                if len(key) > 90:
                    key = key[:87] + "…"
                house = _s(d.get("house"))
                lines.append(f"    · {when} house={house or '—'} {key}")

        if user_miss:
            lines[0] += "  ← перевір id"
        blocks.append("\n".join(lines))

    summary = (
        "Аудит адрес підписників\n"
        f"notify=True: {n_users}\n"
        f"адрес: {n_addr}\n"
        f"без otg_id: {n_miss_otg}\n"
        f"без city_id: {n_miss_city}\n"
        f"юзерів без жодного user_outage_notified: {n_no_notified}\n"
        "MISS_OTG = крон адресу пропускає. MISS_CITY = фетч по otg, часто порожній."
    )
    return summary, blocks


def chunk_messages(summary: str, blocks: list[str], limit: int = 3800) -> list[str]:
    out = [summary]
    buf = ""
    for b in blocks:
        piece = b + "\n\n"
        if len(buf) + len(piece) > limit:
            if buf.strip():
                out.append(buf.strip())
            # один юзер довший за ліміт — ріжемо жорстко
            while len(piece) > limit:
                out.append(piece[:limit])
                piece = piece[limit:]
            buf = piece
        else:
            buf += piece
    if buf.strip():
        out.append(buf.strip())
    return out


def main() -> int:
    uri = os.getenv("MONGO_URI")
    if not uri:
        print("Немає MONGO_URI", file=sys.stderr)
        return 1
    token = os.getenv("ZTOE_BOT_TOKEN")
    if not token:
        print("Немає ZTOE_BOT_TOKEN", file=sys.stderr)
        return 1
    chat = (
        os.getenv("TELEGRAM_LOG_CHANNEL_ID")
        or os.getenv("ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
    )
    if not chat:
        print("Немає TELEGRAM_LOG_CHANNEL_ID", file=sys.stderr)
        return 1
    thread_raw = os.getenv("TELEGRAM_LOG_THREAD_ID", "153").strip()
    thread_id = int(thread_raw) if thread_raw and thread_raw != "0" else None

    db_name = os.getenv("MONGO_DB") or "vidkluchennya"
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    summary, blocks = build_reports(db)
    messages = chunk_messages(summary, blocks)
    print(f"users blocks={len(blocks)} tg messages={len(messages)}")

    bot = Bot(token=token)

    async def _send():
        for i, text in enumerate(messages, start=1):
            kwargs = {
                "chat_id": int(chat),
                "text": text[:4096],
                "disable_web_page_preview": True,
            }
            if thread_id:
                kwargs["message_thread_id"] = thread_id
            await bot.send_message(**kwargs)
            print(f"sent {i}/{len(messages)} chars={len(text)}")

    asyncio.run(_send())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
