#!/usr/bin/env python3
"""
Список аварійних адрес ЖОЕ (до 20 шт.) → лог-канал.

- Тягне всі записи з /pw_accidents (той самий спосіб, що й ztoe_accidents_summary)
- Фільтрує лише справжні аварії (ГАВ/СГАВ, без ГПВ)
- Збирає унікальні адреси: н.п. + вулиця
- Бере перші 20 і шле в TELEGRAM_LOG_CHANNEL_ID
- Без Mongo, без summary, без каналу сповіщень
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

import httpx

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:
    import pytz
    KYIV = pytz.timezone("Europe/Kyiv")


# --- logging buffer (сумісно з log_utils + fallback) ---
_log_lines: list[str] = []

try:
    from log_utils import log_to_buffer as _lu_log, send_log_to_channel as _lu_send
    _HAS_LOG_UTILS = True
except Exception:
    _HAS_LOG_UTILS = False

    def _lu_log(msg: str) -> None:
        pass

    def _lu_send(title: str = "", message_thread_id=None) -> None:
        pass


def log_to_buffer(msg: str) -> None:
    """Пише в консоль + буфер + log_utils (якщо є)."""
    print(msg)
    _log_lines.append(msg)
    try:
        _lu_log(msg)
    except Exception:
        pass


def send_log_to_channel(
    title: str = "📋 ЛОГ · АВАРІЙНІ АДРЕСИ ЖОЕ",
    message_thread_id: int | None = 154,
) -> None:
    """
    Шле лог у TELEGRAM_LOG_CHANNEL_ID.
    1) пробує log_utils
    2) fallback на httpx
    """
    try:
        if _HAS_LOG_UTILS:
            _lu_send(title, message_thread_id=message_thread_id)
    except Exception as e:
        print(f"log_utils send failed: {e}")

    token = os.getenv("ZTOE_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or ""
    if not token or not chat_id or not _log_lines:
        if not chat_id:
            print("⚠️ TELEGRAM_LOG_CHANNEL_ID не задано — лог у канал не піде")
        elif not token:
            print("⚠️ ZTOE_BOT_TOKEN не задано — лог у канал не піде")
        return

    try:
        body = "\n".join(_log_lines)
        text_msg = f"{title}\n\n<pre>{body}</pre>"
        if len(text_msg) > 4000:
            text_msg = text_msg[:3900] + "\n…</pre>"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text_msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        if message_thread_id is not None:
            data["message_thread_id"] = message_thread_id
        r = httpx.post(url, data=data, timeout=15)
        if r.status_code == 200:
            print(f"✓ Лог відправлено в {chat_id}")
        else:
            print(f"❌ Лог Telegram HTTP {r.status_code}: {r.text[:300]}")
    except Exception as e:
        print(f"❌ Лог fallback failed: {e}")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_accidents_addresses")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or "").rstrip("/")
MAX_ADDRESSES = int(os.getenv("MAX_ADDRESSES") or "20")

CITY_PREFIX = {
    "Житомир", "Звягель", "Бердичів", "Овруч", "Коростень", "Малин",
}


def city_label(name: str) -> str:
    if name in CITY_PREFIX:
        return f"м. {name}"
    return name


def is_real_accident(doc: dict) -> bool:
    """
    True лише для справжніх аварій / ГАВ / СГАВ.
    ГПВ (графік погодинних) і явні згадки ГПВ у коментарі — відкидаємо.

    Логіка як у bot_notify._classify_outage / 
... 
