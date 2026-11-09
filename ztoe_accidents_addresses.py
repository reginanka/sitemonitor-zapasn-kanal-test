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
    # True only for real accidents (GAV/SGAV). Skip GPV (hourly schedule).
    # switchSubReason: 1=GAV, 2=GPV(skip), 3=SGAV
    # typeOff (if sub==0): 1=emergency
    koment = (doc.get("koment") or doc.get("comment") or "").upper()
    if "ГПВ" in koment:
        return False

    type_name = (
        doc.get("type_name")
        or doc.get("typeName")
        or doc.get("typeOffName")
        or ""
    )
    if isinstance(type_name, str):
        tn = type_name.lower()
        if "погодин" in tn or "гпв" in tn:
            return False

    try:
        sub = int(doc.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        sub = 0
    try:
        t = int(doc.get("typeOff")) if doc.get("typeOff") is not None else -1
    except (TypeError, ValueError):
        t = -1

    if sub == 2:
        return False  # GPV
    if sub in (1, 3):
        return True  # GAV / SGAV
    if sub > 0:
        return False

    # sub == 0 -> check typeOff
    return t == 1


def fetch_all_accidents() -> List[dict]:
    if not API_BASE:
        raise SystemExit("Немає ZTOE_API_BASE")

    headers = {
        "Accept": "application/ld+json",
        "User-Agent": "ztoe-accidents-addresses/1.0",
    }
    items: List[dict] = []

    with httpx.Client(timeout=60, headers=headers) as client:
        try:
            r = client.get(
                f"{API_BASE}/pw_accidents",
                params={"pagination": "false"},
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                log_to_buffer(f"API: list → {len(data)} записів")
                filtered = [x for x in data if is_real_accident(x)]
                log_to_buffer(
                    f"Фільтр аварій: {len(data)} → {len(filtered)} "
                    f"(відкинуто ГПВ/не-аварії: {len(data) - len(filtered)})"
                )
                return filtered
            batch = data.get("hydra:member") or []
            if batch:
                log_to_buffer(f"API: pagination=false → {len(batch)} записів")
                filtered = [x for x in batch if is_real_accident(x)]
                log_to_buffer(
                    f"Фільтр аварій: {len(batch)} → {len(filtered)} "
                    f"(відкинуто ГПВ/не-аварії: {len(batch) - len(filtered)})"
                )
                return filtered
        except Exception as e:
            log_to_buffer(f"⚠️ pagination=false failed: {e} — сторінки")

        page = 1
        while page <= 100:
            r = client.get(
                f"{API_BASE}/pw_accidents",
                params={"page": page, "itemsPerPage": 100},
            )
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
        log_to_buffer(f"API: по сторінках → {len(items)} записів (сторінок: {page})")

    raw_n = len(items)
    items = [a for a in items if is_real_accident(a)]
    log_to_buffer(
        f"Фільтр аварій: {raw_n} → {len(items)} "
        f"(відкинуто ГПВ/не-аварії: {raw_n - len(items)})"
    )
    return items


def extract_addresses(accidents: List[dict], limit: int = 20) -> List[str]:
    """
    Унікальні адреси у форматі н.п. + вулиця.
    Порядок — як прийшли з API (перші limit унікальних).
    """
    seen: Set[Tuple[str, str]] = set()
    result: List[str] = []

    for a in accidents:
        city = a.get("city") or {}
        street = a.get("street") or {}

        cname = (city.get("name") or "").strip() or "?"
        sname = (street.get("name") or "").strip() or ""

        # fallback: іноді назва вулиці може бути в інших полях
        if not sname:
            sname = (
                (a.get("streetName") or a.get("street_name") or "")
            ).strip()

        key = (cname, sname)
        if key in seen:
            continue
        seen.add(key)

        if sname:
            line = f"{city_label(cname)}, {sname}"
        else:
            line = city_label(cname)

        result.append(line)
        if len(result) >= limit:
            break

    return result


def format_log_message(addresses: List[str], total_accidents: int, as_of: datetime) -> str:
    time_str = as_of.strftime("%H:%M %d.%m.%Y")
    lines = [
        f"Аварійні адреси ЖОЕ (до {MAX_ADDRESSES})",
        f"Записів аварій: {total_accidents}",
        f"Унікальних адрес у вибірці: {len(addresses)}",
        f"Час: {time_str}",
        "",
    ]
    if not addresses:
        lines.append("Немає адрес.")
    else:
        for i, addr in enumerate(addresses, 1):
            lines.append(f"{i}. {addr}")
    return "\n".join(lines)


def main() -> int:
    as_of = datetime.now(KYIV)
    log_to_buffer(f"=== ztoe_accidents_addresses start {as_of.isoformat()} ===")

    try:
        accidents = fetch_all_accidents()
    except Exception as e:
        log_to_buffer(f"❌ fetch failed: {e}")
        send_log_to_channel()
        return 1

    addresses = extract_addresses(accidents, limit=MAX_ADDRESSES)
    log_to_buffer(f"Унікальних адрес: {len(addresses)} (ліміт {MAX_ADDRESSES})")

    msg = format_log_message(addresses, len(accidents), as_of)
    # Дублюємо список у буфер логу (щоб було видно в pre-блоці)
    for line in msg.split("\n"):
        log_to_buffer(line)

    send_log_to_channel()
    log_to_buffer("=== done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
