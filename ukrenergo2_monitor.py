"""Моніторинг статусу енергосистеми — Укренерго 2 (неофіційне джерело).

URL regions.json береться з секрету UKRENERGO2_REGIONS_URL (не хардкодиться).

Окремий скрипт від офіційного ukrenergo_monitor.py (API з Matrix-ключем).
Порівнює status + status_code для обраного регіону з data/ukrenergo2_state.json.
Публічне сповіщення лише при зміні. Перший запуск лише зберігає базовий стан.
"""

import html
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from log_utils import log_to_buffer, send_log_to_channel
from telegram_handler import send_notification

# URL на regions.json — тільки через секрет (не хардкодити в репо)
REGIONS_URL = (os.getenv("UKRENERGO2_REGIONS_URL") or "").strip()
# За замовчуванням Житомир; можна перевизначити секретом UKRENERGO2_REGION_SLUG
REGION_SLUG = (os.getenv("UKRENERGO2_REGION_SLUG") or "zhytomyr").strip().lower()
REGION_NAME_OVERRIDE = (os.getenv("UKRENERGO2_REGION_NAME") or "").strip()
SUBSCRIBE = os.getenv("SUBSCRIBE", "")
STALE_HOURS = 2

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "ukrenergo2_state.json"

# Коди збігаються з офіційним dictionary/getStates і з FALLBACK_STATES у ukrenergo_monitor.py
STATUS_META = {
    "normal": {
        "code": 3,
        "emoji": "🟢",
        "title_uk": "Електроенергії достатньо",
    },
    "shortage": {
        "code": 4,
        "emoji": "🟡",
        "title_uk": "Будь ласка, споживайте електроенергію ощадливо",
    },
    "outage_schedules": {
        "code": 9,
        "emoji": "🔴",
        "title_uk": "Застосовані графіки погодинних відключень",
    },
    "emergency_outages": {
        "code": 10,
        "emoji": "🚨",
        "title_uk": "Застосовані аварійні відключення",
    },
    "grid_damage": {
        "code": 11,
        "emoji": "⚫️",
        "title_uk": "Знеструмлення – наслідок пошкодження мережі",
    },
}


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def load_state() -> Dict:
    if not STATE_FILE.exists():
        return {}
    try:
        with STATE_FILE.open(encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log_to_buffer(f"⚠️ Не вдалось прочитати {STATE_FILE.name}: {e}")
        return {}


def save_state(snapshot: Dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    log_to_buffer(f"💾 Стан збережено в {STATE_FILE.name}")


def is_stale(generated_at: Optional[str]) -> bool:
    if not generated_at:
        return True
    try:
        ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - ts > timedelta(hours=STALE_HOURS)
    except Exception:
        return True


def fetch_regions() -> Dict[str, Any]:
    if not REGIONS_URL:
        raise RuntimeError(
            "UKRENERGO2_REGIONS_URL не задано "
            "(потрібен секрет з посиланням на regions.json)"
        )
    # У лог — лише хост, без повного шляху
    try:
        from urllib.parse import urlparse
        host = urlparse(REGIONS_URL).netloc or "…"
    except Exception:
        host = "…"
    log_to_buffer(f"📡 Завантажую regions.json ({host})")
    resp = requests.get(REGIONS_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    log_to_buffer(
        f"✓ generated_at={data.get('generated_at')}, "
        f"regions={len(data.get('regions') or [])}"
    )
    return data


def find_region(data: Dict[str, Any], slug: str) -> Dict[str, Any]:
    for r in data.get("regions") or []:
        if isinstance(r, dict) and (r.get("slug") or "").lower() == slug:
            return r
    raise RuntimeError(f"Регіон slug='{slug}' не знайдено в regions.json")


def fmt_kyiv(iso: Optional[str]) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        # Europe/Kyiv ≈ UTC+2/+3; для відображення достатньо +3 у більшості випадків,
        # точніше через pytz якщо треба
        try:
            import pytz
            dt = dt.astimezone(pytz.timezone("Europe/Kyiv"))
        except Exception:
            dt = dt.astimezone(timezone(timedelta(hours=3)))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso


def build_snapshot(data: Dict[str, Any], region: Dict[str, Any]) -> Dict:
    status = region.get("status") or "unknown"
    status_code = region.get("status_code")
    meta = STATUS_META.get(status, {})
    emoji = region.get("emoji") or meta.get("emoji") or "❔"
    title = region.get("title_uk") or meta.get("title_uk") or status
    name_uk = REGION_NAME_OVERRIDE or region.get("name_uk") or slug_title(REGION_SLUG)

    return {
        "source": "ua-power-status",
        "slug": REGION_SLUG,
        "name_uk": name_uk,
        "status": status,
        "status_code": status_code,
        "emoji": emoji,
        "title_uk": title,
        "since": region.get("since"),
        "period": region.get("period"),
        "next_change": region.get("next_change"),
        "generated_at": data.get("generated_at"),
        "changed_at": data.get("changed_at"),
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }


def slug_title(slug: str) -> str:
    mapping = {
        "zhytomyr": "Житомирська область",
        "kyiv": "м. Київ",
        "kyiv-oblast": "Київська область",
    }
    return mapping.get(slug, slug)


def comparable(snapshot: Dict) -> Dict:
    """Що порівнюємо для рішення «чи змінилось»."""
    return {
        "status": snapshot.get("status"),
        "status_code": snapshot.get("status_code"),
        "title_uk": snapshot.get("title_uk"),
        "since": snapshot.get("since"),
        "period": snapshot.get("period"),
    }


def format_message(prev: Dict, cur: Dict, stale: bool) -> str:
    lines = [
        "⚡️ <b>Укренерго 2</b> <i>(неофіційне джерело)</i>",
        f"📍 {esc(cur.get('name_uk'))}",
        "",
        f"Було: {esc(prev.get('emoji') or '')} {esc(prev.get('title_uk') or '—')} "
        f"(код {esc(prev.get('status_code'))})",
        f"Стало: {esc(cur.get('emoji') or '')} <b>{esc(cur.get('title_uk'))}</b> "
        f"(код {esc(cur.get('status_code'))})",
    ]

    since_str = fmt_kyiv(cur.get("since"))
    if since_str:
        lines.append(f"🕐 Діє з: {esc(since_str)} (Київ)")

    period = cur.get("period") or {}
    if isinstance(period, dict) and (period.get("start") or period.get("end")):
        p_parts = []
        if period.get("start"):
            p_parts.append(f"з {fmt_kyiv(period['start'])}")
        if period.get("end"):
            p_parts.append(f"до {fmt_kyiv(period['end'])}")
        if p_parts:
            lines.append(f"📅 Період: {esc(' '.join(p_parts))}")

    lines.append("")
    lines.append("Джерело: Укренерго 2 (неофіційно, без гарантій) · НЕК «Укренерго»")
    if stale:
        lines.append(
            f"⚠️ Дані можуть бути застарілими (generated_at={esc(cur.get('generated_at'))})"
        )
    if SUBSCRIBE:
        lines.append(f'<a href="{esc(SUBSCRIBE)}">⚡️ ПІДПИСАТИСЯ</a>')
    return "\n".join(lines)


def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer(f"🚀 СТАРТ УКРЕНЕРГО 2 · slug={REGION_SLUG}")
    try:
        data = fetch_regions()
        region = find_region(data, REGION_SLUG)
        current = build_snapshot(data, region)
        stale = is_stale(current.get("generated_at"))

        log_to_buffer(
            f"✓ Зараз: {current.get('emoji')} {current.get('title_uk')} "
            f"(status={current.get('status')}, code={current.get('status_code')})"
        )
        if stale:
            log_to_buffer(
                f"⚠️ generated_at старший за {STALE_HOURS} год — дані можуть бути неактуальними"
            )

        previous = load_state()
        if not previous:
            save_state(current)
            log_to_buffer("📋 Базовий стан збережено, сповіщення не шлю")
            return

        if comparable(previous) == comparable(current):
            log_to_buffer("✅ Статус регіону не змінився")
            return

        log_to_buffer(
            f"🔔 Зміна: {previous.get('status')}/{previous.get('status_code')} "
            f"→ {current.get('status')}/{current.get('status_code')}"
        )
        message = format_message(previous, current, stale)
        ok = send_notification(message)
        if not ok:
            log_to_buffer("❌ Не вдалось надіслати сповіщення, стан не перезаписую")
            return
        save_state(current)
        log_to_buffer("✅ Сповіщення надіслано, стан збережено")
    except Exception as e:
        log_to_buffer(f"❌ Критична помилка Укренерго 2: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ ВИКОНАННЯ СКРИПТА · УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
