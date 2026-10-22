"""Моніторинг статусу енергосистеми — Укренерго 2 (неофіційне джерело).

URL regions.json береться з секрету UKRENERGO2_REGIONS_URL (не хардкодиться).

Окремий скрипт від офіційного ukrenergo_monitor.py (API з Matrix-ключем).
Порівнює status + status_code для обраного регіону з MongoDB (колекція ukrenergo2_states).
Публічне сповіщення лише при зміні. Перший запуск лише зберігає базовий стан.

Файл data/ukrenergo2_state.json більше НЕ використовується і НЕ комітиться в git.
"""

from __future__ import annotations

import html
import os
from datetime import datetime, timezone, timedelta
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

# Mongo: той самий URI, що для Маячка (або окремий)
MONGO_URI = (
    os.getenv("MAYACHOK_MONGO_URI")
    or os.getenv("MONGO_URI")
    or ""
).strip()
MONGO_DB = (
    os.getenv("MAYACHOK_MONGO_DB")
    or os.getenv("MONGO_DB_NAME")
    or "powerbot"
).strip()
# Колекція стану Укренерго 2
COLLECTION = "ukrenergo2_states"
# Опційно дублюємо в regionstatuses (схема бота Маячок)
SYNC_REGIONSTATUS = (os.getenv("UKRENERGO2_SYNC_REGIONSTATUS") or "1").strip() not in (
    "0",
    "false",
    "no",
)

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


def _get_db():
    if not MONGO_URI:
        raise RuntimeError(
            "MAYACHOK_MONGO_URI (або MONGO_URI) не задано — "
            "стан Укренерго 2 зберігається в MongoDB"
        )
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[MONGO_DB]


def load_state() -> Dict:
    """Читає попередній знімок з Mongo по slug."""
    try:
        db = _get_db()
        doc = db[COLLECTION].find_one({"slug": REGION_SLUG}, {"_id": 0})
        if not doc:
            log_to_buffer(f"📋 Mongo: стану для slug={REGION_SLUG} ще немає")
            return {}
        log_to_buffer(
            f"📋 Mongo: завантажено стан "
            f"{doc.get('status')}/{doc.get('status_code')} "
            f"(checked_at={doc.get('checked_at')})"
        )
        return doc
    except Exception as e:
        log_to_buffer(f"⚠️ Не вдалось прочитати стан з Mongo: {e}")
        return {}


def save_state(snapshot: Dict) -> None:
    """Upsert знімка в Mongo (+ опційно regionstatuses для бота)."""
    db = _get_db()
    doc = dict(snapshot)
    doc["slug"] = REGION_SLUG
    doc["updatedAt"] = datetime.utcnow()

    db[COLLECTION].update_one(
        {"slug": REGION_SLUG},
        {"$set": doc},
        upsert=True,
    )
    log_to_buffer(
        f"💾 Mongo: стан збережено в {COLLECTION} "
        f"(slug={REGION_SLUG}, status={doc.get('status')})"
    )

    if SYNC_REGIONSTATUS:
        try:
            period = doc.get("period") or {}
            region_doc = {
                "slug": REGION_SLUG,
                "status": doc.get("status") or "",
                "statusCode": doc.get("status_code"),
                "emoji": doc.get("emoji"),
                "title": doc.get("title_uk") or doc.get("title"),
                "since": doc.get("since"),
                "period": {
                    "start": period.get("start") if isinstance(period, dict) else None,
                    "end": period.get("end") if isinstance(period, dict) else None,
                },
                "generatedAt": doc.get("generated_at"),
                "changedAt": doc.get("changed_at"),
                "fetchedAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow(),
            }
            db["regionstatuses"].update_one(
                {"slug": REGION_SLUG},
                {"$set": region_doc},
                upsert=True,
            )
            log_to_buffer("💾 Mongo: regionstatuses синхронізовано")
        except Exception as e:
            log_to_buffer(f"⚠️ regionstatuses sync: {e}")


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
    period = snapshot.get("period") or {}
    return {
        "status": snapshot.get("status"),
        "status_code": snapshot.get("status_code"),
        "title_uk": snapshot.get("title_uk"),
        "since": snapshot.get("since"),
        "period_start": period.get("start") if isinstance(period, dict) else None,
        "period_end": period.get("end") if isinstance(period, dict) else None,
    }


def format_message(prev: Dict, cur: Dict, stale: bool) -> str:
    lines = [
        f"{cur.get('emoji') or '⚡️'} <b>Укренерго</b>",
        f"📍 {esc(cur.get('name_uk') or REGION_SLUG)}",
        "",
        f"Було: {esc(prev.get('title_uk') or prev.get('status') or '—')}",
        f"Стало: <b>{esc(cur.get('title_uk') or cur.get('status'))}</b>",
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
            log_to_buffer("📋 Базовий стан збережено в Mongo, сповіщення не шлю")
            return

        if comparable(previous) == comparable(current):
            # Оновлюємо лише checked_at, без сповіщення
            try:
                db = _get_db()
                db[COLLECTION].update_one(
                    {"slug": REGION_SLUG},
                    {"$set": {"checked_at": current.get("checked_at")}},
                )
            except Exception:
                pass
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
        log_to_buffer("✅ Сповіщення надіслано, стан збережено в Mongo")
    except Exception as e:
        log_to_buffer(f"❌ Критична помилка Укренерго 2: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ ВИКОНАННЯ СКРИПТА · УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
