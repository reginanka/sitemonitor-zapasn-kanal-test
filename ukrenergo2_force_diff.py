"""Примусово записує «старий» стан Укренерго 2 в Mongo.

Після цього звичайний ukrenergo2_monitor.py побачить різницю
з live і надішле сповіщення + оновить базу.

Env:
  FORCE_STATUS          — normal | shortage | outage_schedules | emergency_outages | grid_damage
                          (за замовчуванням — shortage)
  FORCE_STATUS_CODE     — опційно, числовий код (3/4/9/10/11)
  UKRENERGO2_REGION_SLUG, MAYACHOK_MONGO_URI, MAYACHOK_MONGO_DB тощо — як у monitor
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict

from log_utils import log_to_buffer, send_log_to_channel
from ukrenergo2_monitor import (
    COLLECTION,
    REGION_SLUG,
    STATUS_META,
    _get_db,
    build_snapshot,
    fetch_regions,
    find_region,
    load_state,
    slug_title,
)

# Дозволені ключі статусу
ALLOWED = {
    "normal": 3,
    "shortage": 4,
    "outage_schedules": 9,
    "emergency_outages": 10,
    "grid_damage": 11,
}


def _resolve_target() -> tuple[str, int]:
    raw = (os.getenv("FORCE_STATUS") or "shortage").strip().lower()
    if raw not in ALLOWED:
        raise RuntimeError(
            f"FORCE_STATUS='{raw}' невідомий. "
            f"Дозволено: {', '.join(ALLOWED)}"
        )
    code_env = (os.getenv("FORCE_STATUS_CODE") or "").strip()
    code = int(code_env) if code_env.isdigit() else ALLOWED[raw]
    return raw, code


def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer(f"🔧 FORCE DIFF · slug={REGION_SLUG}")

    try:
        target_status, target_code = _resolve_target()
        meta = STATUS_META.get(target_status, {})

        # Читаємо live лише щоб узяти name_uk і згенерувати реалістичний знімок
        data = fetch_regions()
        region = find_region(data, REGION_SLUG)
        live = build_snapshot(data, region)

        previous = load_state()
        log_to_buffer(
            f"📋 Mongo зараз: {previous.get('status')}/{previous.get('status_code')}"
            if previous
            else "📋 Mongo порожній"
        )
        log_to_buffer(
            f"📡 Live зараз: {live.get('status')}/{live.get('status_code')}"
        )

        forced: Dict[str, Any] = {
            "source": "force-diff",
            "slug": REGION_SLUG,
            "name_uk": live.get("name_uk") or slug_title(REGION_SLUG),
            "status": target_status,
            "status_code": target_code,
            "emoji": meta.get("emoji") or "❔",
            "title_uk": meta.get("title_uk") or target_status,
            "since": None,
            "period": None,
            "next_change": None,
            "generated_at": live.get("generated_at"),
            "changed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "forced_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "note": f"Примусово встановлено для тесту (було live={live.get('status')})",
        }

        db = _get_db()
        db[COLLECTION].update_one(
            {"slug": REGION_SLUG},
            {"$set": forced},
            upsert=True,
        )

        log_to_buffer(
            f"✅ Mongo оновлено: status={target_status} / code={target_code}"
        )
        log_to_buffer(
            "➡️  Тепер запусти звичайний Monitor Ukrenergo 2 — "
            "він має побачити зміну і надіслати сповіщення"
        )
    except Exception as e:
        log_to_buffer(f"❌ Force diff: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ · FORCE DIFF УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
