"""One-shot: записати поточний стан Укренерго 2 в Mongo І надіслати повідомлення в канал.

Використовується окремим workflow (seed), щоб «засіяти» базу з публікацією,
на відміну від звичайного ukrenergo2_monitor.py (перший запуск там мовчить).

Після успішного seed звичайний monitor працює як завжди (порівняння з Mongo).
"""

from __future__ import annotations

from log_utils import log_to_buffer, send_log_to_channel
from telegram_handler import send_notification
from ukrenergo2_monitor import (
    REGION_SLUG,
    build_snapshot,
    fetch_regions,
    find_region,
    format_message,
    is_stale,
    load_state,
    save_state,
)


def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer(f"🌱 SEED УКРЕНЕРГО 2 · slug={REGION_SLUG}")
    try:
        data = fetch_regions()
        region = find_region(data, REGION_SLUG)
        current = build_snapshot(data, region)
        stale = is_stale(current.get("generated_at"))

        log_to_buffer(
            f"✓ Зараз: {current.get('emoji')} {current.get('title_uk')} "
            f"(status={current.get('status')}, code={current.get('status_code')})"
        )

        previous = load_state()
        # Для форматування «Було → Стало»: якщо в Mongo вже щось є — показуємо diff,
        # якщо ні — «Було: —»
        prev_for_msg = previous or {
            "title_uk": "—",
            "status": "—",
            "status_code": None,
        }

        message = format_message(prev_for_msg, current, stale)
        # Позначаємо, що це seed / перша публікація
        if not previous:
            message = "🌱 <b>Початковий статус області</b>\n\n" + message
        else:
            message = "🔁 <b>Примусове оновлення статусу</b>\n\n" + message

        ok = send_notification(message)
        if not ok:
            log_to_buffer("❌ Не вдалось надіслати в канал — стан у Mongo НЕ змінюю")
            return

        save_state(current)
        log_to_buffer("✅ Seed: повідомлення в канал + стан у Mongo збережено")
    except Exception as e:
        log_to_buffer(f"❌ Seed Укренерго 2: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ · SEED УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
