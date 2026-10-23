"""Тест повідомлення в канал (Укренерго 2).

- Читає попередній стан з Mongo (якщо є)
- Тягне актуальний snapshot з regions.json
- Формує повідомлення у форматі реальної зміни статусу
- Шле в TELEGRAM_CHANNEL_ID
- НІЧОГО не записує в Mongo / regionstatuses / файли

Запуск: лише вручну через workflow ukrenergo2-test.yml
"""

from __future__ import annotations

from log_utils import log_to_buffer, send_log_to_channel
from telegram_handler import send_notification
from ukrenergo2_monitor import (
    REGION_SLUG,
    build_snapshot,
    comparable,
    fetch_regions,
    find_region,
    format_message,
    is_stale,
    load_state,
)


def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer(f"🧪 TEST УКРЕНЕРГО 2 · slug={REGION_SLUG} (без запису в Mongo)")
    try:
        previous = load_state()
        if previous:
            log_to_buffer(
                f"📋 Mongo «було»: {previous.get('status')}/{previous.get('status_code')} "
                f"— {previous.get('title_uk')}"
            )
        else:
            log_to_buffer("📋 Mongo порожній — у повідомленні «Було» не використовується")

        data = fetch_regions()
        region = find_region(data, REGION_SLUG)
        current = build_snapshot(data, region)
        stale = is_stale(current.get("generated_at"))

        log_to_buffer(
            f"✓ Live: {current.get('emoji')} {current.get('title_uk')} "
            f"(status={current.get('status')}, code={current.get('status_code')})"
        )
        log_to_buffer(f"  period={current.get('period')}")
        log_to_buffer(f"  generated_at={current.get('generated_at')}")

        if previous and comparable(previous) == comparable(current):
            log_to_buffer("ℹ️ Live збігається з Mongo — тест все одно шле приклад повідомлення")

        # format_message зараз показує лише поточний статус (стиль каналу),
        # prev потрібен лише для сумісності сигнатури
        prev_for_msg = previous or {}
        body = format_message(prev_for_msg, current, stale)
        message = (
            "🧪 <b>ТЕСТ</b> (нічого не записано в базу)\n"
            "Так виглядатиме сповіщення при зміні статусу:\n\n"
            + body
        )

        ok = send_notification(message)
        if ok:
            log_to_buffer("✅ Тестове повідомлення надіслано в канал")
        else:
            log_to_buffer("❌ Не вдалось надіслати тестове повідомлення")
    except Exception as e:
        log_to_buffer(f"❌ Test Укренерго 2: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ · TEST УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
