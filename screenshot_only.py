#!/usr/bin/env python3
"""
Мінімальний скрипт: тільки скріншот сайту ЖОЕ → MongoDB.
Нічого не перевіряє, сповіщень не надсилає.
"""

import os
from pathlib import Path
from datetime import datetime

from log_utils import log_to_buffer, send_log_to_channel
from site_content import fetch_site_update_and_screenshot


def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_to_buffer("=" * 50)
    log_to_buffer(f"📸 SCREENSHOT ONLY [{timestamp}]")
    log_to_buffer("=" * 50)

    try:
        # 1. Скрін + дата оновлення одним проходом
        date_content, screenshot_path, screenshot_hash = fetch_site_update_and_screenshot()

        if not screenshot_path:
            log_to_buffer("❌ Не вдалося створити скріншот")
            return

        img_path = Path(screenshot_path)
        if not img_path.exists():
            log_to_buffer(f"❌ Файл скріну не існує: {screenshot_path}")
            return

        log_to_buffer(f"✅ Скрін створено: {img_path} ({img_path.stat().st_size} bytes)")
        if date_content:
            log_to_buffer(f"📅 Дата з сайту: {date_content}")
        if screenshot_hash:
            log_to_buffer(f"🔑 Hash: {screenshot_hash}")

        # 2. Зберегти в Mongo
        try:
            from mongo_store import save_site_screenshot

            png_bytes = img_path.read_bytes()
            fid = save_site_screenshot(png_bytes, source_hash=screenshot_hash or "")
            log_to_buffer(f"🗄 Скрін збережено в Mongo (id={fid}, {len(png_bytes)} bytes)")
        except Exception as e:
            log_to_buffer(f"❌ Помилка збереження в Mongo: {e}")

    except Exception as e:
        log_to_buffer(f"❌ Критична помилка: {e}")
    finally:
        # Логи в ту саму гілку, що й основний монітор (або змініть thread_id)
        send_log_to_channel(message_thread_id=151)
        log_to_buffer("🏁 Screenshot-only завершено")


if __name__ == "__main__":
    main()
