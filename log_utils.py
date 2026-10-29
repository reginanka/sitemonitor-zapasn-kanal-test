import os
import io
from datetime import datetime
from typing import List, Optional

import pytz
import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_LOG_CHANNEL_ID = os.getenv("TELEGRAM_LOG_CHANNEL_ID")
# опційно: глобальний thread (якщо не передали в send_log_to_channel)
TELEGRAM_LOG_THREAD_ID = os.getenv("TELEGRAM_LOG_THREAD_ID")
UKRAINE_TZ = pytz.timezone("Europe/Kyiv")

log_messages: List[str] = []


def get_ukraine_time() -> datetime:
    return datetime.now().astimezone(UKRAINE_TZ)


def log_to_buffer(message: str) -> None:
    ts = get_ukraine_time().strftime("%H:%M:%S")
    line = f"{ts} - {message}"
    print(line)
    log_messages.append(line)


def send_log_to_channel(
    title: str = "📊 ЛОГ ВИКОНАННЯ СКРИПТА · ЖОЕ",
    message_thread_id: Optional[int] = None,
) -> None:
    """
    Одне повідомлення в лог-групу (з гілкою) + файл .txt з повним логом.
    message_thread_id — ID теми (topic) у супергрупі.
    Якщо не передано — береться з TELEGRAM_LOG_THREAD_ID (env).
    """
    if not TELEGRAM_LOG_CHANNEL_ID or not TELEGRAM_BOT_TOKEN or not log_messages:
        return

    # пріоритет: аргумент → env → без гілки
    thread_id = message_thread_id
    if thread_id is None and TELEGRAM_LOG_THREAD_ID:
        try:
            thread_id = int(TELEGRAM_LOG_THREAD_ID)
        except ValueError:
            thread_id = None

    try:
        finished = get_ukraine_time().strftime("%d.%m.%Y %H:%M:%S")
        log_body = "\n".join(log_messages)
        caption = (
            f"{title}\n"
            f"рядків: {len(log_messages)}\n"
            f"⏰ Завершено: {finished} (Київ)"
        )
        # caption ліміт ~1024
        if len(caption) > 1000:
            caption = caption[:997] + "…"

        filename = f"monitor_log_{get_ukraine_time().strftime('%Y%m%d_%H%M%S')}.txt"
        file_bytes = (title + "\n\n" + log_body + f"\n\n---\nЗавершено: {finished} (Київ)\n").encode(
            "utf-8"
        )

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        files = {
            "document": (filename, io.BytesIO(file_bytes), "text/plain; charset=utf-8"),
        }
        data = {
            "chat_id": TELEGRAM_LOG_CHANNEL_ID,
            "caption": caption,
        }
        if thread_id is not None:
            data["message_thread_id"] = thread_id

        r = requests.post(url, data=data, files=files, timeout=30)
        if r.status_code != 200:
            print(f"❌ sendDocument log: {r.status_code} {r.text[:300]}")
            # fallback: коротке текстове повідомлення без файлу
            short = caption + "\n\n(не вдалося надіслати .txt)"
            fallback_data = {
                "chat_id": TELEGRAM_LOG_CHANNEL_ID,
                "text": short[:4000],
            }
            if thread_id is not None:
                fallback_data["message_thread_id"] = thread_id
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data=fallback_data,
                timeout=10,
            )
    except Exception as e:
        print(f"❌ Помилка відправки логу в Telegram: {e}")
