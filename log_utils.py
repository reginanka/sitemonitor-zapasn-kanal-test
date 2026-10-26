import os
import io
from datetime import datetime
from typing import List

import pytz
import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_LOG_CHANNEL_ID = os.getenv("TELEGRAM_LOG_CHANNEL_ID")
UKRAINE_TZ = pytz.timezone("Europe/Kyiv")

log_messages: List[str] = []


def get_ukraine_time() -> datetime:
    return datetime.now().astimezone(UKRAINE_TZ)


def log_to_buffer(message: str) -> None:
    ts = get_ukraine_time().strftime("%H:%M:%S")
    line = f"{ts} - {message}"
    print(line)
    log_messages.append(line)


def send_log_to_channel(title: str = "📊 ЛОГ ВИКОНАННЯ СКРИПТА · ЖОЕ") -> None:
    """
    Одне повідомлення в лог-канал + файл .txt з повним логом.
    Більше не дробить на 10 текстових частин.
    """
    if not TELEGRAM_LOG_CHANNEL_ID or not TELEGRAM_BOT_TOKEN or not log_messages:
        return

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
        r = requests.post(url, data=data, files=files, timeout=30)
        if r.status_code != 200:
            print(f"❌ sendDocument log: {r.status_code} {r.text[:300]}")
            # fallback: коротке текстове повідомлення без файлу
            short = caption + "\n\n(не вдалося надіслати .txt)"
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={
                    "chat_id": TELEGRAM_LOG_CHANNEL_ID,
                    "text": short[:4000],
                },
                timeout=10,
            )
    except Exception as e:
        print(f"❌ Помилка відправки логу в Telegram: {e}")
