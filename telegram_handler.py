import os
import logging
from pathlib import Path
import asyncio
from typing import List, Optional, Sequence, Union

from telegram import Bot, InputMediaPhoto
from telegram.error import TelegramError

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")


async def send_message(message: str, channel_id: str = TELEGRAM_CHANNEL_ID) -> bool:
    """Відправити текстове повідомлення"""
    if not TELEGRAM_BOT_TOKEN or not channel_id:
        logger.error("❌ TELEGRAM_BOT_TOKEN або TELEGRAM_CHANNEL_ID не налаштовані")
        return False

    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(
            chat_id=channel_id,
            text=message,
            parse_mode="HTML",
        )
        logger.info("✓ Повідомлення відправлено в Telegram")
        return True
    except TelegramError as e:
        logger.error(f"❌ Помилка Telegram: {e}")
        return False


async def send_photo(
    image_path: Path,
    caption: str = None,
    channel_id: str = TELEGRAM_CHANNEL_ID,
) -> bool:
    """Відправити картинку"""
    if not TELEGRAM_BOT_TOKEN or not channel_id:
        logger.error("❌ Telegram не налаштований")
        return False

    if not image_path or not image_path.exists():
        logger.warning(f"⚠️  Картинка не знайдена: {image_path}")
        return False

    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        with open(image_path, "rb") as f:
            await bot.send_photo(
                chat_id=channel_id,
                photo=f,
                caption=caption,
                parse_mode="HTML",
            )
        logger.info(f"✓ Картинка відправлена: {image_path.name}")
        return True
    except TelegramError as e:
        logger.error(f"❌ Помилка картинки: {e}")
        return False


async def send_photos_with_caption(
    chat_id: Union[str, int],
    images: Sequence[bytes],
    caption: str = "",
    *,
    bot_token: Optional[str] = None,
) -> bool:
    """
    Надіслати 1..N картинок одним повідомленням.
    caption — під першим фото (ліміт Telegram ~1024 для media group caption).
    Якщо images порожній — шле тільки текст.
    """
    token = bot_token or TELEGRAM_BOT_TOKEN
    if not token:
        logger.error("❌ TELEGRAM_BOT_TOKEN не налаштований")
        return False

    bot = Bot(token=token)
    cap = (caption or "")[:1024]

    try:
        if not images:
            if not caption:
                return False
            await bot.send_message(
                chat_id=chat_id,
                text=caption[:4096],
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return True

        if len(images) == 1:
            await bot.send_photo(
                chat_id=chat_id,
                photo=images[0],
                caption=cap or None,
                parse_mode="HTML" if cap else None,
            )
            return True

        # Media group: до 10 фото; caption лише на першому
        media: List[InputMediaPhoto] = []
        for i, png in enumerate(images[:10]):
            media.append(
                InputMediaPhoto(
                    media=png,
                    caption=cap if i == 0 and cap else None,
                    parse_mode="HTML" if i == 0 and cap else None,
                )
            )
        await bot.send_media_group(chat_id=chat_id, media=media)
        return True
    except TelegramError as e:
        logger.error(f"❌ Помилка send_photos chat={chat_id}: {e}")
        return False


def send_notification(message: str, image_path: Path = None) -> bool:
    """
    Синхронна обгортка для відправлення в канал.
    Якщо є картинка — шле повідомлення З картинкою (без дублювання).
    Якщо нема картинки — шле просто текст.
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            if image_path and image_path.exists():
                loop.run_until_complete(send_photo(image_path, caption=message))
            else:
                loop.run_until_complete(send_message(message))
            return True
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"❌ Помилка відправлення: {e}")
        return False


def send_photos_sync(
    chat_id: Union[str, int],
    images: Sequence[bytes],
    caption: str = "",
    *,
    bot_token: Optional[str] = None,
) -> bool:
    """Синхронна обгортка send_photos_with_caption."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                send_photos_with_caption(
                    chat_id, images, caption, bot_token=bot_token
                )
            )
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"❌ Помилка send_photos_sync: {e}")
        return False
