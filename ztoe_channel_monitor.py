"""
Монітор публічного каналу ЖОЕ (URL у секреті ZTOE_CHANNEL_URL): нові пости → канал + DM підписникам бота.

Логіка:
  1. Тягнемо публічну веб-версію каналу, збираємо пости (data-post="<канал>/<id>").
  2. last_id беремо з Mongo: колекція `channel_monitor_state` (_id = назва каналу)
     в тій самій БД (MONGO_DB), що й решта модулів. Чужі колекції НЕ чіпаємо.
  3. Перший запуск — лише запам'ятовуємо найбільший id, нічого не шлемо.
  4. Усі id > last_id (за зростанням): беремо текст, шлемо в канал і юзерам бота,
     і тільки потім двигаємо last_id (якщо канал не прийняв — зупиняємось,
     наступний запуск повторить).

Пересилаємо ТІЛЬКИ пости, текст яких містить одну з фраз із RELEVANT_PHRASES
(порівняння нечутливе до регістру, емодзі, розділових знаків і зайвих пробілів).
Усе інше (включно з репостами, службовими подіями, постами без тексту) ігнорується.

Env:
  MONGO_URI, MONGO_DB          — як у решти модулів
  TELEGRAM_BOT_TOKEN           — бот, що пише в канал
  TELEGRAM_CHANNEL_ID          — канал-приймач
  ZTOE_BOT_TOKEN               — бот для DM підписникам (+ логи)
  TELEGRAM_LOG_CHANNEL_ID      — лог-група (опційно)
  ZTOE_CHANNEL_URL             — https://t.me/s/<канал> (обов'язково, лише в секретах)
"""

from __future__ import annotations

import html
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from bot_notify import is_quiet_now, load_notify_users
from log_utils import log_to_buffer, send_log_to_channel
from mongo_store import get_db
from telegram_handler import send_photos_sync


def _channel_from_url(url: str) -> str:
    m = re.match(r"^https?://t\.me/(?:s/)?([A-Za-z0-9_]+)/?(?:\d+/?)?(?:\?.*)?$", url.strip())
    if not m:
        raise RuntimeError("ZTOE_CHANNEL_URL має вигляд https://t.me/s/<канал>")
    return m.group(1)


CHANNEL_URL = (os.getenv("ZTOE_CHANNEL_URL") or "").strip()
if not CHANNEL_URL:
    raise RuntimeError("Немає секрету ZTOE_CHANNEL_URL")
SOURCE_CHANNEL = _channel_from_url(CHANNEL_URL)
PAGE_URL = f"https://t.me/s/{SOURCE_CHANNEL}"
STATE_COLLECTION = "channel_monitor_state"
NOTIFY_KIND = "ztoe_news"
TEXT_LIMIT = 3500

# ID гілки (topic) в групі, куди відправляти повідомлення
MESSAGE_THREAD_ID = 12135

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

RELEVANT_PHRASES = (
    "Увага! Аварійні відключення!",
    "⚡️ Аварійні відключення на Житомирщині скасовано.",
    "на Житомирщині аварійні знеструмлення",
)

# ---------- парсинг ----------

def _post_text(el) -> str:
    for t in el.select(".tgme_widget_message_text"):
        if t.find_parent(class_="tgme_widget_message_reply"):
            continue
        for br in t.find_all("br"):
            br.replace_with("\n")
        txt = t.get_text()
        txt = re.sub(r"[ \t]+\n", "\n", txt)
        txt = re.sub(r"\n{3,}", "\n\n", txt)
        return txt.strip()
    return ""


def parse_posts(page: str) -> Dict[int, dict]:
    soup = BeautifulSoup(page, "html.parser")
    pat = re.compile(rf"^{re.escape(SOURCE_CHANNEL)}/(\d+)$", re.IGNORECASE)
    out: Dict[int, dict] = {}
    for el in soup.select("div.tgme_widget_message[data-post]"):
        m = pat.match(el.get("data-post", ""))
        if not m:
            continue
        classes = " ".join(el.get("class", []))
        wrap = el.find_parent(class_="tgme_widget_message_wrap")
        wrap_classes = " ".join(wrap.get("class", [])) if wrap else ""
        out[int(m.group(1))] = {
            "text": _post_text(el),
            "forwarded": bool(el.select_one(".tgme_widget_message_forwarded_from")),
            "service": "service" in classes or "service" in wrap_classes,
        }
    return out


def fetch_posts() -> Dict[int, dict]:
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            r = requests.get(PAGE_URL, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            posts = parse_posts(r.text)
            if not posts:
                raise RuntimeError("На сторінці не знайдено жодного поста (змінилась верстка?)")
            return posts
        except Exception as e:
            last_err = e
            log_to_buffer(f"⚠️ Запит до {PAGE_URL} (спроба {attempt + 1}/3): {e}")
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Не вдалось отримати {PAGE_URL}: {last_err}")


# ---------- фільтр ----------

def _norm(text: str) -> str:
    return "".join(ch for ch in (text or "").casefold() if ch.isalnum())


_NORMALIZED_PHRASES = tuple(_norm(p) for p in RELEVANT_PHRASES)


def is_relevant(text: str) -> bool:
    t = _norm(text)
    return bool(t) and any(ph and ph in t for ph in _NORMALIZED_PHRASES)


# ---------- відправка ----------

def format_message(post: dict) -> str:
    text = post["text"]
    if len(text) > TEXT_LIMIT:
        text = text[:TEXT_LIMIT].rstrip() + "…"
    return f"<b>ЖОЕ інформує -</b>\n\n{html.escape(text)}"


def send_to_channel(message: str) -> bool:
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not chat_id or not token:
        log_to_buffer("❌ Немає TELEGRAM_CHANNEL_ID / TELEGRAM_BOT_TOKEN")
        return False
    return send_photos_sync(
        chat_id,
        [],
        message,
        bot_token=token,
        message_thread_id=MESSAGE_THREAD_ID,
    )


def send_to_users(message: str) -> Tuple[int, int, int]:
    """Повертає (sent, failed, total_users)."""
    token = os.getenv("ZTOE_BOT_TOKEN")
    if not token:
        log_to_buffer("⚠️ Немає ZTOE_BOT_TOKEN — DM не шлю")
        return 0, 0, 0
    users = [u for u in load_notify_users(NOTIFY_KIND) if u.get("telegram_id") is not None]
    if not users:
        log_to_buffer("ℹ️ Немає підписників бота для DM")
        return 0, 0, 0
    sent = failed = 0
    for u in users:
        silent = is_quiet_now(u)
        ok = send_photos_sync(
            int(u["telegram_id"]), [], message, bot_token=token, disable_notification=silent
        )
        if ok:
            sent += 1
        else:
            failed += 1
        time.sleep(0.05)
    log_to_buffer(f"📬 DM підписникам: sent={sent} failed={failed} users={len(users)}")
    return sent, failed, len(users)


# ---------- стан ----------

def save_last_id(col, last_id: int) -> None:
    col.update_one(
        {"_id": SOURCE_CHANNEL},
        {"$set": {"last_id": int(last_id), "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ---------- main ----------

def run() -> None:
    """Завжди пише детальний лог. Лог у гілку відправляється з main()."""
    col = get_db()[STATE_COLLECTION]
    posts = fetch_posts()
    ids: List[int] = sorted(posts)
    state = col.find_one({"_id": SOURCE_CHANNEL})

    page_max = ids[-1] if ids else 0
    log_to_buffer(f"📥 Сторінка: {len(ids)} постів, max_id={page_max}")

    if not state or state.get("last_id") is None:
        save_last_id(col, page_max)
        log_to_buffer(f"🆕 Перший запуск: last_id встановлено = {page_max} (нічого не шлю)")
        log_to_buffer("📌 Підсумок: ініціалізація стану, сповіщень немає")
        return

    last = int(state["last_id"])
    new_ids = [i for i in ids if i > last]

    if not new_ids:
        log_to_buffer(f"ℹ️ Нових постів немає (last_id={last}, на сторінці до {page_max})")
        log_to_buffer("📌 Підсумок: прохід без змін, нічого не розсилали")
        return

    log_to_buffer(f"🔔 Нових постів: {len(new_ids)} ({new_ids[0]}…{new_ids[-1]}), last_id={last}")

    sent_to_channel = 0
    skipped = 0
    total_dm_sent = 0
    total_dm_failed = 0
    total_dm_users = 0

    for pid in new_ids:
        p = posts[pid]
        reason = (
            "службова подія" if p["service"]
            else "репост" if p["forwarded"]
            else "не підходить під фільтр" if not is_relevant(p["text"])
            else ""
        )
        if reason:
            log_to_buffer(f"⏭ #{pid} пропущено: {reason} → last_id оновлено в Mongo")
            save_last_id(col, pid)
            skipped += 1
            continue

        message = format_message(p)
        if not send_to_channel(message):
            log_to_buffer(
                f"❌ #{pid}: канал не прийняв, last_id НЕ рухаю — повторю наступного запуску"
            )
            log_to_buffer(
                f"📌 Підсумок (перервано): відправлено в канал={sent_to_channel}, "
                f"пропущено={skipped}, DM sent={total_dm_sent} failed={total_dm_failed}"
            )
            return

        sent_to_channel += 1
        log_to_buffer(f"✅ #{pid} відправлено в канал ({len(p['text'])} симв.)")
        s, f, u = send_to_users(message)
        total_dm_sent += s
        total_dm_failed += f
        total_dm_users = u
        save_last_id(col, pid)
        log_to_buffer(f"💾 #{pid}: last_id оновлено в Mongo")

    if sent_to_channel == 0 and skipped > 0:
        log_to_buffer(
            f"📌 Підсумок: нових постів {len(new_ids)}, усі пропущені "
            f"(не під фільтр / репост / службові). Пропущено={skipped}. "
            f"last_id оновлено в Mongo. Нічого не розсилали."
        )
    elif sent_to_channel > 0:
        log_to_buffer(
            f"📌 Підсумок: нових постів {len(new_ids)} → "
            f"відправлено в канал={sent_to_channel}, пропущено={skipped}, "
            f"DM: sent={total_dm_sent} failed={total_dm_failed} (підписників={total_dm_users})"
        )
    else:
        log_to_buffer(f"📌 Підсумок: нових постів {len(new_ids)}, обробка завершена")


def main() -> int:
    code = 0
    try:
        run()
    except Exception as e:
        code = 1
        log_to_buffer(f"❌ Критична помилка ZTOE channel monitor: {e}")
    finally:
        # Завжди відправляємо лог у гілку
        send_log_to_channel("📊 ЛОГ ВИКОНАННЯ СКРИПТА · ЖОЕ КАНАЛ")
    return code


if __name__ == "__main__":
    sys.exit(main())
