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

При відправці релевантного поста додається картинка assets/channel.jpg
(повідомлення йде як caption до фото — одне повідомлення).

Повідомлення (і в канал, і в DM) містить внизу відступ, рядок посилань
«🔗 Сайт "ЖОЕ" | ⚡️ ПІДПИСАТИСЯ» та час/дату поста «🕐 ГГ:ХХ ДД.ММ» (за Києвом,
час публікації поста в джерельному каналі; якщо його не вдалось прочитати — поточний).

Лог проходу:
- якщо була активність (нові пости / ініціалізація / помилки) → гілка 12135
- якщо просто перевірка без нових постів → гілка 13196
Лог-група: TELEGRAM_LOG_CHANNEL_ID

Env:
  MONGO_URI, MONGO_DB       — як у решти модулів
  TELEGRAM_BOT_TOKEN        — бот, що пише в канал
  TELEGRAM_CHANNEL_ID       — канал-приймач
  ZTOE_BOT_TOKEN            — бот для DM підписникам (+ логи)
  TELEGRAM_LOG_CHANNEL_ID   — лог-група
  ZTOE_CHANNEL_URL          — https://t.me/s/<канал> (обов'язково, лише в секретах)
  URL                       — посилання для «🔗 Сайт "ЖОЕ"» у футері
  SUBSCRIBE                 — посилання для «⚡️ ПІДПИСАТИСЯ» у футері
                              (якщо будь-яке з двох не задано — рядка посилань не буде,
                              час «🕐» додається завжди)
"""
from __future__ import annotations

import html
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from bot_notify import is_quiet_now, load_notify_users
from log_utils import log_to_buffer as _log_to_buffer
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

SOURCE_CHANNEL = _channel_from_url(CHANNEL_URL)  # назва каналу для data-post і посилань на пости
PAGE_URL = f"https://t.me/s/{SOURCE_CHANNEL}"

# Ім'я каналу для посилань у лозі (секрет ZTOE_CHANNEL_NAME, лише ім'я без URL).
# Посилання вигляду https://t.me/<канал>/<id> (без /s/) відкривається прямо в застосунку Telegram.
# Якщо секрету немає — беремо ім'я з ZTOE_CHANNEL_URL.
LINK_CHANNEL = (os.getenv("ZTOE_CHANNEL_NAME") or "").strip().lstrip("@") or SOURCE_CHANNEL

def _site_root(url: str) -> str:
    """https://домен/сторінка?x=1 → https://домен (без шляху, параметрів і якоря)."""
    u = (url or "").strip()
    if not u:
        return ""
    if "://" not in u:
        u = "https://" + u
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}" if p.netloc else ""


# Посилання для футера повідомлень (канал і DM); URL обрізається до кореня сайту
SITE_URL = _site_root(os.environ.get("URL") or "")
SUBSCRIBE_URL = (os.environ.get("SUBSCRIBE") or "").strip()

STATE_COLLECTION = "channel_monitor_state"

# kind не входить у PREF_DEFAULTS бота → user_wants() віддає True кожному з notify=true.
# Якщо колись додасте окремий перемикач у боті — просто додайте ключ у PREF_DEFAULTS.
NOTIFY_KIND = "ztoe_news"

TEXT_LIMIT = 3500  # запас до 4096 під шапку/посилання

# Гілки (topics) у лог-групі
LOG_THREAD_ID_ACTIVITY = 12135  # були нові пости / ініціалізація / помилки
LOG_THREAD_ID_IDLE = 13196      # просто перевірка без нових постів

# Ліміт Telegram — 4096 символів на повідомлення (після HTML-екранування); з запасом 4000.
# Якщо лог довший — він відправляється файлом .txt
LOG_MESSAGE_LIMIT = 4000

# Картинка, яка додається до кожного релевантного повідомлення
_ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
CHANNEL_PHOTO = os.path.join(_ASSETS_DIR, "channel.jpg")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Пост пересилається, лише якщо містить одну з цих фраз.
# Додати нову фразу = дописати рядок у список.
RELEVANT_PHRASES = (
    "Увага! Аварійні відключення!",
    "⚡️ Аварійні відключення на Житомирщині скасовано.",
    "на Житомирщині аварійні знеструмлення",
)

# ---------- лог ----------
_LOG_LINES: List[str] = []

# Службові маркери: текст між ними в лозі загортається в розкриту цитату (<blockquote expandable>)
_Q_OPEN = "\x01"
_Q_CLOSE = "\x02"

# Службові маркери для клікабельного посилання: _L_OPEN <url> _L_SEP <текст> _L_CLOSE
_L_OPEN = "\x03"
_L_SEP = "\x04"
_L_CLOSE = "\x05"
_LINK_RE = re.compile(f"{_L_OPEN}(.*?){_L_SEP}(.*?){_L_CLOSE}", re.DOTALL)


def _link(url: str, text: str) -> str:
    """Повертає маркований фрагмент, який send_log_to_thread перетворить на <a href>."""
    url = url.replace(_L_OPEN, "").replace(_L_SEP, "").replace(_L_CLOSE, "")
    text = text.replace(_L_OPEN, "").replace(_L_SEP, "").replace(_L_CLOSE, "")
    return f"{_L_OPEN}{url}{_L_SEP}{text}{_L_CLOSE}"


def log_to_buffer(msg: str) -> None:
    """Пише і в спільний буфер log_utils, і в локальний (для відправки в гілку)."""
    _LOG_LINES.append(msg)
    # У спільний буфер (GitHub Actions тощо) — лише текст посилання, без URL,
    # щоб назва каналу не світилась у відкритих логах
    _log_to_buffer(_LINK_RE.sub(r"\2", msg))


def send_log_to_thread(title: str, message_thread_id: int) -> None:
    """Відправляє накопичений лог у лог-групу в зазначену гілку."""
    chat_id = os.getenv("TELEGRAM_LOG_CHANNEL_ID")
    token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not chat_id or not token:
        print("Лог у гілку не відправлено: немає TELEGRAM_LOG_CHANNEL_ID / токена бота")
        return
    if not _LOG_LINES:
        return

    raw = "\n".join(_LOG_LINES)

    # для .txt без розмітки: «текст (url)»
    plain = _LINK_RE.sub(r"\2 (\1)", raw).replace(_Q_OPEN, "").replace(_Q_CLOSE, "")

    body_html = (
        html.escape(raw)
        .replace(_Q_CLOSE + "\n", "</blockquote>")
        .replace(_Q_CLOSE, "</blockquote>")
        .replace(_Q_OPEN, "<blockquote expandable>")
    )
    # html.escape вже екранував лапки та & в URL — безпечно для href
    body_html = _LINK_RE.sub(r'<a href="\1">\2</a>', body_html)

    text = f"<b>{html.escape(title)}</b>\n\n{body_html}"

    base = f"https://api.telegram.org/bot{token}"
    try:
        if len(text) <= LOG_MESSAGE_LIMIT:
            r = requests.post(
                f"{base}/sendMessage",
                data={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                    "message_thread_id": message_thread_id,
                },
                timeout=20,
            )
        else:
            # Лог завеликий для одного повідомлення → шлемо повний лог файлом .txt
            filename = f"ztoe_channel_log_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.txt"
            r = requests.post(
                f"{base}/sendDocument",
                data={
                    "chat_id": chat_id,
                    "caption": f"<b>{html.escape(title)}</b>\n(лог завеликий — файлом)",
                    "parse_mode": "HTML",
                    "message_thread_id": message_thread_id,
                },
                files={"document": (filename, plain.encode("utf-8"), "text/plain")},
                timeout=30,
            )
        if not (r.status_code == 200 and r.json().get("ok")):
            print(f"Лог у гілку не відправлено: {r.status_code} {r.text[:300]}")
    except Exception as e:  # noqa: BLE001
        print(f"Лог у гілку: виняток {e}")


# ---------- парсинг ----------
def _post_text(el) -> str:
    """Текст самого поста (не прев'ю відповіді/репосту)."""
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


def _kyiv_tz():
    for name in ("Europe/Kyiv", "Europe/Kiev"):
        try:
            return ZoneInfo(name)
        except Exception:  # noqa: BLE001
            continue
    return timezone.utc


KYIV_TZ = _kyiv_tz()


def _post_dt(el) -> Optional[datetime]:
    """Час публікації поста з веб-версії каналу (<time datetime="...">), або None."""
    t = el.select_one("a.tgme_widget_message_date time[datetime]") or el.select_one("time[datetime]")
    if not t:
        return None
    try:
        return datetime.fromisoformat(t["datetime"].replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_posts(page: str) -> Dict[int, dict]:
    """{id: {text, forwarded, service, dt}} для всіх постів на сторінці."""
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
            "dt": _post_dt(el),
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
        except Exception as e:  # noqa: BLE001
            last_err = e
            # Не логуємо PAGE_URL, щоб секрет не світився в GitHub Actions
            log_to_buffer(f"⚠️ Запит до каналу (спроба {attempt + 1}/3): {e}")
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Не вдалось отримати сторінку каналу: {last_err}")


# ---------- фільтр ----------
def _norm(text: str) -> str:
    """Нижній регістр; лишаємо тільки літери й цифри (емодзі, пунктуація, пробіли, \u200b геть)."""
    return "".join(ch for ch in (text or "").casefold() if ch.isalnum())


_NORMALIZED_PHRASES = tuple(_norm(p) for p in RELEVANT_PHRASES)


def is_relevant(text: str) -> bool:
    t = _norm(text)
    return bool(t) and any(ph and ph in t for ph in _NORMALIZED_PHRASES)


# ---------- відправка ----------
def build_footer(post_dt: Optional[datetime]) -> str:
    """
    Футер для всіх повідомлень (канал і DM): відступ + рядок посилань + «🕐 ГГ:ХХ ДД.ММ».
    Якщо URL / SUBSCRIBE не задані — посилань не буде, час усе одно додається.
    """
    parts: List[str] = []
    parts.append("")  # Додатковий відступ
    # Посилання
    if SITE_URL and SUBSCRIBE_URL:
        parts.append(
            f'<a href="{html.escape(SITE_URL, quote=True)}">🔗 Сайт "ЖОЕ"</a> | '
            f'<a href="{html.escape(SUBSCRIBE_URL, quote=True)}">⚡️ ПІДПИСАТИСЯ</a>'
        )
    # Час і дата поста (за Києвом)
    dt = post_dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    parts.append(f"🕐 {dt.astimezone(KYIV_TZ):%H:%M %d.%m}")
    return "\n" + "\n".join(parts)


def format_message(post: dict) -> str:
    """Повідомлення і для каналу, і для DM: текст + відступ + посилання + час."""
    text = post["text"]
    if len(text) > TEXT_LIMIT:
        text = text[:TEXT_LIMIT].rstrip() + "…"
    return f"<b>ЖОЕ інформує -</b>\n\n{html.escape(text)}" + build_footer(post.get("dt"))


def _get_photo_list() -> List[str]:
    """Повертає список з шляхом до картинки, якщо файл існує."""
    if os.path.isfile(CHANNEL_PHOTO):
        return [CHANNEL_PHOTO]
    log_to_buffer(f"⚠️ Картинка не знайдена: {CHANNEL_PHOTO} — відправляю без фото")
    return []


def send_to_channel(message: str) -> bool:
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not chat_id or not token:
        log_to_buffer("❌ Немає TELEGRAM_CHANNEL_ID / TELEGRAM_BOT_TOKEN")
        return False
    photos = _get_photo_list()
    return send_photos_sync(chat_id, photos, message, bot_token=token)


def send_to_users(message: str) -> Tuple[int, int, int]:
    """
    Повертає (sent, failed, total_users).
    """
    token = os.getenv("ZTOE_BOT_TOKEN")
    if not token:
        log_to_buffer("⚠️ Немає ZTOE_BOT_TOKEN — DM не шлю")
        return 0, 0, 0

    users = [u for u in load_notify_users(NOTIFY_KIND) if u.get("telegram_id") is not None]
    if not users:
        log_to_buffer("ℹ️ Немає підписників бота для DM")
        return 0, 0, 0

    photos = _get_photo_list()
    sent = failed = 0
    for u in users:
        silent = is_quiet_now(u)
        ok = send_photos_sync(
            int(u["telegram_id"]),
            photos,
            message,
            bot_token=token,
            disable_notification=silent,
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
def run() -> bool:
    """
    Повертає True, якщо була активність (нові пости, ініціалізація, помилки тощо).
    False — якщо просто перевірка без нових постів.
    """
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
        return True  # ініціалізація вважаємо активністю

    last = int(state["last_id"])
    new_ids = [i for i in ids if i > last]
    if not new_ids:
        log_to_buffer(f"ℹ️ Нових постів немає (last_id={last}, на сторінці до {page_max})")
        log_to_buffer("📌 Підсумок: прохід без змін, нічого не розсилали")
        return False  # ← немає нових постів

    log_to_buffer(f"🔔 Нових постів: {len(new_ids)} ({new_ids[0]}…{new_ids[-1]}), last_id={last}")

    if not SITE_URL or not SUBSCRIBE_URL:
        log_to_buffer("⚠️ Не задано URL / SUBSCRIBE — посилання у футері не додаються")

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
            # У Telegram-лог-групі причина пропуску — клікабельне посилання на пост.
            # Без /s/ → відкривається в застосунку Telegram, а не в браузері.
            post_url = f"https://t.me/{LINK_CHANNEL}/{pid}"
            reason_link = _link(post_url, reason)

            if reason == "не підходить під фільтр":
                snippet = " ".join((p["text"] or "").split())  # в один рядок
                if not snippet:
                    snippet = "(без тексту)"
                elif len(snippet) > 200:
                    snippet = snippet[:200].rstrip() + "…"
                snippet = snippet.replace(_Q_OPEN, "").replace(_Q_CLOSE, "")
                log_to_buffer(
                    f"⏭ #{pid} пропущено: {reason_link}\n"
                    f"{_Q_OPEN}«{snippet}»{_Q_CLOSE}"
                )
            else:
                log_to_buffer(f"⏭ #{pid} пропущено: {reason_link}")

            save_last_id(col, pid)
            skipped += 1
            continue

        message = format_message(p)  # однаковий для каналу і DM: посилання + час внизу

        if not send_to_channel(message):
            log_to_buffer(
                f"❌ #{pid}: канал не прийняв, last_id НЕ рухаю — повторю наступного запуску"
            )
            log_to_buffer(
                f"📌 Підсумок (перервано): відправлено в канал={sent_to_channel}, "
                f"пропущено={skipped}, DM sent={total_dm_sent} failed={total_dm_failed}"
            )
            return True

        sent_to_channel += 1
        log_to_buffer(f"✅ #{pid} відправлено в канал ({len(p['text'])} симв.) + фото")

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

    return True  # була активність


def main() -> int:
    code = 0
    had_activity = False
    try:
        had_activity = run()
    except Exception as e:  # noqa: BLE001
        code = 1
        had_activity = True
        log_to_buffer(f"❌ Критична помилка ZTOE channel monitor: {e}")
    finally:
        # Розділення логів:
        # - якщо були нові пости / активність → гілка 12135
        # - якщо просто перевірка без нових постів → гілка 13196
        thread_id = LOG_THREAD_ID_ACTIVITY if had_activity else LOG_THREAD_ID_IDLE
        send_log_to_thread(
            "📊 ЛОГ ВИКОНАННЯ СКРИПТА · ЖОЕ КАНАЛ",
            message_thread_id=thread_id,
        )
    return code


if __name__ == "__main__":
    sys.exit(main())
