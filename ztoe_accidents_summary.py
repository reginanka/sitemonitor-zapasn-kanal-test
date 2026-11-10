#!/usr/bin/env python3
"""
Звіт про аварійні відключення в Житомирській області (ЖОЕ).

- Тягне всі записи з /pw_accidents
- Рахує унікальні вулиці / нас. пункти по РЕМ
- Сортує РЕМ за відсотком уражених вулиць у районі (а не лише за кількістю)
- Під кожним РЕМ — топ населених пунктів: % РЕМ і % вимкнення самого н.п.
- У рядку РЕМ — % району: яка частка вулиць району зараз без світла
  (знаменник з /pw_streets; село в чужій картці РЕМ не потребує винятку)
- Запис без РЕМ («АТ Житомиробленерго») клеїться до РЕМ цього ж city id,
  а не в окреме «Інше» (Нова Борова → Хорошівський)
- Завжди зберігає знімок у Mongo (accidents_summary: latest + історія по годині)
- У Telegram-канал шле ЛИШЕ при рівні CRITICAL
  (антиспам: last_notified_date / last_notified_level у Mongo)
- FORCE_NOTIFY=1 — примусово в канал
- Лог у TELEGRAM_LOG_CHANNEL_ID

Рівні (за кількістю н.п.):
  NORM <30 | WARN 30–150 | HIGH 150–400 | CRITICAL >400
"""

from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Set, Tuple

import httpx

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:
    import pytz
    KYIV = pytz.timezone("Europe/Kyiv")


# Telegram HTML message limit; keep margin for safety
TG_MSG_LIMIT = 4000


def split_by_rem_blocks(text: str, limit: int = TG_MSG_LIMIT) -> List[str]:
    """
    Ріже довге зведення по межах блоків РЕМ (emoji + <b>Назва</b>).
    Повертає 1–N частин ≤ limit символів. Header — у першій, footer — в останній.
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    lines = text.split("\n")
    # рядок початку РЕМ: 🔴/🟠/🟡/🟢/⚪ + пробіл + <b>
    rem_start = re.compile(
        r"^[\s]*(?:🔴|🟠|🟡|🟢|⚪)\s*<b>[^<]+</b>\s*[—\-]\s*",
    )

    header: List[str] = []
    blocks: List[List[str]] = []  # кожен блок = список рядків одного РЕМ
    footer: List[str] = []
    current: List[str] | None = None
    in_rem_zone = False
    past_blockquote = False

    for line in lines:
        if rem_start.match(line):
            in_rem_zone = True
            if current is not None:
                blocks.append(current)
            current = [line]
            continue
        if current is not None:
            # продовження блоку РЕМ (міста, порожні, закриття blockquote)
            if line.strip() == "</blockquote>":
                current.append(line)
                blocks.append(current)
                current = None
                past_blockquote = True
                continue
            # якщо після blockquote пішли footer-рядки — не чіпаємо current
            if past_blockquote:
                footer.append(line)
            else:
                current.append(line)
            continue
        if in_rem_zone or past_blockquote:
            footer.append(line)
        else:
            header.append(line)

    if current is not None:
        blocks.append(current)

    # якщо не знайшли жодного РЕМ-блоку — грубий split
    if not blocks:
        parts = []
        rest = text
        while rest:
            if len(rest) <= limit:
                parts.append(rest)
                break
            cut = rest.rfind("\n", 0, limit)
            if cut < limit // 2:
                cut = limit
            parts.append(rest[:cut].rstrip())
            rest = rest[cut:].lstrip("\n")
        return parts or [text[:limit]]

    header_s = "\n".join(header).rstrip()
    footer_s = "\n".join(footer).strip()
    # відкриваємо blockquote лише якщо header його ще не містить
    open_bq = "<blockquote expandable>"
    close_bq = "</blockquote>"
    header_has_bq = open_bq in header_s

    parts: List[str] = []
    # перша частина: header + скільки влізе РЕМ
    buf_lines: List[str] = []
    if header_s:
        buf_lines.extend(header_s.split("\n"))
    if not header_has_bq and blocks:
        # header зазвичай закінчується рядком «Ситуація по РЕМ», blockquote додаємо
        if open_bq not in "\n".join(buf_lines):
            buf_lines.append(open_bq)

    def _flush(is_last: bool, extra_footer: str = "") -> None:
        nonlocal buf_lines
        body = "\n".join(buf_lines).rstrip()
        # закрити blockquote якщо відкритий і ще не закритий
        if open_bq in body and close_bq not in body:
            body = body + "\n" + close_bq
        if is_last and extra_footer:
            body = body + ("\n\n" if body else "") + extra_footer
        if body:
            parts.append(body)
        buf_lines = []

    for bi, block in enumerate(blocks):
        # прибрати зайві </blockquote> зсередини блоку (ми закриємо самі)
        clean = [ln for ln in block if ln.strip() != close_bq]
        candidate_lines = buf_lines + clean
        candidate = "\n".join(candidate_lines)
        # запас на закриття blockquote + footer на останній
        reserve = 20
        is_last_block = bi == len(blocks) - 1
        if is_last_block and footer_s:
            reserve += len(footer_s) + 4
        if len(candidate) + reserve <= limit or not buf_lines or (
            # header-only ще порожній від РЕМ — все одно кладемо
            len(buf_lines) <= (len(header_s.split("\n")) + 2) and not any(
                rem_start.match(x) for x in buf_lines
            )
        ):
            buf_lines = candidate_lines
            continue
        # не влазить — flush поточний (без цього блоку) і почни новий
        _flush(is_last=False)
        # нова частина: короткий заголовок-продовження + blockquote
        buf_lines = [
            "⚠️ <b>Ситуація по РЕМ</b> (продовження):",
            open_bq,
        ] + clean

    _flush(is_last=True, extra_footer=footer_s)

    # safety: якщо щось все одно > limit — грубий доріз
    fixed: List[str] = []
    for p in parts:
        if len(p) <= limit:
            fixed.append(p)
            continue
        rest = p
        while rest:
            if len(rest) <= limit:
                fixed.append(rest)
                break
            cut = rest.rfind("\n", 0, limit)
            if cut < limit // 2:
                cut = limit
            fixed.append(rest[:cut].rstrip())
            rest = rest[cut:].lstrip("\n")
    return fixed or [text[:limit]]


def send_notification(message: str, image_path=None) -> bool:
    """Шле в TELEGRAM_CHANNEL_ID без preview лінків.
    Якщо текст довший за ліміт — ріже по межах РЕМ і шле 2–3 повідомлення підряд.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID") or ""
    if not token or not chat_id:
        print("❌ TELEGRAM_BOT_TOKEN або TELEGRAM_CHANNEL_ID не налаштовані")
        return False
    parts = split_by_rem_blocks(message, TG_MSG_LIMIT)
    if not parts:
        return False
    ok_all = True
    for i, part in enumerate(parts):
        try:
            r = httpx.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={
                    "chat_id": chat_id,
                    "text": part[:4096],
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
                timeout=30,
            )
            if r.status_code == 200:
                if len(parts) > 1:
                    print(f"✓ канал part {i+1}/{len(parts)} ({len(part)} символів)")
            else:
                print(f"❌ Telegram send HTTP {r.status_code}: {r.text[:300]}")
                ok_all = False
        except Exception as e:
            print(f"❌ Telegram send failed: {e}")
            ok_all = False
    return ok_all


def _user_wants_region(u: dict) -> bool:
    """notify_prefs.region (за замовч. True, якщо ключа немає)."""
    if u.get("notify", True) is False:
        return False
    raw = u.get("notify_prefs")
    if isinstance(raw, dict):
        return bool(raw.get("region", True))
    return True


def _is_quiet_now(u: dict) -> bool:
    """Тихий режим з хвилинами (як у bot_notify / ZTOE-боті)."""
    if not u.get("quiet_mode"):
        return False
    try:
        fh = int(u.get("quiet_from", 23)) % 24
        th = int(u.get("quiet_to", 7)) % 24
    except (TypeError, ValueError):
        fh, th = 23, 7
    try:
        fm = max(0, min(59, int(u.get("quiet_from_min", 0) or 0)))
        tm = max(0, min(59, int(u.get("quiet_to_min", 0) or 0)))
    except (TypeError, ValueError):
        fm, tm = 0, 0
    now = datetime.now(KYIV)
    cur = now.hour * 60 + now.minute
    start = fh * 60 + fm
    end = th * 60 + tm
    if start == end:
        return True
    if start < end:
        return start <= cur < end
    return cur >= start or cur < end


def send_region_dms(message: str) -> tuple[int, int]:
    """
    Особисті DM підписникам з notify_prefs.region=true.
    Повертає (sent, failed).
    """
    token = (os.getenv("ZTOE_BOT_TOKEN") or "").strip()
    if not token:
        log_to_buffer("⚠️ ZTOE_BOT_TOKEN немає — DM region пропуск")
        return 0, 0

    try:
        from pymongo import MongoClient
    except Exception as e:
        log_to_buffer(f"❌ pymongo для DM: {e}")
        return 0, 0

    uri = (os.getenv("MONGO_URI") or "").strip()
    db_name = (os.getenv("MONGO_DB") or "").strip()
    if not uri or not db_name:
        log_to_buffer("⚠️ Mongo URI/DB немає — DM region пропуск")
        return 0, 0

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        users = list(client[db_name]["users"].find({"notify": {"$ne": False}}))
    except Exception as e:
        log_to_buffer(f"❌ Mongo users для DM: {e}")
        return 0, 0

    targets = [u for u in users if _user_wants_region(u) and u.get("telegram_id")]
    log_to_buffer(f"📬 region DM candidates: {len(targets)} (з {len(users)} notify)")

    parts = split_by_rem_blocks(message, TG_MSG_LIMIT)
    if not parts:
        log_to_buffer("⚠️ region DM: порожнє повідомлення після split")
        return 0, 0
    if len(parts) > 1:
        log_to_buffer(f"✂️ region DM split: {len(parts)} parts, lens={[len(p) for p in parts]}")

    sent = failed = 0
    sent_quiet = sent_sound = 0
    for u in targets:
        tid = u.get("telegram_id")
        silent = _is_quiet_now(u)
        user_ok = True
        try:
            for part in parts:
                r = httpx.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={
                        "chat_id": str(tid),
                        "text": part[:4096],
                        "parse_mode": "HTML",
                        "disable_web_page_preview": "true",
                        "disable_notification": "true" if silent else "false",
                    },
                    timeout=30,
                )
                if not (r.status_code == 200 and (r.json() or {}).get("ok")):
                    user_ok = False
                    log_to_buffer(
                        f"❌ region DM tid={tid}: HTTP {r.status_code} {r.text[:120]}"
                    )
                    break
            if user_ok:
                sent += 1
                if silent:
                    sent_quiet += 1
                else:
                    sent_sound += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            log_to_buffer(f"❌ region DM tid={tid}: {e}")

    log_to_buffer(f"📬 region DM: sent={sent} failed={failed}")
    log_to_buffer(
        f"📬 region DM: sent={sent} failed={failed} (🔊={sent_sound} 🔇={sent_quiet})"
    )

    return sent, failed


# --- logging buffer (сумісно з log_utils + fallback) ---
_log_lines: list[str] = []

try:
    from log_utils import log_to_buffer as _lu_log, send_log_to_channel as _lu_send
    _HAS_LOG_UTILS = True
except Exception:
    _HAS_LOG_UTILS = False

    def _lu_log(msg: str) -> None:
        pass

    def _lu_send(title: str = "", message_thread_id=None) -> None:
        pass


def log_to_buffer(msg: str) -> None:
    """Пише в консоль + буфер + log_utils (якщо є)."""
    print(msg)
    _log_lines.append(msg)
    try:
        _lu_log(msg)
    except Exception:
        pass


def send_log_to_channel(title: str = "📊 ЛОГ · АВАРІЇ ЖОЕ", message_thread_id: int | None = 154) -> None:
    """
    Шле лог у TELEGRAM_LOG_CHANNEL_ID.
    1) пробує log_utils
    2) fallback на httpx (якщо log_utils/requests злетіли)
    """
    try:
        if _HAS_LOG_UTILS:
            _lu_send(title, message_thread_id=message_thread_id)
    except Exception as e:
        print(f"log_utils send failed: {e}")

    token = os.getenv("ZTOE_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or ""
    if not token or not chat_id or not _log_lines:
        if not chat_id:
            print("⚠️ TELEGRAM_LOG_CHANNEL_ID не задано — лог у канал не піде")
        elif not token:
            print("⚠️ ZTOE_BOT_TOKEN не задано — лог у канал не піде")
        return

    try:
        body = "\n".join(_log_lines)
        text_msg = f"{title}\n\n<pre>{body}</pre>"
        if len(text_msg) > 4000:
            text_msg = text_msg[:3900] + "\n…</pre>"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            }
        if message_thread_id is not None:
            data["message_thread_id"] = message_thread_id
        r = httpx.post(
            url,
            data=data,
            timeout=15,
        )
        if r.status_code == 200:
            print(f"✓ Лог відправлено в {chat_id}")
        else:
            print(f"❌ Лог Telegram HTTP {r.status_code}: {r.text[:300]}")
    except Exception as e:
        print(f"❌ Лог fallback failed: {e}")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_accidents_summary")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or "").rstrip("/")
AVARII_ZHOE = (os.getenv("AVARII_ZHOE") or "").strip()
SUBSCRIBE = (os.getenv("SUBSCRIBE") or "").strip()

# н.п. під РЕМ: показуємо окремо, якщо ≥ CITY_PCT_MIN % вулиць району
CITY_PCT_MIN = 1.0
CITY_LIST_MAX = 20  # захист від занадто довгого списку

CITY_PREFIX = {
    "Житомир", "Звягель", "Бердичів", "Овруч", "Коростень", "Малин",
}

# Покриття району.
# % району — живий знаменник: вулиці /pw_streets, місто віднесене до РЕМ
# за відключеннями (більшість записів), а не за otg картки. Статична таблиця
# нижче — лише якщо довідник не відкрився.
# % н.п. — з довідника addresses_directory (у API pw_cities дублікати, % там кривий).
REM_TOTAL_STREETS: Dict[str, int] = {
    "Житомирський": 2782,
    "Звягельський": 2474,
    "Овруцький": 1670,
    "Попільнянський": 1418,
    "Коростенський": 1154,
    "Малинський": 1126,
    "Олевський": 890,
    "Баранівський": 871,
    "Хорошівський": 785,
    "Радомишльський": 725,
    "Бердичівський": 714,
    "Черняхівський": 703,
    "Чуднівський": 686,
    "Любарський": 543,
    "Пулинський": 524,
    "Романівський": 464,
    "Андрушівський": 433,
    "Коростишівський": 404,
    "Брусилівський": 357,
}

REM_TOTAL_CITIES: Dict[str, int] = {
    "Житомирський": 103,
    "Звягельський": 224,
    "Овруцький": 286,
    "Коростенський": 150,
    "Попільнянський": 101,
    "Бердичівський": 66,
    "Малинський": 110,
    "Баранівський": 60,
    "Олевський": 60,
    "Хорошівський": 78,
    "Радомишльський": 84,
    "Чуднівський": 60,
    "Андрушівський": 41,
    "Коростишівський": 64,
    "Любарський": 46,
    "Черняхівський": 58,
    "Пулинський": 68,
    "Брусилівський": 36,
    "Романівський": 63,
}

# Якщо otg = АТ «Житомиробленерго» (без РЕМ) — підставляємо РЕМ за назвою н.п.
# Зібрано з /pw_cities (найчастіший otg для назви).
OBLAST_TOTAL_STREETS = sum(REM_TOTAL_STREETS.values())
OBLAST_TOTAL_CITIES = sum(REM_TOTAL_CITIES.values())

CITY_TO_REM: Dict[str, str] = {
    "Малин": "Малинський РЕМ",
    "Житомир": "Житомирський РЕМ",
    "Звягель": "Звягельський РЕМ",
    "Новоград-Волинський": "Звягельський РЕМ",
    "Бердичів": "Бердичівський РЕМ",
    "Коростень": "Коростенський РЕМ",
    "Овруч": "Овруцький РЕМ",
    "Олевськ": "Олевський РЕМ",
    "Радомишль": "Радомишльський РЕМ",
    "Коростишів": "Коростишівський РЕМ",
    "Андрушівка": "Андрушівський РЕМ",
    "Баранівка": "Баранівський РЕМ",
    "Чуднів": "Чуднівський РЕМ",
    "Любар": "Любарський РЕМ",
    "Романів": "Романівський РЕМ",
    "Пулини": "Пулинський РЕМ",
    "Черняхів": "Черняхівський РЕМ",
    "Брусилів": "Брусилівський РЕМ",
    "Попільня": "Попільнянський РЕМ",
    "Хорошів": "Хорошівський РЕМ",
    "Головине": "Черняхівський РЕМ",
    "Іршанськ": "Хорошівський РЕМ",
    "Великі Низгірці": "Бердичівський РЕМ",
}


def _norm_rem_name(name: str) -> str:
    """«Звягельський РЕМ» → «Звягельський»."""
    s = (name or "").strip()
    s = s.replace(" РЕМ", "").replace(" рем", "").strip()
    return s


# Живий знаменник вулиць району. None — ще не завантажили, лишається статика.
_REM_STREETS_DYNAMIC: Dict[str, int] | None = None
_OBLAST_STREETS_DYNAMIC: int | None = None
# city id → {сира назва РЕМ: скільки записів}, з усіх відключень, включно з ГПВ.
_CITY_REM_VOTES: Dict[int, Dict[str, int]] = {}
_CITY_REM_NAMES: Dict[int, str] = {}


def _rem_street_total(rem_name: str) -> int | None:
    """Скільки вулиць у районі: живий довідник, інакше статична таблиця."""
    key = _norm_rem_name(rem_name)
    if _REM_STREETS_DYNAMIC is not None:
        dyn = int(_REM_STREETS_DYNAMIC.get(key) or 0)
        if dyn > 0:
            return dyn
    return REM_TOTAL_STREETS.get(key)


def _oblast_street_total() -> int:
    if _OBLAST_STREETS_DYNAMIC and _OBLAST_STREETS_DYNAMIC > 0:
        return int(_OBLAST_STREETS_DYNAMIC)
    if _REM_STREETS_DYNAMIC:
        summed = sum(int(v or 0) for v in _REM_STREETS_DYNAMIC.values())
        if summed > 0:
            return summed
    return OBLAST_TOTAL_STREETS


def rem_streets_pct(rem_name: str, affected_streets: int) -> float | None:
    """
    % району: уражені вулиці / усі вулиці цього району.
    Може бути >100%, якщо API дає більше унікальних ключів, ніж є в довіднику.
    """
    total = _rem_street_total(rem_name)
    if not total or total <= 0:
        return None
    n = max(0, int(affected_streets or 0))
    return 100.0 * n / total


def rem_cities_pct(rem_name: str, affected_cities: int) -> float | None:
    """Відсоток уражених н.п. у РЕМ (надійніший за вулиці)."""
    total = REM_TOTAL_CITIES.get(_norm_rem_name(rem_name))
    if not total or total <= 0:
        return None
    n = max(0, int(affected_cities or 0))
    return min(100.0, 100.0 * n / total)


def rem_coverage_pct(
    rem_name: str, affected_streets: int, affected_cities: int
) -> tuple[float | None, str]:
    """
    Ключ сортування: max(% н.п., % вул.), щоб район з повним покриттям
    н.п. не програвав через розбіжність довідника по вулицях.
    """
    sp = rem_streets_pct(rem_name, affected_streets)
    cp = rem_cities_pct(rem_name, affected_cities)
    candidates = []
    if cp is not None:
        candidates.append((cp, "cities"))
    if sp is not None:
        candidates.append((min(sp, 100.0), "streets"))  # для сорту не роздуваємо >100
    if not candidates:
        return None, "streets"
    candidates.sort(key=lambda x: -x[0])
    return candidates[0]


def _is_oblenergo(otg_name: str) -> bool:
    """АТ «Житомиробленерго» без конкретного РЕМ."""
    s = (otg_name or "").lower()
    return "житомиробленерго" in s and "рем" not in s


def _ref_id(value: Any) -> int | None:
    """id з int, з об'єкта API або з IRI виду /api/pw_cities/214."""
    if isinstance(value, dict):
        value = value.get("id") if value.get("id") is not None else value.get("@id")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value:
        tail = value.rstrip("/").rsplit("/", 1)[-1]
        try:
            return int(tail)
        except ValueError:
            return None
    return None


def _accident_place(accident: dict) -> Tuple[str, str, int | None]:
    """РЕМ (як у зведенні), назва н.п. і city id з аварії."""
    city = accident.get("city") or {}
    otg = accident.get("otg") or {}
    cname = (city.get("name") or "").strip() or "?"
    cid = _ref_id(city)
    oname = (otg.get("name") or "").strip() or "?"
    # АТ «Житомиробленерго» без конкретного РЕМ.
    # Спочатку РЕМ, у якому цей самий city id уже є (Нова Борова → Хорошівський).
    # Список CITY_TO_REM — лише якщо інших записів по селу немає.
    if _is_oblenergo(oname):
        mapped = _rem_from_city_votes(cid)
        if not mapped:
            mapped = CITY_TO_REM.get(cname)
        oname = mapped if mapped else "Інше (обленерго)"
    return oname, cname, cid


def _fetch_collection(client: httpx.Client, path: str) -> List[dict]:
    r = client.get(f"{API_BASE}{path}", params={"pagination": "false"})
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    return list(data.get("hydra:member") or [])


def city_street_totals_from(
    accidents: List[dict], per_city: Dict[int, int]
) -> Dict[Tuple[str, str], int]:
    """
    (нормалізований РЕМ, назва н.п.) → скільки вулиць у довіднику.

    Беремо city id з самої аварії, а не otg з картки міста: у дублів
    (Калинівка, Сколобів, Добринь) картка числиться в іншому РЕМ,
    хоча відключення приходить уже з Хорошівського.
    """
    ids: Dict[Tuple[str, str], Set[int]] = defaultdict(set)
    for accident in accidents:
        oname, cname, cid = _accident_place(accident)
        if not cname or cname == "?" or cid is None:
            continue
        ids[(_norm_rem_name(oname), cname)].add(cid)

    totals: Dict[Tuple[str, str], int] = {}
    for key, cids in ids.items():
        n = sum(int(per_city.get(cid) or 0) for cid in cids)
        if n > 0:
            totals[key] = n
    return totals


def _prefer_real_rem(votes: Dict[str, int]) -> str | None:
    """Більшість записів. Справжній РЕМ важливіший за голе «обленерго»."""
    if not votes:
        return None
    real = [(n, c) for n, c in votes.items() if n and not _is_oblenergo(n)]
    pool = real or [(n, c) for n, c in votes.items() if n]
    if not pool:
        return None
    pool.sort(key=lambda x: (-x[1], x[0]))
    return pool[0][0]


def _rem_from_city_votes(cid: int | None) -> str | None:
    """РЕМ, який уже стоїть у відключеннях цього city id. Не назва з картки."""
    if cid is None:
        return None
    winner = _prefer_real_rem(_CITY_REM_VOTES.get(cid) or {})
    if winner and not _is_oblenergo(winner):
        return winner
    return None


def resolve_city_rem(
    card_otg: str, card_name: str, votes: Dict[str, int] | None
) -> str | None:
    """
    Куди класти вулиці міста в знаменник району.

    Є відключення по цьому city id — РЕМ беремо з них, картку ігноруємо.
    Окремий виняток на село не потрібен: навіть якщо картка в чужому РЕМ,
    відсоток району рахується за тим РЕМ, який стоїть у відключенні.
    Картка лишається лише для сіл, яких зараз немає в жодному відключенні.
    """
    winner = _prefer_real_rem(votes or {})
    if winner and _is_oblenergo(winner):
        winner = CITY_TO_REM.get((card_name or "").strip()) or ""
    if winner:
        return winner
    name = (card_otg or "").strip()
    if not name:
        return None
    if _is_oblenergo(name):
        return CITY_TO_REM.get((card_name or "").strip()) or None
    return name


def rem_street_totals_from(
    per_city: Dict[int, int],
    cards: Dict[int, Tuple[str, str]],
    votes: Dict[int, Dict[str, int]],
) -> Tuple[Dict[str, int], List[Tuple[int, str, str, str]]]:
    """
    Нормалізований РЕМ → усі вулиці району.
    Друге значення — села, чия картка вказує інший РЕМ, ніж відключення:
    (вулиць, назва, РЕМ картки, РЕМ відключення).
    """
    totals: Dict[str, int] = defaultdict(int)
    moves: List[Tuple[int, str, str, str]] = []
    for cid, nstreets in per_city.items():
        nstreets = int(nstreets or 0)
        if nstreets <= 0:
            continue
        cname, cotg = cards.get(cid, ("", ""))
        if not cname:
            cname = _CITY_REM_NAMES.get(cid, "")
        chosen = resolve_city_rem(cotg, cname, votes.get(cid))
        if not chosen:
            continue
        totals[_norm_rem_name(chosen)] += nstreets
        if cotg and not _is_oblenergo(cotg) and _norm_rem_name(cotg) != _norm_rem_name(chosen):
            moves.append((nstreets, cname or str(cid), cotg, chosen))
    moves.sort(key=lambda row: -row[0])
    return dict(totals), moves


def fetch_city_street_totals(accidents: List[dict]) -> Dict[Tuple[str, str], int]:
    """
    Вулиці н.п. з /pw_streets.
    Заодно збирає знаменник «% району»: кожне місто — в один РЕМ,
    за відключеннями, а не за карткою.
    """
    global _REM_STREETS_DYNAMIC, _OBLAST_STREETS_DYNAMIC
    if not API_BASE or not accidents:
        return {}

    headers = {
        "Accept": "application/ld+json",
        "User-Agent": "ztoe-accidents-summary/1.3",
    }
    cities: List[dict] = []
    try:
        with httpx.Client(timeout=180, headers=headers) as client:
            streets = _fetch_collection(client, "/pw_streets")
            try:
                cities = _fetch_collection(client, "/pw_cities")
            except Exception as e:
                log_to_buffer(f"⚠️ Довідник міст недоступний, % району зі статичного: {e}")
    except Exception as e:
        log_to_buffer(f"⚠️ Довідник вулиць н.п. недоступний: {e}")
        return {}

    per_city: Dict[int, int] = defaultdict(int)
    for street in streets:
        cid = _ref_id(street.get("city"))
        if cid is None:
            continue
        per_city[cid] += 1

    totals = city_street_totals_from(accidents, per_city)
    log_to_buffer(
        f"Довідник вулиць н.п.: {len(totals)} (streets={len(streets)})"
    )

    if cities:
        cards: Dict[int, Tuple[str, str]] = {}
        for city in cities:
            cid = _ref_id(city)
            if cid is None:
                continue
            otg = city.get("otg") or {}
            oname = (otg.get("name") or "").strip() if isinstance(otg, dict) else ""
            cards[cid] = ((city.get("name") or "").strip(), oname)
        rem_totals, moves = rem_street_totals_from(per_city, cards, _CITY_REM_VOTES)
        if rem_totals:
            _REM_STREETS_DYNAMIC = rem_totals
            _OBLAST_STREETS_DYNAMIC = sum(int(v or 0) for v in per_city.values())
            moved_streets = sum(row[0] for row in moves)
            log_to_buffer(
                f"Довідник районів: {sum(rem_totals.values())} вул. у {len(rem_totals)} РЕМ "
                f"(усіх вулиць {_OBLAST_STREETS_DYNAMIC}, "
                f"перенесено з чужої картки {moved_streets} вул. / {len(moves)} н.п.)"
            )
            if moves:
                sample = ", ".join(
                    f"{cname}: {_norm_rem_name(src)}→{_norm_rem_name(dst)} ({n})"
                    for n, cname, src, dst in moves[:5]
                )
                log_to_buffer(f"  не той РЕМ у картці, рахуємо за аварією: {sample}")
    return totals


# Рівень по кількості населених пунктів (область ~1400 н.п.)
# 🟢 NORM     < 30
# 🟡 WARN     30–150
# 🟠 HIGH     150–400
# 🔴 CRITICAL > 400
LEVEL_ORDER = {"NORM": 0, "WARN": 1, "HIGH": 2, "CRITICAL": 3}


def level_from_cities(total_cities: int) -> Tuple[str, str, str]:
    """Повертає (code, emoji, label_uk).

    Код (NORM/WARN/HIGH/CRITICAL) — для антиспаму в Mongo.
    label_uk — для повідомлення в канал (варіант A).
    """
    n = int(total_cities or 0)
    if n > 400:
        return "CRITICAL", "🔴", "КРИТИЧНИЙ"
    if n >= 150:
        return "HIGH", "🟠", "СКЛАДНИЙ"
    if n >= 30:
        return "WARN", "🟡", "Погіршення стану"
    return "NORM", "🟢", "Стабільний"


def get_mongo_col():
    """Колекція accidents_summary або None."""
    uri = os.getenv("MONGO_URI")
    if not uri:
        return None
    db_name = os.getenv("MONGO_DB") or "vidkluchennya"
    try:
        from pymongo import MongoClient

        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        return client[db_name]["accidents_summary"]
    except Exception as e:
        log_to_buffer(f"⚠️ Mongo connect failed: {e}")
        return None


def load_latest_summary() -> dict | None:
    col = get_mongo_col()
    if col is None:
        return None
    try:
        return col.find_one({"_id": "latest"})
    except Exception as e:
        log_to_buffer(f"⚠️ Mongo read latest failed: {e}")
        return None


def should_notify_channel(
    level: str,
    prev: dict | None,
    as_of: datetime,
) -> Tuple[bool, str]:
    """
    Mongo — завжди. Канал — без щогодинного спаму.

    1) FORCE_NOTIFY=1/true/yes — завжди слати
    2) Рівень CRITICAL і ескалація (або перше за день) — слати
    3) Всі інші рівні (NORM / WARN / HIGH) — у канал не слати
    """
    force = (os.getenv("FORCE_NOTIFY") or "").strip().lower() in ("1", "true", "yes")
    if force:
        return True, "FORCE_NOTIFY"

    rank = LEVEL_ORDER.get(level, 0)
    today = as_of.strftime("%Y-%m-%d")
    prev = prev or {}

    last_date = str(prev.get("last_notified_date") or "")[:10]
    last_notified_level = prev.get("last_notified_level") or ""
    last_rank = (
        LEVEL_ORDER.get(last_notified_level, -1) if last_notified_level else -1
    )

    # Тільки CRITICAL
    if rank < LEVEL_ORDER["CRITICAL"]:
        return False, f"рівень {level}, у канал не шлемо (тільки CRITICAL)"

    # Ескалація до CRITICAL (навіть якщо вже писали сьогодні)
    if rank > last_rank:
        return True, f"ескалація {last_notified_level or '—'} → {level}"

    # Сьогодні вже є сповіщення — не спамимо
    if last_date == today and last_rank >= 0:
        return False, f"вже сповіщали сьогодні ({last_notified_level}, {last_date})"

    # Перше за день при CRITICAL
    return True, f"перше сповіщення за день, рівень {level}"


def severity_emoji(streets: int = 0, coverage_pct: float | None = None) -> str:
    """
    Колір за % покриття району (як сортування).
    Якщо % немає — fallback на абсолютну кількість вулиць.
    """
    if coverage_pct is not None:
        p = float(coverage_pct)
        if p >= 30:
            return "🔴"
        if p >= 15:
            return "🟠"
        if p >= 5:
            return "🟡"
        if p > 0:
            return "🟢"
        return "⚪"
    n = int(streets or 0)
    if n >= 400:
        return "🔴"
    if n >= 150:
        return "🟠"
    if n >= 50:
        return "🟡"
    if n > 0:
        return "🟢"
    return "⚪"


def city_label(name: str) -> str:
    if name in CITY_PREFIX:
        return f"м. {name}"
    return name



def is_real_accident(doc: dict) -> bool:
    """
    True лише для справжніх аварій / ГАВ / СГАВ.
    ГПВ (графік погодинних) і явні згадки ГПВ у коментарі — відкидаємо.

    Логіка як у bot_notify._classify_outage:
      switchSubReason: 1=ГАВ, 2=ГПВ(ігнор), 3=СГАВ
      typeOff (якщо sub==0): 1=аварійне
    """
    koment = (doc.get("koment") or doc.get("comment") or "").upper()
    if "ГПВ" in koment:
        return False

    # текстовий тип з API / таблиці
    type_name = (
        doc.get("type_name")
        or doc.get("typeName")
        or doc.get("typeOffName")
        or ""
    )
    if isinstance(type_name, str):
        tn = type_name.lower()
        if "погодин" in tn or "гпв" in tn:
            return False

    try:
        sub = int(doc.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        sub = 0
    try:
        t = int(doc.get("typeOff")) if doc.get("typeOff") is not None else -1
    except (TypeError, ValueError):
        t = -1

    if sub == 2:
        return False  # ГПВ
    if sub in (1, 3):
        return True   # ГАВ / СГАВ
    if sub > 0:
        # невідомий sub — не рахуємо як аварію
        return False

    # sub == 0 → дивимось typeOff
    return t == 1


def _note_city_rems(accidents: List[dict]) -> None:
    """city id → РЕМ з усіх записів API, включно з ГПВ. Картку міста не чіпаємо."""
    global _CITY_REM_VOTES, _CITY_REM_NAMES
    votes: Dict[int, Dict[str, int]] = {}
    names: Dict[int, str] = {}
    for accident in accidents:
        city = accident.get("city") or {}
        otg = accident.get("otg") or {}
        cid = _ref_id(city)
        if cid is None:
            continue
        cname = (city.get("name") or "").strip() if isinstance(city, dict) else ""
        oname = (otg.get("name") or "").strip() if isinstance(otg, dict) else ""
        if not oname:
            continue
        bucket = votes.setdefault(cid, {})
        bucket[oname] = bucket.get(oname, 0) + 1
        if cname:
            names[cid] = cname
    _CITY_REM_VOTES = votes
    _CITY_REM_NAMES = names


def fetch_all_accidents() -> List[dict]:

    if not API_BASE:
        raise SystemExit("Немає ZTOE_API_BASE")

    headers = {
        "Accept": "application/ld+json",
        "User-Agent": "ztoe-accidents-summary/1.3",
    }
    items: List[dict] = []

    with httpx.Client(timeout=60, headers=headers) as client:
        try:
            r = client.get(
                f"{API_BASE}/pw_accidents",
                params={"pagination": "false"},
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                log_to_buffer(f"API: list → {len(data)} записів")
                _note_city_rems(data)
                filtered = [x for x in data if is_real_accident(x)]
                log_to_buffer(
                    f"Фільтр аварій: {len(data)} → {len(filtered)} "
                    f"(відкинуто ГПВ/не-аварії: {len(data) - len(filtered)})"
                )
                return filtered
            batch = data.get("hydra:member") or []
            if batch:
                log_to_buffer(f"API: pagination=false → {len(batch)} записів")
                _note_city_rems(batch)
                filtered = [x for x in batch if is_real_accident(x)]
                log_to_buffer(
                    f"Фільтр аварій: {len(batch)} → {len(filtered)} "
                    f"(відкинуто ГПВ/не-аварії: {len(batch) - len(filtered)})"
                )
                return filtered
        except Exception as e:
            log_to_buffer(f"⚠️ pagination=false failed: {e} — сторінки")

        page = 1
        while page <= 100:
            r = client.get(
                f"{API_BASE}/pw_accidents",
                params={"page": page, "itemsPerPage": 100},
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                items.extend(data)
                break
            batch = data.get("hydra:member") or []
            items.extend(batch)
            if not (data.get("hydra:view") or {}).get("hydra:next") or not batch:
                break
            page += 1
        log_to_buffer(f"API: по сторінках → {len(items)} записів (сторінок: {page})")

    _note_city_rems(items)
    raw_n = len(items)
    items = [a for a in items if is_real_accident(a)]
    log_to_buffer(
        f"Фільтр аварій: {raw_n} → {len(items)} "
        f"(відкинуто ГПВ/не-аварії: {raw_n - len(items)})"
    )
    return items


def aggregate(accidents: List[dict]) -> Dict[str, Any]:
    """
    rem[name] = {
      records, streets(set), cities(set),
      city_streets: {city_name: set(street_keys)}
    }
    """
    all_streets: Set[Any] = set()
    all_cities: Set[Any] = set()
    zhytomyr_streets: Set[Any] = set()

    rem: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "records": 0,
            "streets": set(),
            "cities": set(),
            "city_streets": defaultdict(set),
        }
    )

    for a in accidents:
        street = a.get("street") or {}
        oname, cname, cid = _accident_place(a)
        sid = street.get("id")

        street_key = sid if sid is not None else (cname, street.get("name"))
        city_key = cid if cid is not None else cname

        all_streets.add(street_key)
        all_cities.add(city_key)

        if cname == "Житомир":
            zhytomyr_streets.add(street_key)

        rem[oname]["records"] += 1
        rem[oname]["streets"].add(street_key)
        rem[oname]["cities"].add(city_key)
        rem[oname]["city_streets"][cname].add(street_key)

    # серіалізація для Mongo / форматування
    rem_out: Dict[str, Any] = {}
    for name, st in rem.items():
        city_counts = {
            c: len(sids) for c, sids in st["city_streets"].items()
        }
        streets_n = len(st["streets"])
        cities_n = len(st["cities"])
        sp = rem_streets_pct(name, streets_n)
        cp = rem_cities_pct(name, cities_n)
        cov, cov_kind = rem_coverage_pct(name, streets_n, cities_n)
        entry: Dict[str, Any] = {
            "records": st["records"],
            "streets": streets_n,
            "cities": cities_n,
            "city_streets": city_counts,
        }
        if sp is not None:
            entry["streets_pct"] = round(sp, 1)
            entry["streets_total"] = _rem_street_total(name)
        if cp is not None:
            entry["cities_pct"] = round(cp, 1)
            entry["cities_total"] = REM_TOTAL_CITIES.get(_norm_rem_name(name))
        if cov is not None:
            entry["coverage_pct"] = round(cov, 1)
            entry["coverage_kind"] = cov_kind
        rem_out[name] = entry

    return {
        "total_records": len(accidents),
        "total_streets": len(all_streets),
        "total_cities": len(all_cities),
        "zhytomyr_streets": len(zhytomyr_streets),
        "rem": rem_out,
    }


def format_message(
    stats: Dict[str, Any],
    as_of: datetime,
    level_meta: Tuple[str, str, str] | None = None,
    city_street_totals: Dict[Tuple[str, str], int] | None = None,
) -> str:
    time_full = as_of.strftime("%d.%m.%Y %H:%M")
    time_short = as_of.strftime("%H:%M %d.%m")

    total_cities = stats["total_cities"]
    total_streets = stats["total_streets"]

    if level_meta is None:
        level_meta = level_from_cities(total_cities)
    _level_code, level_emoji, level_label = level_meta

    cities_pct_obl = None
    streets_pct_obl = None
    if OBLAST_TOTAL_CITIES > 0:
        cities_pct_obl = min(100.0, 100.0 * int(total_cities or 0) / OBLAST_TOTAL_CITIES)
    if _oblast_street_total() > 0:
        streets_pct_obl = min(
            100.0, 100.0 * int(total_streets or 0) / _oblast_street_total()
        )

    header_counts = (
        f"Аварійні відключення зараз у <b>{total_cities}</b> населених пунктах "
        f"(загалом <b>{total_streets:,}</b> вулиць)"
    ).replace(",", " ")
    if cities_pct_obl is not None and streets_pct_obl is not None:
        header_counts += (
            f" — <b>{cities_pct_obl:.1f}%</b> н.п. / <b>{streets_pct_obl:.1f}%</b> вул. області"
        )
    header_counts += "."

    lines: List[str] = [
        f"{level_emoji}<b>{level_label}</b> стан електромереж у Житомирській області",
        "",
        header_counts,
        "",
        "⚠️ <b>Ситуація по РЕМ</b> (від найбільш проблемних):",
    ]

    def _rem_sort_key(item: Tuple[str, dict]) -> Tuple[float, int, int, int]:
        """Спочатку за % покриття району (вулиці або н.п.), далі за кількістю."""
        name, st = item
        streets = int(st.get("streets") or 0)
        cities = int(st.get("cities") or 0)
        pct = st.get("coverage_pct")
        if pct is None:
            pct, _ = rem_coverage_pct(name, streets, cities)
        pct_v = float(pct) if pct is not None else -1.0
        return (
            -pct_v,
            -streets,
            -cities,
            -int(st.get("records") or 0),
        )

    rem_sorted = sorted(stats["rem"].items(), key=_rem_sort_key)

    block: List[str] = []
    for name, st in rem_sorted:
        if st["streets"] == 0:
            continue
        short = name.replace(" РЕМ", "").strip()
        streets_s = f"{st['streets']:,}".replace(",", " ")

        cp = st.get("cities_pct")
        sp = st.get("streets_pct")
        if cp is None:
            cp = rem_cities_pct(name, st["cities"])
        if sp is None:
            sp = rem_streets_pct(name, st["streets"])

        cov = st.get("coverage_pct")
        if cov is None:
            cov, _ = rem_coverage_pct(name, st["streets"], st["cities"])
        emoji = severity_emoji(st["streets"], coverage_pct=cov)

        pct_parts: List[str] = []
        if cp is not None:
            pct_parts.append(f"{cp:.1f}% н.п.")
        if sp is not None:
            pct_parts.append(f"{min(sp, 100.0):.1f}% району")
        if pct_parts:
            block.append(
                f"{emoji} <b>{short}</b> — {st['cities']} нас.п. / "
                f"{streets_s} вул. ({' - '.join(pct_parts)})"
            )
        else:
            block.append(
                f"{emoji} <b>{short}</b> — {st['cities']} нас.п. / "
                f"{streets_s} вул."
            )

        city_items = sorted(
            st.get("city_streets", {}).items(),
            key=lambda x: -x[1],
        )
        rem_total = _rem_street_total(name)
        rem_affected = int(st.get("streets") or 0) or 1

        def _city_pct(cnt_i: int) -> float:
            if rem_total and rem_total > 0:
                return 100.0 * cnt_i / rem_total
            return 100.0 * cnt_i / rem_affected

        def _own_pct(cname: str, cnt_i: int) -> float | None:
            """Частка вимкнених вулиць у самому населеному пункті."""
            if not city_street_totals or cnt_i <= 0:
                return None
            total = city_street_totals.get(
                (_norm_rem_name(name), (cname or "").strip())
            )
            if not total or total <= 0:
                return None
            return min(100.0, 100.0 * cnt_i / total)

        def _fmt_np_line(cname: str, cnt_i: int) -> str:
            bits = [f"{_city_pct(cnt_i):.1f}% РЕМ"]
            own = _own_pct(cname, cnt_i)
            if own is not None:
                bits.append(f"{own:.1f}% нас. пункту")
            return (
                f"    · {city_label(cname)} — {cnt_i} вул. "
                f"({' - '.join(bits)})"
            )

        def _own_pct_many(items: List[Tuple[str, int]]) -> float | None:
            """Сумарна частка вимкнених вулиць по кількох н.п. разом."""
            if not city_street_totals or not items:
                return None
            total = 0
            affected = 0
            rem_key = _norm_rem_name(name)
            for cname, cnt_i in items:
                known = city_street_totals.get((rem_key, (cname or "").strip()))
                if not known or known <= 0:
                    return None
                total += int(known)
                affected += int(cnt_i or 0)
            if total <= 0 or affected <= 0:
                return None
            return min(100.0, 100.0 * affected / total)

        # окремо — н.п. з ≥ CITY_PCT_MIN % вулиць району; дрібні → «ще N»
        shown: list = []
        rest: list = []
        for cname, cnt in city_items:
            cnt_i = int(cnt or 0)
            if _city_pct(cnt_i) >= CITY_PCT_MIN and len(shown) < CITY_LIST_MAX:
                shown.append((cname, cnt_i))
            else:
                rest.append((cname, cnt_i))

        for cname, cnt_i in shown:
            block.append(_fmt_np_line(cname, cnt_i))

        if rest:
            # < CITY_PCT_MIN: один н.п. → лише назва; кілька → назва + «ще N»
            if len(rest) == 1:
                cname, cnt_i = rest[0]
                if cnt_i > 0:
                    block.append(_fmt_np_line(cname, cnt_i))
            else:
                first_name, _first_cnt = rest[0]
                more = len(rest) - 1
                rest_streets = sum(c for _, c in rest)
                if rest_streets > 0:
                    bits = [f"{_city_pct(rest_streets):.1f}% РЕМ"]
                    own = _own_pct_many(rest)
                    if own is not None:
                        bits.append(f"{own:.1f}% нас. пункту")
                    block.append(
                        f"    · {city_label(first_name)} + ще {more} нас.п. — "
                        f"{rest_streets} вул. ({' - '.join(bits)})"
                    )

    if block:
        lines.append("<blockquote expandable>")
        lines.extend(block)
        lines.append("</blockquote>")
    else:
        lines.append("<i>немає даних</i>")

    lines.append("")

    # Лише для CRITICAL — заклик після блоку РЕМ, перед легендою
    if _level_code == "CRITICAL":
        lines.append("<i>Будь ласка, не користуйтесь потужними електроприладами.</i>")
        lines.append("")

    lines.append("<blockquote expandable>")
    lines.append("ℹ️ <b>Як читати відсотки</b>")
    lines.append("• шапка — частка <b>області</b>: % н.п. (населені пункти) і % вул. (вулиці)")
    lines.append(
        "• рядок РЕМ: <b>% н.п.</b> — частка населених пунктів району; "
        "<b>% району</b> — яка частка вулиць цього району зараз без світла"
    )
    lines.append(
        "• рядок н.п.: <b>% РЕМ</b> — внесок пункту у вулиці району; "
        "<b>% нас. пункту</b> — яка частка вулиць самого пункту зараз без світла"
    )
    lines.append("</blockquote>")
    lines.append("")

    footer_parts: List[str] = []
    if AVARII_ZHOE:
        footer_parts.append(f'<a href="{AVARII_ZHOE}">🔗 Сайт "ЖОЕ"</a>')
    if SUBSCRIBE:
        footer_parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
    if footer_parts:
        lines.append("  ".join(footer_parts))
    lines.append(f"🕐 {time_short}")

    return '\n'.join(lines)



def save_to_mongo(
    stats: Dict[str, Any],
    as_of: datetime,
    message: str,
    *,
    level: str,
    level_emoji: str,
    notified: bool,
    prev: dict | None = None,
) -> None:
    """
    Завжди пише latest + історію по годині.
    notified=True → оновлює last_notified_* (антиспам на день).
    notified=False → зберігає попередні last_notified_* з prev.
    """
    col = get_mongo_col()
    if col is None:
        log_to_buffer("⚠️ Немає MONGO_URI — пропускаємо запис у Mongo")
        return

    db_name = os.getenv("MONGO_DB") or "vidkluchennya"

    try:
        prev = prev or {}
        today = as_of.strftime("%Y-%m-%d")

        if notified:
            last_notified_date = today
            last_notified_level = level
            last_notified_at = datetime.now(timezone.utc)
            last_notified_cities = stats["total_cities"]
        else:
            last_notified_date = prev.get("last_notified_date")
            last_notified_level = prev.get("last_notified_level")
            last_notified_at = prev.get("last_notified_at")
            last_notified_cities = prev.get("last_notified_cities")

        oc_pct = None
        os_pct = None
        if OBLAST_TOTAL_CITIES > 0:
            oc_pct = round(
                min(100.0, 100.0 * int(stats["total_cities"] or 0) / OBLAST_TOTAL_CITIES),
                1,
            )
        if _oblast_street_total() > 0:
            os_pct = round(
                min(100.0, 100.0 * int(stats["total_streets"] or 0) / _oblast_street_total()),
                1,
            )

        doc = {
            "_id": "latest",
            "as_of": as_of.isoformat(),
            "as_of_utc": datetime.now(timezone.utc),
            "total_records": stats["total_records"],
            "total_streets": stats["total_streets"],
            "total_cities": stats["total_cities"],
            "zhytomyr_streets": stats["zhytomyr_streets"],
            "oblast_cities_pct": oc_pct,
            "oblast_streets_pct": os_pct,
            "rem": stats["rem"],
            "message": message,
            "level": level,
            "level_emoji": level_emoji,
            "updated_at": datetime.now(timezone.utc),
            "last_notified_date": last_notified_date,
            "last_notified_level": last_notified_level,
            "last_notified_at": last_notified_at,
            "last_notified_cities": last_notified_cities,
        }
        col.update_one({"_id": "latest"}, {"$set": doc}, upsert=True)

        history_id = as_of.strftime("%Y-%m-%dT%H")
        hist = {k: v for k, v in doc.items() if k != "_id"}
        hist["_id"] = history_id
        col.update_one({"_id": history_id}, {"$set": hist}, upsert=True)

        log_to_buffer(
            f"🗄 Mongo: {db_name}.accidents_summary (latest + {history_id}), "
            f"level={level}, notified={notified}"
        )
    except Exception as e:
        log_to_buffer(f"❌ Mongo write failed: {e}")



def main() -> int:
    log_to_buffer("=" * 40)
    log_to_buffer("🚀 СТАРТ · Зведення аварій ЖОЕ")
    log_to_buffer(f"ZTOE_API_BASE={'задано' if API_BASE else 'НЕМАЄ'}")
    log_to_buffer(
        f"AVARII_ZHOE={'є' if AVARII_ZHOE else 'немає'} · "
        f"SUBSCRIBE={'є' if SUBSCRIBE else 'немає'}"
    )
    force = (os.getenv("FORCE_NOTIFY") or "").strip()
    if force:
        log_to_buffer(f"FORCE_NOTIFY={force}")

    try:
        accidents = fetch_all_accidents()
    except Exception as e:
        log_to_buffer(f"❌ Fetch failed: {e}")
        send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ (помилка)", message_thread_id=154)
        return 1

    as_of = datetime.now(KYIV)
    prev = load_latest_summary()

    if not accidents:
        log_to_buffer("⚠️ Порожня відповідь API")
        stats = {
            "total_records": 0,
            "total_streets": 0,
            "total_cities": 0,
            "zhytomyr_streets": 0,
            "rem": {},
        }
        level, level_emoji, level_label = level_from_cities(0)
        msg = (
            f"{level_emoji}<b>{level_label}</b> стан електромереж у Житомирській області\n\n"
            "Зараз аварійних відключень за даними API немає."
        )
        if AVARII_ZHOE or SUBSCRIBE:
            parts = []
            if AVARII_ZHOE:
                parts.append(f'<a href="{AVARII_ZHOE}">🔗 Сайт "ЖОЕ"</a>')
            if SUBSCRIBE:
                parts.append(f'<a href="{SUBSCRIBE}">⚡️ ПІДПИСАТИСЯ</a>')
            msg += "\n\n" + "  ".join(parts)
            msg += f"\n🕐 {as_of.strftime('%H:%M %d.%m')}"

        do_notify, reason = should_notify_channel(level, prev, as_of)
        log_to_buffer(f"📢 notify={do_notify} ({reason})")
        save_to_mongo(
            stats, as_of, msg,
            level=level, level_emoji=level_emoji,
            notified=False,  # порожньо / NORM — не ставимо мітку «вже слали»
            prev=prev,
        )
        # при порожньому API в канал не шлемо (база оновлена)
        send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ", message_thread_id=154)
        return 0

    city_street_totals = fetch_city_street_totals(accidents)
    stats = aggregate(accidents)
    level, level_emoji, level_label = level_from_cities(stats["total_cities"])
    msg = format_message(
        stats, as_of, (level, level_emoji, level_label), city_street_totals
    )

    log_to_buffer(
        f"📊 records={stats['total_records']} "
        f"streets={stats['total_streets']} "
        f"cities={stats['total_cities']} "
        f"zhytomyr={stats['zhytomyr_streets']} "
        f"level={level}"
    )
    top_rem = sorted(
        stats["rem"].items(),
        key=lambda x: (-x[1]["streets"], -x[1]["cities"]),
    )[:5]
    for name, st in top_rem:
        top_cities = sorted(st.get("city_streets", {}).items(), key=lambda x: -x[1])[:3]
        cities_s = ", ".join(f"{c}:{n}" for c, n in top_cities)
        log_to_buffer(
            f"  REM {name}: streets={st['streets']} cities={st['cities']} [{cities_s}]"
        )

    do_notify, reason = should_notify_channel(level, prev, as_of)
    log_to_buffer(f"📢 notify={do_notify} ({reason})")

    notified_ok = False
    if do_notify:
        ok = send_notification(msg)
        if ok:
            notified_ok = True
            log_to_buffer(f"✓ Telegram канал: звіт відправлено ({len(msg)} символів)")
        else:
            log_to_buffer("❌ Telegram канал: не вдалося відправити")
        # Особисті DM тим, у кого увімкнено «Стан області» (notify_prefs.region)
        try:
            send_region_dms(msg)
        except Exception as e:
            log_to_buffer(f"❌ region DM batch failed: {e}")
    else:
        log_to_buffer("⏭ Telegram: пропущено (антиспам / рівень)")

    # Mongo завжди; мітку last_notified оновлюємо лише якщо реально відправили
    save_to_mongo(
        stats, as_of, msg,
        level=level,
        level_emoji=level_emoji,
        notified=notified_ok,
        prev=prev,
    )

    send_log_to_channel("📊 ЛОГ · АВАРІЇ ЖОЕ", message_thread_id=154)
    if do_notify and not notified_ok:
        return 1
    return 0



if __name__ == "__main__":
    sys.exit(main())
