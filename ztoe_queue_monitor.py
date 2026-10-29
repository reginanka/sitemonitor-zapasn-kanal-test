#!/usr/bin/env python3
"""
Тижнева перевірка черг ГПВ для адрес підписників бота Житомиробленерго.

Читає колекцію queue_directory (унікальні адреси з чергами),
оновлює чергу з URL з секрету ZTOE_QUEUE_URL,
якщо змінилася — сповіщає користувачів з users (notify=true) і оновлює їх addresses.

Secrets / env:
  MAYACHOK_MONGO_URI або MONGO_URI  — MongoDB connection string
  ZTOE_MONGO_DB або MONGO_DB        — назва БД (напр. vidkluchennya)
  ZTOE_BOT_TOKEN або TELEGRAM_BOT_TOKEN — токен бота для сповіщень користувачам
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from pymongo import MongoClient
from telegram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_queue_monitor")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

BASE = os.getenv("ZTOE_QUEUE_URL") or os.getenv("QUEUE_SEARCH_URL")
if not BASE:
    raise SystemExit("Немає ZTOE_QUEUE_URL (URL сторінки пошуку черг)")
REQUEST_PAUSE_SEC = float(os.getenv("ZTOE_REQUEST_PAUSE", "0.8"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
}


# ---------- helpers: matching / parse (як у боті) ----------

def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("'", "'").replace("`", "'")
    s = re.sub(r"\s+", " ", s)
    for p in ("вул.", "вул ", "пров.", "пров ", "с.", "с ", "місто ", "м.", "м "):
        if s.startswith(p):
            s = s[len(p) :].strip()
    s = re.sub(r"^(вулиця|провулок)\s+", "", s)
    return s


def address_key(otg_name: str, city_name: str, street_name: str) -> str:
    raw = f"{_norm(otg_name)}|{_norm(city_name)}|{_norm(street_name)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def queue_fingerprint(queue: str, subqueue: str, houses: str, note: str) -> str:
    return "|".join([
        (queue or "").strip(),
        (subqueue or "").strip(),
        (houses or "").strip(),
        (note or "").strip(),
    ])


def _options(soup: BeautifulSoup, name: str) -> list[tuple[str, str]]:
    sel = soup.find("select", {"name": name})
    if not sel:
        return []
    out = []
    for o in sel.find_all("option"):
        val = (o.get("value") or "").strip()
        text = o.get_text(strip=True)
        if val and val != "0":
            out.append((val, text))
    return out


def _score_match(query: str, option_text: str) -> float:
    q = _norm(query)
    t = _norm(option_text)
    if not q or not t:
        return 0.0
    if q == t:
        return 1.0
    if q in t or t in q:
        return 0.85 - 0.1 * abs(len(q) - len(t)) / max(len(q), len(t), 1)
    q_words = set(re.findall(r"[а-яіїєґa-z0-9']+", q, re.I))
    t_words = set(re.findall(r"[а-яіїєґa-z0-9']+", t, re.I))
    if not q_words or not t_words:
        return 0.0
    inter = len(q_words & t_words)
    if inter == 0:
        return 0.0
    return 0.5 * inter / max(len(q_words), len(t_words))


def _best_match(options: list[tuple[str, str]], query: str) -> Optional[tuple[str, str]]:
    best = None
    best_sc = 0.0
    for vid, text in options:
        sc = _score_match(query, text)
        if sc > best_sc:
            best_sc = sc
            best = (vid, text)
    if best_sc < 0.5:
        return None
    return best


def _parse_result_table(soup: BeautifulSoup) -> list[dict]:
    """Усі рядки таблиці результату (не лише перший)."""
    results: list[dict] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for i, row in enumerate(rows):
            headers = [c.get_text(strip=True).lower() for c in row.find_all(["th", "td"])]
            if not any("черг" in h for h in headers):
                continue

            def idx(*needles: str) -> int | None:
                for j, h in enumerate(headers):
                    if any(n in h for n in needles):
                        return j
                return None

            i_queue = idx("черг")
            i_sub = idx("підчерг", "подчерг")
            i_houses = idx("будинк")
            i_note = idx("приміт")
            i_rem = idx("рем")
            i_city = idx("населен")
            i_street = idx("назв", "вулиц")

            for r in rows[i + 1 :]:
                cols = [c.get_text(strip=True) for c in r.find_all(["td", "th"])]
                if len(cols) < 3:
                    continue
                if cols and cols[0] == "РЕМ":
                    continue
                if not any(cols):
                    continue

                rem = cols[i_rem] if i_rem is not None and i_rem < len(cols) else ""
                city = cols[i_city] if i_city is not None and i_city < len(cols) else ""
                street = cols[i_street] if i_street is not None and i_street < len(cols) else ""
                houses = cols[i_houses] if i_houses is not None and i_houses < len(cols) else ""
                queue = cols[i_queue] if i_queue is not None and i_queue < len(cols) else ""
                subqueue = cols[i_sub] if i_sub is not None and i_sub < len(cols) else ""
                note = cols[i_note] if i_note is not None and i_note < len(cols) else ""

                if _is_time_token(houses) or _is_time_token(city) or _is_time_token(street):
                    continue
                if not rem and not city and not street:
                    continue
                if _is_time_token(queue) or _is_time_token(subqueue):
                    continue
                # рядок сітки черг без адреси
                if not street and not houses and re.match(r"^[\d.]+$", (rem or "").strip()):
                    continue

                results.append({
                    "rem": rem,
                    "city": city,
                    "street": street,
                    "houses": houses,
                    "queue": queue,
                    "subqueue": subqueue,
                    "note": note,
                })
            if results:
                return results
    return results


_TIME_TOKEN_RE = re.compile(
    r"^\d{1,2}:\d{2}(\s*[-–—]\s*\d{1,2}:\d{2})?$"
)


def _is_time_token(s: str) -> bool:
    return bool(_TIME_TOKEN_RE.match((s or "").strip()))


def _split_house_tokens(houses_str: str) -> list[str]:
    if not houses_str:
        return []
    parts = []
    for p in houses_str.replace(";", ",").split(","):
        p = p.strip()
        if not p or _is_time_token(p):
            continue
        parts.append(p)
    return parts


def _house_in_list(house: str, houses_str: str) -> bool:
    h = (house or "").strip().lower().replace(" ", "")
    if not h:
        return False
    for tok in _split_house_tokens(houses_str):
        t = tok.strip().lower().replace(" ", "")
        if t == h or t.rstrip(".") == h.rstrip("."):
            return True
        # діапазони типу 1-5 / 10–20
        m = re.match(r"^(\d+)\s*[-–—]\s*(\d+)$", t)
        if m and h.isdigit():
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo <= int(h) <= hi:
                return True
    return False


def _aggregate_street_rows(rows: list[dict]) -> dict:
    """Збирає всі будинки вулиці + house → row."""
    if not rows:
        return {"error": "no_rows", "message": "Немає запису черги для адреси"}

    all_houses: list[str] = []
    seen_h: set[str] = set()
    house_to_row: dict[str, dict] = {}
    for row in rows:
        tokens = _split_house_tokens(row.get("houses") or "")
        if not tokens:
            continue
        for tok in tokens:
            key = tok.strip().lower().replace(" ", "")
            if key not in seen_h:
                seen_h.add(key)
                all_houses.append(tok.strip())
            if key not in house_to_row:
                house_to_row[key] = row

    base = rows[0]
    if not all_houses:
        # вся вулиця одним рядком / без номерів
        return {
            "ok": True,
            "queue": base.get("queue") or "",
            "subqueue": base.get("subqueue") or "",
            "houses": base.get("houses") or "",
            "note": base.get("note") or "",
            "matched_rem": base.get("rem") or "",
            "matched_city": base.get("city") or "",
            "matched_street": base.get("street") or "",
            "rows": rows,
            "house_map": {},
            "all_houses": [],
        }

    return {
        "ok": True,
        "queue": base.get("queue") or "",
        "subqueue": base.get("subqueue") or "",
        "houses": ", ".join(all_houses),
        "note": base.get("note") or "",
        "matched_rem": base.get("rem") or "",
        "matched_city": base.get("city") or "",
        "matched_street": base.get("street") or "",
        "rows": rows,
        "all_houses": all_houses,
        "house_map": house_to_row,
        "multi_queue": len({
            ((r.get("queue") or "").strip(), (r.get("subqueue") or "").strip())
            for r in rows
        }) > 1,
    }


def apply_house_to_info(info: dict, house: str | None) -> dict:
    """
    Якщо в користувача є номер будинку — беремо чергу саме для нього.
    Якщо будинок не входить у жоден список довідника — це «немає черги / не вимикають»
    (не беремо чужий рядок на кшталт «120 · непобутові»).
    """
    if not info.get("ok"):
        return info
    house = (house or "").strip()
    if not house:
        return info

    # 1) точний ключ у house_map
    hmap = info.get("house_map") or {}
    key = house.lower().replace(" ", "")
    row = hmap.get(key)
    if row is None:
        # 2) пошук через _house_in_list (діапазони тощо)
        for r in info.get("rows") or []:
            if _house_in_list(house, r.get("houses") or ""):
                row = r
                break

    out = dict(info)
    out["house"] = house
    if row is not None:
        out["queue"] = row.get("queue") or ""
        out["subqueue"] = row.get("subqueue") or ""
        out["houses"] = row.get("houses") or house
        out["note"] = row.get("note") or ""
        out["house_matched"] = True
        return out

    # будинок не знайдено в довіднику → не присвоюємо чужу чергу
    out["queue"] = "не вимикають, крім аварійних"
    out["subqueue"] = ""
    out["houses"] = house
    out["note"] = "номер не в списках черг довідника"
    out["house_matched"] = False
    return out


def fetch_queue(otg_name: str, city_name: str, street_name: str) -> dict:
    """Повертає dict з queue/subqueue/house_map/... або error + temporary."""
    with httpx.Client(timeout=25, follow_redirects=True, headers=HEADERS) as client:
        r = client.post(BASE, data={})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        rems = _options(soup, "rem_id")
        if not rems:
            return {
                "error": "directory_unavailable",
                "temporary": True,
                "message": "Довідник черг тимчасово недоступний (порожній список РЕМ)",
            }

        rem_query = re.sub(r"\s*рем\s*$", "", otg_name or "", flags=re.I).strip()
        rem = _best_match(rems, rem_query)
        if not rem:
            return {"error": "rem_not_found", "message": f"РЕМ не знайдено: {otg_name}"}

        time.sleep(REQUEST_PAUSE_SEC)
        r = client.post(BASE, data={"rem_id": rem[0]})
        soup = BeautifulSoup(r.text, "html.parser")
        cities = _options(soup, "naspunkt_id")
        if not cities:
            return {
                "error": "directory_unavailable",
                "temporary": True,
                "message": "Довідник черг тимчасово недоступний (порожній список пунктів)",
            }
        city = _best_match(cities, city_name)
        if not city:
            return {"error": "city_not_found", "message": f"Пункт не знайдено: {city_name}"}

        time.sleep(REQUEST_PAUSE_SEC)
        r = client.post(BASE, data={"rem_id": rem[0], "naspunkt_id": city[0]})
        soup = BeautifulSoup(r.text, "html.parser")
        streets = _options(soup, "vulica_id")
        if not streets:
            return {
                "error": "directory_unavailable",
                "temporary": True,
                "message": "Довідник черг тимчасово недоступний (порожній список вулиць)",
            }
        street = _best_match(streets, street_name)
        if not street:
            return {"error": "street_not_found", "message": f"Вулицю не знайдено: {street_name}"}

        time.sleep(REQUEST_PAUSE_SEC)
        r = client.post(
            BASE,
            data={"rem_id": rem[0], "naspunkt_id": city[0], "vulica_id": street[0]},
        )
        soup = BeautifulSoup(r.text, "html.parser")
        rows = _parse_result_table(soup)
        if not rows:
            return {"error": "no_rows", "message": "Немає запису черги для адреси"}

        info = _aggregate_street_rows(rows)
        info["matched_rem"] = rem[1]
        info["matched_city"] = city[1]
        info["matched_street"] = street[1]
        return info



def _short_queue_label(queue: str = "", subqueue: str = "", note: str = "") -> str:
    """Короткий рядок черги для «було / стало»."""
    q = (queue or "").strip()
    sq = (subqueue or "").strip()
    q_low = q.lower()
    if any(m in q_low for m in ("не вимикається", "не вимикають", "не відключається", "не відключають")):
        base = "не вимикають, крім аварійних"
    elif sq and sq != q and len(sq) < 40 and re.match(r"^[\d.]+$", sq):
        base = f"черга {q}, підчерга {sq}"
    elif re.match(r"^[\d.]+$", q):
        base = f"черга {q}" + (f", підчерга {sq}" if sq and re.match(r"^[\d.]+$", sq) else "")
    elif q:
        base = q if len(q) < 80 else "не вимикають, крім аварійних"
    else:
        base = "—"
    n = (note or "").strip()
    if n and n not in base:
        base = f"{base} ({n})" if len(n) < 60 else base
    return base


def format_queue_msg(info: dict) -> str:
    q = info.get("queue") or "—"
    sq = info.get("subqueue") or ""
    q_low = (q or "").lower()
    no_gpv = any(
        m in q_low
        for m in ("не вимикається", "не вимикають", "не відключається", "не відключають")
    )
    if no_gpv:
        line = "не вимикають, крім аварійних"
    elif sq and sq != q and len(sq) < 40 and re.match(r"^[\d.]+$", sq.strip()):
        line = f"Черга: <b>{q}</b>, підчерга: <b>{sq}</b>"
    elif re.match(r"^[\d.]+$", q.strip()):
        line = f"Черга: <b>{q}</b>" + (
            f", підчерга: <b>{sq}</b>" if sq and re.match(r"^[\d.]+$", sq.strip()) else ""
        )
    else:
        line = q if len(q) < 80 else "не вимикають, крім аварійних"
    parts = [line]
    house = (info.get("house") or "").strip()
    if house:
        parts.append(f"🏠 Буд. <b>{house}</b>")
    elif info.get("houses"):
        parts.append(f"🏘 Будинки: {info['houses']}")
    if info.get("note"):
        parts.append(f"📝 {info['note']}")
    return "\n".join(parts)


def addr_matches_doc(addr: dict, doc: dict) -> bool:
    """Чи адреса користувача відповідає запису в queue_directory."""
    if addr.get("queue_key") and doc.get("_id") and addr["queue_key"] == doc["_id"]:
        return True
    return (
        _norm(addr.get("otg_name", "")) == _norm(doc.get("otg_name", ""))
        and _norm(addr.get("city_name", "")) == _norm(doc.get("city_name", ""))
        and _norm(addr.get("street_name", "")) == _norm(doc.get("street_name", ""))
    )



def send_log(text: str, message_thread_id: int = 155) -> None:
    """Пише підсумок у лог-групу ботом репо (TELEGRAM_BOT_TOKEN).
    155 — гілка «Перевірка черг».
    """
    channel = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or os.getenv("ZTOE_LOG_CHANNEL_ID")
    log_token = os.getenv("TELEGRAM_BOT_TOKEN")  # той самий, що в інших monitor'ах
    if not channel:
        log.info("TELEGRAM_LOG_CHANNEL_ID не задано — пропуск лог-каналу")
        return
    if not log_token:
        log.info("TELEGRAM_BOT_TOKEN не задано — пропуск лог-каналу")
        return
    try:
        log_bot = Bot(token=log_token)
        async def _send():
            kwargs = {
                "chat_id": channel,
                "text": text,
                "parse_mode": "HTML",
            }
            if message_thread_id is not None:
                kwargs["message_thread_id"] = message_thread_id
            await log_bot.send_message(**kwargs)
        asyncio.run(_send())
        log.info("Лог надіслано в %s (thread=%s)", channel, message_thread_id)
    except Exception as e:
        log.error("Не вдалося надіслати в лог-канал: %s", e)



def main() -> int:
    mongo_uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    mongo_db = os.getenv("ZTOE_MONGO_DB") or os.getenv("MONGO_DB") or "vidkluchennya"
    bot_token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

    if not mongo_uri:
        log.error("Немає MAYACHOK_MONGO_URI / MONGO_URI")
        return 1
    if not bot_token:
        log.error("Немає ZTOE_BOT_TOKEN / TELEGRAM_BOT_TOKEN")
        return 1

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=15000)
    db = client[mongo_db]
    directory = db["queue_directory"]
    users = db["users"]

    docs = list(directory.find({}))
    log.info("Адреси в queue_directory: %s (db=%s)", len(docs), mongo_db)

    if not docs:
        log.info("Немає адрес для перевірки — вихід")
        bot = Bot(token=bot_token)
        send_log("✅ <b>Перевірка черг</b>\n\nУ базі ще немає збережених адрес з чергами.")
        return 0

    bot = Bot(token=bot_token)
    checked = 0
    changed = 0
    skipped_temp = 0
    notified_users = 0
    errors = 0

    for doc in docs:
        otg = doc.get("otg_name") or ""
        city = doc.get("city_name") or ""
        street = doc.get("street_name") or ""
        label = f"{otg} / {city} / {street}"
        checked += 1

        try:
            street_info = fetch_queue(otg, city, street)
        except Exception as e:
            log.exception("Помилка запиту для %s: %s", label, e)
            directory.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "last_error": str(e),
                    "last_check_at": datetime.now(timezone.utc),
                }},
            )
            errors += 1
            continue

        if street_info.get("temporary"):
            log.warning("Тимчасово недоступно: %s — %s", label, street_info.get("message"))
            directory.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "last_error": street_info.get("message") or "temporary",
                    "last_check_at": datetime.now(timezone.utc),
                }},
            )
            skipped_temp += 1
            continue

        if street_info.get("error"):
            log.warning("Не знайдено: %s — %s", label, street_info.get("message"))
            directory.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "last_error": street_info.get("message") or street_info.get("error"),
                    "last_check_at": datetime.now(timezone.utc),
                }},
            )
            errors += 1
            continue

        # street-level snapshot у directory (для діагностики; сповіщення — per-house)
        street_fp = queue_fingerprint(
            street_info.get("queue", ""),
            street_info.get("subqueue", ""),
            street_info.get("houses", ""),
            street_info.get("note", ""),
        )
        directory.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "queue": street_info.get("queue", ""),
                "subqueue": street_info.get("subqueue", ""),
                "houses": street_info.get("houses", ""),
                "note": street_info.get("note", ""),
                "queue_fp": street_fp,
                "matched_rem": street_info.get("matched_rem"),
                "matched_city": street_info.get("matched_city"),
                "matched_street": street_info.get("matched_street"),
                "all_houses": street_info.get("all_houses") or [],
                "updated_at": datetime.now(timezone.utc),
                "last_check_at": datetime.now(timezone.utc),
                "last_error": None,
            }},
        )

        # Користувачі з цією вулицею — черга рахується по їхньому номеру будинку
        cursor = users.find({"notify": True})
        for user in cursor:
            addrs = user.get("addresses") or (
                [user["address"]] if user.get("address") else []
            )
            new_addrs = []
            touched = False
            notify_info = None
            notify_label_house = ""
            notify_old = {}

            for a in addrs:
                if not addr_matches_doc(a, doc):
                    new_addrs.append(a)
                    continue

                house = (a.get("house") or "").strip()
                info = apply_house_to_info(street_info, house)
                new_fp = queue_fingerprint(
                    info.get("queue", ""),
                    info.get("subqueue", ""),
                    # у fp кладемо саме house (не весь список), щоб не плутати з чужими номерами
                    house or (info.get("houses") or ""),
                    info.get("note", ""),
                )
                old_fp = a.get("queue_fp") or ""

                updated = {
                    **a,
                    "queue": info.get("queue", ""),
                    "subqueue": info.get("subqueue", ""),
                    "queue_houses": info.get("houses", ""),
                    "queue_note": info.get("note", ""),
                    "queue_fp": new_fp,
                    "queue_updated_at": datetime.now(timezone.utc).isoformat(),
                    "queue_key": doc["_id"],
                }
                if house:
                    updated["house"] = house

                new_addrs.append(updated)
                touched = True

                if old_fp and new_fp and old_fp != new_fp:
                    changed += 1
                    notify_info = info
                    notify_label_house = house
                    notify_old = {
                        "queue": a.get("queue") or "",
                        "subqueue": a.get("subqueue") or "",
                        "note": a.get("queue_note") or "",
                    }
                    log.info(
                        "Зміна черги: %s house=%s old_fp=%s new_fp=%s matched=%s",
                        label, house or "—", old_fp[:40], new_fp[:40],
                        info.get("house_matched"),
                    )
                elif not old_fp and new_fp:
                    log.info("Перша фіксація черги (без notify): %s house=%s", label, house or "—")

            if not touched:
                continue

            users.update_one(
                {"telegram_id": user["telegram_id"]},
                {"$set": {"addresses": new_addrs}, "$unset": {"address": ""}},
            )

            if notify_info is None:
                continue

            city_s = city
            street_s = street
            house_bit = f", буд. {notify_label_house}" if notify_label_house else ""
            was = _short_queue_label(
                notify_old.get("queue", ""),
                notify_old.get("subqueue", ""),
                notify_old.get("note", ""),
            )
            now = _short_queue_label(
                notify_info.get("queue", ""),
                notify_info.get("subqueue", ""),
                notify_info.get("note", ""),
            )
            text_msg = (
                "🔔 <b>Змінилася черга відключень:</b>\n\n"
                f"<b>{city_s}, {street_s}{house_bit}</b>\n\n"
                f"Було: <b>{was}</b>\n"
                f"Стало: <b>{now}</b>"
            )
            try:
                async def _send(tid=user["telegram_id"], body=text_msg):
                    await bot.send_message(
                        chat_id=tid,
                        text=body,
                        parse_mode="HTML",
                    )

                asyncio.run(_send())
                notified_users += 1
                log.info("Сповіщено %s", user["telegram_id"])
                time.sleep(0.3)
            except Exception as e:
                log.error("Не вдалося надіслати %s: %s", user.get("telegram_id"), e)

    summary = (
        f"✅ <b>Перевірка черг</b>\n\n"
        f"Перевірено адрес: <b>{checked}</b>\n"
        f"Черга змінилась: <b>{changed}</b>\n"
        f"Сповіщено людей: <b>{notified_users}</b>\n"
        f"Сайт тимчасово недоступний: <b>{skipped_temp}</b>\n"
        f"Помилок: <b>{errors}</b>"
    )
    log.info(
        "Готово: checked=%s changed=%s notified=%s temp=%s errors=%s",
        checked, changed, notified_users, skipped_temp, errors,
    )
    send_log(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
