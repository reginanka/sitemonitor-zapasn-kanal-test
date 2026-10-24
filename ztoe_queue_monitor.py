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
    results = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for i, row in enumerate(rows):
            cols_raw = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if not cols_raw or cols_raw[0] != "РЕМ":
                continue
            if not any(c == "Черга" for c in cols_raw):
                continue
            headers = [c.lower() for c in cols_raw]

            def idx(*names):
                for j, h in enumerate(headers):
                    for n in names:
                        if n in h:
                            return j
                return None

            i_queue = idx("черга")
            i_sub = idx("підчерг")
            i_houses = idx("будинк")
            i_note = idx("приміт")
            i_rem = idx("рем")
            i_city = idx("населен")
            i_street = idx("назв", "вулиц")

            if i + 1 >= len(rows):
                continue
            cols = [c.get_text(strip=True) for c in rows[i + 1].find_all(["td", "th"])]
            if len(cols) < 5 or cols[0] == "РЕМ":
                continue

            results.append({
                "rem": cols[i_rem] if i_rem is not None and i_rem < len(cols) else "",
                "city": cols[i_city] if i_city is not None and i_city < len(cols) else "",
                "street": cols[i_street] if i_street is not None and i_street < len(cols) else "",
                "houses": cols[i_houses] if i_houses is not None and i_houses < len(cols) else "",
                "queue": cols[i_queue] if i_queue is not None and i_queue < len(cols) else "",
                "subqueue": cols[i_sub] if i_sub is not None and i_sub < len(cols) else "",
                "note": cols[i_note] if i_note is not None and i_note < len(cols) else "",
            })
            if results:
                return results
    return results


def fetch_queue(otg_name: str, city_name: str, street_name: str) -> dict:
    """Повертає dict з queue/subqueue/... або error + temporary."""
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

        row = rows[0]
        return {
            "ok": True,
            "queue": row.get("queue") or "",
            "subqueue": row.get("subqueue") or "",
            "houses": row.get("houses") or "",
            "note": row.get("note") or "",
            "matched_rem": rem[1],
            "matched_city": city[1],
            "matched_street": street[1],
        }


def format_queue_msg(info: dict) -> str:
    q = info.get("queue") or "—"
    sq = info.get("subqueue") or ""
    if sq and sq != q and len(sq) < 40:
        line = f"Черга: <b>{q}</b>, підчерга: <b>{sq}</b>"
    elif re.match(r"^[\d.]+$", q.strip()):
        line = f"Черга: <b>{q}</b>" + (
            f", підчерга: <b>{sq}</b>" if sq and re.match(r"^[\d.]+$", sq.strip()) else ""
        )
    else:
        line = q
    parts = [line]
    if info.get("houses"):
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



def send_log(text: str) -> None:
    """Пише підсумок у лог-канал ботом репо (TELEGRAM_BOT_TOKEN), не ботом відключень."""
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
            await log_bot.send_message(chat_id=channel, text=text, parse_mode="HTML")
        asyncio.run(_send())
        log.info("Лог надіслано в %s", channel)
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
        send_log(f"✅ <b>ZTOE queues</b>\ndb: <code>{mongo_db}</code>\nchecked: 0\n(немає адрес у queue_directory)")
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
            info = fetch_queue(otg, city, street)
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

        if info.get("temporary"):
            log.warning("Тимчасово недоступно: %s — %s", label, info.get("message"))
            directory.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "last_error": info.get("message") or "temporary",
                    "last_check_at": datetime.now(timezone.utc),
                }},
            )
            skipped_temp += 1
            # не затираємо стару чергу
            continue

        if info.get("error"):
            log.warning("Не знайдено: %s — %s", label, info.get("message"))
            directory.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "last_error": info.get("message") or info.get("error"),
                    "last_check_at": datetime.now(timezone.utc),
                }},
            )
            errors += 1
            continue

        new_fp = queue_fingerprint(
            info.get("queue", ""),
            info.get("subqueue", ""),
            info.get("houses", ""),
            info.get("note", ""),
        )
        old_fp = doc.get("queue_fp") or ""

        update_fields = {
            "queue": info.get("queue", ""),
            "subqueue": info.get("subqueue", ""),
            "houses": info.get("houses", ""),
            "note": info.get("note", ""),
            "queue_fp": new_fp,
            "matched_rem": info.get("matched_rem"),
            "matched_city": info.get("matched_city"),
            "matched_street": info.get("matched_street"),
            "updated_at": datetime.now(timezone.utc),
            "last_check_at": datetime.now(timezone.utc),
            "last_error": None,
        }
        directory.update_one({"_id": doc["_id"]}, {"$set": update_fields})

        if old_fp and new_fp and old_fp != new_fp:
            changed += 1
            log.info("Зміна черги: %s", label)

            # знайти користувачів з цією адресою
            cursor = users.find({"notify": True})
            for user in cursor:
                addrs = user.get("addresses") or (
                    [user["address"]] if user.get("address") else []
                )
                touched = False
                new_addrs = []
                for a in addrs:
                    if addr_matches_doc(a, doc):
                        a = {
                            **a,
                            "queue": update_fields["queue"],
                            "subqueue": update_fields["subqueue"],
                            "queue_houses": update_fields["houses"],
                            "queue_note": update_fields["note"],
                            "queue_fp": new_fp,
                            "queue_updated_at": datetime.now(timezone.utc).isoformat(),
                            "queue_key": doc["_id"],
                        }
                        touched = True
                    new_addrs.append(a)

                if not touched:
                    continue

                users.update_one(
                    {"telegram_id": user["telegram_id"]},
                    {"$set": {"addresses": new_addrs}, "$unset": {"address": ""}},
                )

                text = (
                    "🔔 <b>Змінилася черга відключень:</b>\n\n"
                    f"<b>{city}, {street}</b>\n"
                    f"{format_queue_msg(info)}"
                )
                try:
                    import asyncio

                    async def _send():
                        await bot.send_message(
                            chat_id=user["telegram_id"],
                            text=text,
                            parse_mode="HTML",
                        )

                    asyncio.run(_send())
                    notified_users += 1
                    log.info("Сповіщено %s", user["telegram_id"])
                    time.sleep(0.3)
                except Exception as e:
                    log.error("Не вдалося надіслати %s: %s", user.get("telegram_id"), e)

        elif not old_fp and new_fp:
            # перший успішний запис без сповіщення (щоб не спамити історією)
            log.info("Перша фіксація черги (без notify): %s", label)

    summary = (
        f"✅ <b>ZTOE queues</b>\n"
        f"db: <code>{mongo_db}</code>\n"
        f"checked: {checked}\n"
        f"changed: {changed}\n"
        f"notified: {notified_users}\n"
        f"temporary_skip: {skipped_temp}\n"
        f"errors: {errors}"
    )
    log.info(
        "Готово: checked=%s changed=%s notified=%s temp=%s errors=%s",
        checked, changed, notified_users, skipped_temp, errors,
    )
    send_log(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
