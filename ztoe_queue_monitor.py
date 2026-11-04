#!/usr/bin/env python3
"""
Тижнева перевірка черг ГПВ.

ВАЖЛИВО: логіка пошуку черги = копія bot/queue_client.py (/queue).
Ті самі _score_match / resolve_queue_step / find_queue / apply_house_to_result.
Не «майже як бот» — той самий алгоритм.

Secrets: MONGO_URI, MONGO_DB, ZTOE_BOT_TOKEN
Optional: ZTOE_QUEUE_URL (default unhooking-search.php), ZTOE_REQUEST_PAUSE
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
from collections import Counter
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

BASE = (
    os.getenv("ZTOE_QUEUE_URL")
    or os.getenv("QUEUE_SEARCH_URL")
    or "https://www.ztoe.com.ua/unhooking-search.php"
)
REQUEST_PAUSE_SEC = float(os.getenv("ZTOE_REQUEST_PAUSE", "0.35"))
HTTP_TIMEOUT = float(os.getenv("ZTOE_HTTP_TIMEOUT", "60"))
SKIP_RECENT_MINUTES = float(os.getenv("ZTOE_QUEUE_SKIP_MINUTES", "5"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
}


# Кеш на один прогін крону (інакше кожна адреса сканує всі РЕМ → timeout)
_CITY_OPTIONS_CACHE: dict[str, list[tuple[str, str]]] = {}
_STREET_OPTIONS_CACHE: dict[tuple[str, str], list[tuple[str, str]]] = {}
_REMS_CACHE: list[tuple[str, str]] | None = None


def clear_queue_http_cache() -> None:
    global _CITY_OPTIONS_CACHE, _STREET_OPTIONS_CACHE, _REMS_CACHE
    _CITY_OPTIONS_CACHE = {}
    _STREET_OPTIONS_CACHE = {}
    _REMS_CACHE = None


# =============================================================================
# === ПОЧАТОК: логіка з bot/queue_client.py (sync-порт, без змін алгоритму) ===
# =============================================================================

def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("'", "'").replace("`", "'")
    s = re.sub(r"\s+", " ", s)
    for p in ("вул.", "вул ", "пров.", "пров ", "с.", "с ", "місто ", "м.", "м "):
        if s.startswith(p):
            s = s[len(p) :].strip()
    s = re.sub(r"^(вулиця|провулок)\s+", "", s)
    return s


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
    """0 = немає збігу, 1 = точний, 0.5..0.99 = частковий."""
    q = _norm(query)
    t = _norm(option_text)
    if not q or not t:
        return 0.0
    if q == t:
        return 1.0
    if q in t or t in q:
        # коротший запит у довшій назві — ок, але не ідеал
        return 0.85 - 0.1 * abs(len(q) - len(t)) / max(len(q), len(t), 1)
    q_words = set(re.findall(r"[а-яіїєґa-z0-9']+", q, re.I))
    t_words = set(re.findall(r"[а-яіїєґa-z0-9']+", t, re.I))
    if not q_words or not t_words:
        return 0.0
    inter = len(q_words & t_words)
    if inter == 0:
        return 0.0
    return 0.5 * inter / max(len(q_words), len(t_words))


def _filter_matches(options: list[tuple[str, str]], query: str, min_score: float = 0.5) -> list[tuple[str, str, float]]:
    scored = []
    for vid, text in options:
        sc = _score_match(query, text)
        if sc >= min_score:
            scored.append((vid, text, sc))
    scored.sort(key=lambda x: (-x[2], x[1]))
    return scored


def _best_match(options: list[tuple[str, str]], query: str) -> Optional[tuple[str, str]]:
    matches = _filter_matches(options, query)
    if not matches:
        return None
    return matches[0][0], matches[0][1]


def _post(client: httpx.Client, data: dict) -> BeautifulSoup:
    if REQUEST_PAUSE_SEC > 0:
        time.sleep(REQUEST_PAUSE_SEC)
    r = client.post(BASE, data=data, headers=HEADERS)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def _directory_unavailable(soup: BeautifulSoup) -> bool:
    """
    True, якщо форма довідника черг тимчасово вимкнена / порожня
    (немає списку РЕМ), а не «адреса не знайдена».
    """
    rems = _options(soup, "rem_id")
    if rems:
        return False
    # немає select rem_id або він порожній
    text = soup.get_text(" ", strip=True).lower()
    # якщо сторінка взагалі без форми пошуку
    if "unhooking" in text or "черг" in text or "рем" in text:
        return True
    # коротка заглушка / технічна сторінка
    if len(text) < 500 or "select" not in str(soup).lower():
        return True
    return True  # порожній список РЕМ = довідник недоступний


def _parse_result_table(soup: BeautifulSoup) -> list[dict]:
    """Парсить УСІ рядки результату (на одній вулиці може бути багато черг по будинках)."""
    results = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        header_idx = None
        headers = []
        for i, row in enumerate(rows):
            cols_raw = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if not cols_raw or cols_raw[0] != "РЕМ":
                continue
            if not any(c == "Черга" for c in cols_raw):
                continue
            header_idx = i
            headers = [c.lower() for c in cols_raw]
            break
        if header_idx is None:
            continue

        def idx(*names):
            for j, h in enumerate(headers):
                for n in names:
                    if n in h:
                        return j
            return None

        i_rem = idx("рем")
        i_city = idx("населен")
        i_street = idx("назв", "вулиц")
        i_houses = idx("будинк")
        i_queue = idx("черга")
        i_sub = idx("підчерг")
        i_note = idx("приміт")

        for row in rows[header_idx + 1 :]:
            cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cols) < 5 or cols[0] == "РЕМ":
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
            # Сітка графіка на тій же сторінці: рядок зі слотами 00:00-00:30 …
            # і рядки «1 / 1.1 / 1.2» без адреси — пропускаємо.
            if _is_time_token(houses) or _is_time_token(city) or _is_time_token(street):
                continue
            if not rem and not city and not street:
                continue
            if _is_time_token(queue) or _is_time_token(subqueue):
                continue
            # рядок сітки черг: rem="1"/"1.2", без вулиці й будинків
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
    """Слот графіка (01:00-01:30), не номер будинку."""
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
    return False


def _aggregate_street_rows(rows: list[dict]) -> dict:
    """
    З кількох рядків однієї вулиці збирає всі будинки і мапінг house → queue row.
    Якщо будинків немає — бере перший рядок як раніше.
    """
    if not rows:
        return {"error": "Записів черги не знайдено"}

    all_houses = []
    seen_h = set()
    house_to_row = {}
    for row in rows:
        tokens = _split_house_tokens(row.get("houses") or "")
        if not tokens:
            # рядок без номерів — запасний варіант «на всю вулицю»
            continue
        for tok in tokens:
            key = tok.strip().lower().replace(" ", "")
            if key not in seen_h:
                seen_h.add(key)
                all_houses.append(tok.strip())
            # перший рядок, де зустрівся цей будинок, вважаємо джерелом черги
            if key not in house_to_row:
                house_to_row[key] = row

    base = rows[0]
    if not all_houses:
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
        }

    # унікальні черги серед рядків
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
        "multi_queue": len({(
            (r.get("queue") or "").strip(),
            (r.get("subqueue") or "").strip(),
        ) for r in rows}) > 1,
    }


def resolve_queue_step(
    otg_name: str,
    city_name: str,
    street_name: str,
    rem_id: str = None,
    city_id: str = None,
    street_id: str = None,
) -> dict:
    """
    Покроковий пошук черги.
    Повертає один з варіантів:
      need_choice: "rem" | "city" | "street" + candidates: [(id, name), ...]
      ok: queue info
      error: повідомлення
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    }
    with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, headers=headers) as client:
        # --- РЕМ + населений пункт ---
        # У API power-on РЕМ часто «кривий» (Коростень → Хорошівський).
        # Тому: спочатку пробуємо otg_name, якщо не вийшло — шукаємо місто по всіх РЕМ.
        global _REMS_CACHE
        if _REMS_CACHE is not None:
            rems = _REMS_CACHE
            soup0 = None
        else:
            soup0 = _post(client, {})
            rems = _options(soup0, "rem_id")
            _REMS_CACHE = rems
        if not rems:
            return {
                "error": (
                    "Довідник черг тимчасово недоступний "
                    "(пошук адрес на сайті Житомиробленерго вимкнено). "
                    "Спробуй пізніше: /queue"
                ),
                "temporary": True,
            }

        rem_name = otg_name
        city_label = city_name

        def find_city_in_rem(rid: str):
            rid = str(rid)
            if rid in _CITY_OPTIONS_CACHE:
                cities = _CITY_OPTIONS_CACHE[rid]
                matches = _filter_matches(cities, city_name, min_score=0.5)
                return None, matches, cities
            soup = _post(client, {"rem_id": rid})
            cities = _options(soup, "naspunkt_id")
            _CITY_OPTIONS_CACHE[rid] = cities
            if not cities:
                return soup, [], cities
            matches = _filter_matches(cities, city_name, min_score=0.5)
            return soup, matches, cities

        if not rem_id:
            rem_query = re.sub(r"\s*рем\s*$", "", otg_name or "", flags=re.I).strip()
            # ігноруємо службові/загальні назви на кшталт АТ "Житомиробленерго"
            bad_rem = (
                not rem_query
                or "житомиробленерго" in rem_query.lower()
                or rem_query.lower() in ("ат", "пнб", "тзов")
            )
            rem_matches = [] if bad_rem else _filter_matches(rems, rem_query, min_score=0.5)
            exact_rem = [m for m in rem_matches if m[2] >= 0.99]
            use_rem = exact_rem if exact_rem else rem_matches

            found_via_otg = False
            if use_rem and not (len(use_rem) > 1 and not exact_rem):
                cand_rem_id, cand_rem_name = use_rem[0][0], use_rem[0][1]
                if not city_id:
                    _, city_matches, _ = find_city_in_rem(cand_rem_id)
                    exact_c = [m for m in city_matches if m[2] >= 0.99]
                    use_c = exact_c if exact_c else city_matches
                    if use_c and not (len(use_c) > 1 and (not exact_c or len(exact_c) > 1)):
                        rem_id, rem_name = cand_rem_id, cand_rem_name
                        city_id, city_label = use_c[0][0], use_c[0][1]
                        found_via_otg = True
                    elif use_c and len(use_c) > 1:
                        # кілька міст у «правильному» РЕМ — хай користувач обере
                        pool = exact_c if len(exact_c) > 1 else use_c
                        return {
                            "need_choice": "city",
                            "candidates": [(m[0], m[1]) for m in pool[:20]],
                            "rem_id": cand_rem_id,
                            "matched_rem": cand_rem_name,
                            "otg_name": otg_name,
                            "city_name": city_name,
                            "street_name": street_name,
                        }
                else:
                    rem_id, rem_name = cand_rem_id, cand_rem_name
                    found_via_otg = True
            elif use_rem and len(use_rem) > 1 and not exact_rem:
                # неоднозначний РЕМ за otg — спитаємо лише якщо не знайдемо місто інакше
                ambiguous_rems = use_rem

            if not found_via_otg and not city_id:
                # Fallback: шукаємо населений пункт у всіх РЕМ довідника (паралельно)
                
                def _scan(rid_rname):
                    rid, rname = rid_rname
                    try:
                        _, city_matches, _ = find_city_in_rem(rid)
                    except Exception:
                        return []
                    out = []
                    for cid, cname, sc in city_matches:
                        if sc >= 0.7:
                            out.append((sc, rid, rname, cid, cname))
                    return out

                cross = []
                for rr in rems:
                    cross.extend(_scan(rr))
                cross.sort(key=lambda x: -x[0])
                if not cross:
                    return {
                        "error": (
                            f"Населений пункт «{city_name}» не знайдено в довіднику черг"
                            + (f" (API РЕМ: {otg_name})" if otg_name else "")
                        ),
                    }
                # точні збіги назви
                exact_cross = [x for x in cross if x[0] >= 0.99]
                pool = exact_cross if exact_cross else cross[:15]
                # унікальні пари rem+city
                if len(pool) == 1 or (exact_cross and len({(x[1], x[3]) for x in exact_cross}) == 1):
                    sc, rem_id, rem_name, city_id, city_label = pool[0]
                else:
                    # кілька варіантів — показуємо як вибір «місто (РЕМ)»
                    seen = set()
                    cands = []
                    for sc, rid, rname, cid, cname in pool:
                        key = (rid, cid)
                        if key in seen:
                            continue
                        seen.add(key)
                        cands.append((f"{rid}|{cid}", f"{cname} ({rname})"))
                        if len(cands) >= 20:
                            break
                    return {
                        "need_choice": "city",
                        "candidates": cands,
                        "rem_id": None,  # прийде composite id
                        "matched_rem": None,
                        "otg_name": otg_name,
                        "city_name": city_name,
                        "street_name": street_name,
                        "composite_city_choice": True,
                    }
            elif not found_via_otg and not rem_id:
                return {"error": f"РЕМ не знайдено в довіднику черг: {otg_name}"}
        else:
            rem_name = otg_name or rem_name

        # city_id уже може бути заданий (у т.ч. після вибору користувача)
        if not city_id:
            _, city_matches, _ = find_city_in_rem(rem_id)
            exact = [m for m in city_matches if m[2] >= 0.99]
            use = exact if exact else city_matches
            if not use:
                return {
                    "error": f"Населений пункт не знайдено: {city_name}",
                    "matched_rem": rem_name,
                    "rem_id": rem_id,
                }
            if len(use) > 1 and (not exact or len(exact) > 1):
                pool = exact if len(exact) > 1 else use
                return {
                    "need_choice": "city",
                    "candidates": [(m[0], m[1]) for m in pool[:20]],
                    "rem_id": rem_id,
                    "matched_rem": rem_name,
                    "otg_name": otg_name,
                    "city_name": city_name,
                    "street_name": street_name,
                }
            city_id, city_label = use[0][0], use[0][1]
        else:
            city_label = city_name

        # --- Вулиця ---
        if not street_id:
            sk = (str(rem_id), str(city_id))
            if sk in _STREET_OPTIONS_CACHE:
                streets = _STREET_OPTIONS_CACHE[sk]
            else:
                soup = _post(client, {"rem_id": rem_id, "naspunkt_id": city_id})
                streets = _options(soup, "vulica_id")
                _STREET_OPTIONS_CACHE[sk] = streets
            if not streets:
                return {
                    "error": (
                        "Довідник черг тимчасово недоступний "
                        "(список вулиць порожній). Спробуй пізніше: /queue"
                    ),
                    "temporary": True,
                    "matched_rem": rem_name,
                    "matched_city": city_label,
                    "rem_id": rem_id,
                    "city_id": city_id,
                }
            matches = _filter_matches(streets, street_name, min_score=0.45)
            exact = [m for m in matches if m[2] >= 0.99]
            use = exact if exact else matches
            if not use:
                return {
                    "error": f"Вулицю не знайдено: {street_name}",
                    "matched_rem": rem_name,
                    "matched_city": city_label,
                    "rem_id": rem_id,
                    "city_id": city_id,
                }
            if len(use) > 1 and (not exact or len(exact) > 1):
                pool = exact if len(exact) > 1 else use
                return {
                    "need_choice": "street",
                    "candidates": [(m[0], m[1]) for m in pool[:20]],
                    "rem_id": rem_id,
                    "city_id": city_id,
                    "matched_rem": rem_name,
                    "matched_city": city_label,
                    "otg_name": otg_name,
                    "city_name": city_name,
                    "street_name": street_name,
                }
            street_id, street_label = use[0][0], use[0][1]
        else:
            street_label = street_name

        # --- Результат черги ---
        soup = _post(client, {
            "rem_id": rem_id,
            "naspunkt_id": city_id,
            "vulica_id": street_id,
        })
        rows = _parse_result_table(soup)
        if not rows:
            return {
                "error": "Записів черги не знайдено для цієї адреси",
                "matched_rem": rem_name,
                "matched_city": city_label,
                "matched_street": street_label,
            }

        agg = _aggregate_street_rows(rows)
        return {
            **agg,
            "matched_rem": rem_name if isinstance(rem_name, str) and rem_name else agg.get("matched_rem", ""),
            "matched_city": city_label if isinstance(city_label, str) and city_label else agg.get("matched_city", ""),
            "matched_street": street_label if isinstance(street_label, str) and street_label else agg.get("matched_street", ""),
            "rem_id": rem_id,
            "city_id": city_id,
            "street_id": street_id,
        }


def find_queue(otg_name: str, city_name: str, street_name: str) -> dict:
    """Сумісність зі старим API: одразу найкращий збіг або error."""
    result = resolve_queue_step(otg_name, city_name, street_name)
    if result.get("need_choice"):
        # беремо перший кандидат і добиваємо
        cands = result["candidates"]
        if result["need_choice"] == "rem":
            return resolve_queue_step(
                otg_name, city_name, street_name, rem_id=cands[0][0]
            )
        if result["need_choice"] == "city":
            return resolve_queue_step(
                otg_name, city_name, street_name,
                rem_id=result.get("rem_id"), city_id=cands[0][0],
            )
        if result["need_choice"] == "street":
            return resolve_queue_step(
                otg_name, city_name, street_name,
                rem_id=result.get("rem_id"), city_id=result.get("city_id"),
                street_id=cands[0][0],
            )
    if result.get("ok"):
        return result
    return result


def _is_no_gpv_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t or re.match(r"^[\d.]+$", t):
        return False
    return any(
        m in t
        for m in (
            "не вимикається",
            "не вимикають",
            "не відключається",
            "не відключають",
        )
    )


def format_queue_info_short(info: dict) -> str:
    """Тільки черга/помилка, без блоку «Деталі». Черга — у blockquote."""
    if info.get("error"):
        return f"❌ {info['error']}"
    q = info.get("queue") or "—"
    sq = info.get("subqueue") or ""
    if _is_no_gpv_text(q) or _is_no_gpv_text(sq):
        inner = "не вимикають, крім аварійних"
    elif sq and sq != q and len(sq) < 40 and re.match(r"^[\d.]+$", sq.strip()):
        inner = f"Черга: <b>{q}</b>, підчерга: <b>{sq}</b>"
    elif re.match(r"^[\d.]+$", (q or "").strip()):
        inner = f"Черга: <b>{q}</b>"
        if sq and re.match(r"^[\d.]+$", sq.strip()):
            inner += f", підчерга: <b>{sq}</b>"
    else:
        # один раз, без дубля queue+subqueue
        inner = q if len(q) < 80 else "не вимикають, крім аварійних"
    return f"<blockquote>{inner}</blockquote>"


def format_queue_info(info: dict) -> str:
    if info.get("error"):
        return f"❌ {info['error']}"

    q = info.get("queue") or "—"
    sq = info.get("subqueue") or ""
    if _is_no_gpv_text(q) or _is_no_gpv_text(sq):
        queue_line = "не вимикають, крім аварійних"
    elif sq and sq != q and len(sq) < 40 and re.match(r"^[\d.]+$", sq.strip()):
        queue_line = f"Черга: <b>{q}</b>, підчерга: <b>{sq}</b>"
    elif re.match(r"^[\d.]+$", q.strip()):
        queue_line = f"Черга: <b>{q}</b>" + (
            f", підчерга: <b>{sq}</b>" if sq and re.match(r"^[\d.]+$", sq.strip()) else ""
        )
    else:
        queue_line = q if len(q) < 80 else "не вимикають, крім аварійних"

    details = []
    if info.get("houses"):
        details.append(f"🏘 Будинки у довіднику: {info['houses']}")
    if info.get("note"):
        details.append(f"📝 {info['note']}")
    matched = []
    if info.get("matched_city"):
        matched.append(info["matched_city"])
    if info.get("matched_street"):
        matched.append(info["matched_street"])
    if matched:
        details.append(f"<i>Знайдено як: {', '.join(matched)}</i>")

    if not details:
        return queue_line

    # порожній рядок + згортаємий блок «Деталі» (клік у Telegram розкриває)
    details_block = (
        "\n\n<blockquote expandable><b>Деталі</b>\n"
        + "\n".join(details)
        + "</blockquote>"
    )
    return queue_line + details_block


def queue_fingerprint_info(info: dict) -> str:
    if info.get("error") or not (info.get("queue") or info.get("ok")):
        if info.get("error"):
            return ""
    return "|".join([
        (info.get("queue") or "").strip(),
        (info.get("subqueue") or "").strip(),
        (info.get("houses") or "").strip(),
        (info.get("note") or "").strip(),
    ])


def apply_house_to_result(result: dict, house: str) -> dict:
    """Підставляє чергу/підчергу/примітку з рядка довідника для конкретного будинку.
    Ідентично bot/services/address_service.py
    """
    out = {**result, "house": house}
    hmap = result.get("house_map") or {}
    key = (house or "").strip().lower().replace(" ", "")
    row = hmap.get(key)
    if row:
        out["queue"] = row.get("queue") or out.get("queue") or ""
        out["subqueue"] = row.get("subqueue") or out.get("subqueue") or ""
        out["note"] = row.get("note") or out.get("note") or ""
        out["houses"] = row.get("houses") or house
    return out


def apply_house_to_info(info: dict, house: str | None) -> dict:
    """Аліас для монітора: без будинку — street-level як у бота."""
    if not info.get("ok") and not info.get("queue") and info.get("error"):
        return info
    house = (house or "").strip()
    if not house:
        return info
    return apply_house_to_result(info, house)


# =============================================================================
# === КІНЕЦЬ queue_client ===
# =============================================================================


def address_key(otg_name: str, city_name: str, street_name: str) -> str:
    raw = f"{_norm(otg_name)}|{_norm(city_name)}|{_norm(street_name)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _is_no_gpv_text(text: str) -> bool:
    """Як bot/queue_client.py — статус «не вимикають / не відключають»."""
    t = (text or "").strip().lower()
    if not t or re.match(r"^[\d.]+$", t):
        return False
    return any(
        m in t
        for m in (
            "не вимикається",
            "не вимикають",
            "не відключається",
            "не відключають",
        )
    )


def canonicalize_queue(queue: str, subqueue: str) -> tuple[str, str]:
    """Та сама нормалізація, що в bot/handlers/add_address.py при збереженні адреси і /queue."""
    raw_q = str(queue or "").strip()
    raw_sq = str(subqueue or "").strip()
    if _is_no_gpv_text(raw_q) or _is_no_gpv_text(raw_sq):
        return "не вимикають, крім аварійних", ""
    if not re.fullmatch(r"[1-6](\.[12])?", raw_q):
        m = re.search(r"\b([1-6](?:\.[12])?)\b", raw_q)
        if m:
            raw_q = m.group(1)
        else:
            low = raw_q.lower()
            if (
                len(raw_q) > 12
                or "оберіть" in low
                or "рем" in low
                or "андруш" in low
            ):
                raw_q = ""
    if raw_sq and not re.fullmatch(r"[12]", raw_sq):
        raw_sq = ""
    return raw_q, raw_sq


def queue_fingerprint(queue: str, subqueue: str, houses: str, note: str) -> str:
    q, sq = canonicalize_queue(queue, subqueue)
    return "|".join([
        q,
        sq,
        (houses or "").strip(),
        (note or "").strip(),
    ])


def queue_number_fp(queue: str, subqueue: str) -> str:
    """Канонічний номер черги — те, що бачить користувач після /queue."""
    q, sq = canonicalize_queue(queue, subqueue)
    # «1» + підчерга «1» і суцільне «1.1» — один і той самий статус
    if re.fullmatch(r"[1-6]", q) and re.fullmatch(r"[12]", sq):
        return f"{q}.{sq}"
    if re.fullmatch(r"[1-6]\.[12]", q):
        return q
    return f"{q}|{sq}"


def _short_queue_label(queue: str, subqueue: str, note: str = "") -> str:
    q = (queue or "").strip()
    sq = (subqueue or "").strip()
    if not q:
        return "—"
    t = q.lower()
    if any(x in t for x in ("не вимика", "не відключ")):
        return "не вимикають, крім аварійних"
    if re.match(r"^[\d.]+$", q):
        if sq and re.match(r"^[\d.]+$", sq):
            return f"{q}.{sq}" if not q.endswith(f".{sq}") else q
        return q
    return q if len(q) < 60 else "не вимикають, крім аварійних"


def addr_matches_doc(addr: dict, doc: dict) -> bool:
    if addr.get("queue_key") and doc.get("_id") and addr["queue_key"] == doc["_id"]:
        return True
    return (
        _norm(addr.get("otg_name", "")) == _norm(doc.get("otg_name", ""))
        and _norm(addr.get("city_name", "")) == _norm(doc.get("city_name", ""))
        and _norm(addr.get("street_name", "") or addr.get("street", ""))
        == _norm(doc.get("street_name", ""))
    )

# Гілка «Перевірка черг» у лог-групі. Не публічний канал.
QUEUE_LOG_THREAD_ID = 155


def send_log(text: str, message_thread_id: int = QUEUE_LOG_THREAD_ID) -> None:
    """Підсумок у лог-групу ботом ZTOE (ZTOE_BOT_TOKEN + TELEGRAM_LOG_CHANNEL_ID).
    Той самий чат і гілка 155, що й звичайна перевірка черг. Користувачам це не йде.
    """
    channel = os.getenv("TELEGRAM_LOG_CHANNEL_ID") or os.getenv("ZTOE_LOG_CHANNEL_ID")
    log_token = os.getenv("ZTOE_BOT_TOKEN")
    if not channel:
        log.info("TELEGRAM_LOG_CHANNEL_ID не задано — пропуск лог-каналу")
        return
    if not log_token:
        log.info("ZTOE_BOT_TOKEN не задано — пропуск лог-каналу")
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
    """
    Схема:
      users (notify) → унікальні вулиці (otg|city|street)
        → якщо directory.last_check_at < SKIP_RECENT_MINUTES — skip мережі
        → інакше find_queue ОДИН раз на вулицю (кеш прогону)
        → apply_house_to_result для кожного будинку
        → оновлення addresses + notify лише при зміні queue/subqueue
    """
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB") or "vidkluchennya"
    bot_token = os.getenv("ZTOE_BOT_TOKEN")

    if not mongo_uri:
        log.error("Немає MONGO_URI")
        return 1
    if not bot_token:
        log.error("Немає ZTOE_BOT_TOKEN")
        return 1

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=15000)
    db = client[mongo_db]
    directory = db["queue_directory"]
    users_col = db["users"]

    clear_queue_http_cache()

    try:
        from bot_notify import user_wants as _user_wants, is_quiet_now as _is_quiet
    except Exception:
        def _user_wants(u, kind):
            # як у боті: notify не False → можна
            if not u:
                return False
            if u.get("notify", True) is False:
                return False
            return True

        def _is_quiet(_u):
            return False

    def _addr_list(u: dict) -> list:
        if isinstance(u.get("addresses"), list) and u["addresses"]:
            return [a for a in u["addresses"] if isinstance(a, dict)]
        if isinstance(u.get("address"), dict):
            return [u["address"]]
        return []

    def _city_of(a: dict) -> str:
        return (
            a.get("city_name")
            or a.get("city")
            or (a.get("cityName") if isinstance(a.get("cityName"), str) else "")
            or ""
        ).strip()

    def _street_of(a: dict) -> str:
        return (
            a.get("street_name")
            or a.get("street")
            or a.get("streetName")
            or ""
        ).strip()

    def _otg_of(a: dict) -> str:
        return (
            a.get("otg_name")
            or a.get("otg")
            or a.get("rem_name")
            or a.get("otgName")
            or ""
        ).strip()

    # notify: true АБО поле відсутнє (у боті default True)
    raw_users = list(
        users_col.find({"$or": [{"notify": True}, {"notify": {"$exists": False}}]})
    )
    # Фільтр hourly лише на етапі РОЗСИЛКИ; перевірку черг робимо для всіх з notify
    users = list(raw_users)

    streets: dict = {}
    skip_reasons: Counter = Counter()
    for u in users:
        addrs = _addr_list(u)
        if not addrs:
            skip_reasons["no_addresses"] += 1
            continue
        for idx, a in enumerate(addrs):
            street = _street_of(a)
            city = _city_of(a)
            otg = _otg_of(a)
            # лише черга без вулиці — з сайту не звірити
            if a.get("queue_only") and not street:
                skip_reasons["queue_only"] += 1
                continue
            if not street:
                skip_reasons["no_street"] += 1
                log.info("skip addr: no street")
                continue
            if not city:
                skip_reasons["no_city"] += 1
                log.info("skip addr: no city")
                continue
            key = address_key(otg, city, street)
            bucket = streets.setdefault(
                key,
                {"otg": otg, "city": city, "street": street, "pairs": []},
            )
            bucket["pairs"].append((u, idx, a))

    log.info(
        "users=%s streets_unique=%s skip=%s timeout=%ss pause=%ss skip_min=%s",
        len(users),
        len(streets),
        dict(skip_reasons),
        HTTP_TIMEOUT,
        REQUEST_PAUSE_SEC,
        SKIP_RECENT_MINUTES,
    )

    if not streets:
        detail = ", ".join(f"{k}={v}" for k, v in skip_reasons.most_common()) or "—"
        send_log(
            "✅ <b>Перевірка черг</b>\n\n"
            "Немає адрес з вулицею для перевірки.\n"
            f"Юзерів у вибірці: <b>{len(raw_users)}</b>\n"
            f"Причини пропуску: {detail}"
        )
        return 0

    bot = Bot(token=bot_token)
    now = datetime.now(timezone.utc)
    skip_delta = __import__("datetime").timedelta(minutes=SKIP_RECENT_MINUTES)

    checked = 0
    skipped_recent = 0
    skipped_temp = 0
    changed = 0
    notified_users = 0
    errors = 0
    error_msg_counts: Counter = Counter()
    temp_msg_counts: Counter = Counter()
    changed_cities: set[str] = set()
    changed_user_ids: set[int] = set()
    quiet_sync = os.getenv("ZTOE_QUEUE_SILENT", "").strip().lower() in ("1", "true", "yes")

    # run-level cache: key -> street_info dict
    run_cache: dict = {}

    for key, meta in streets.items():
        otg, city, street = meta["otg"], meta["city"], meta["street"]
        label = f"{otg} / {city} / {street}"
        pairs = meta["pairs"]

        # directory doc (для last_check_at + snapshot)
        doc = directory.find_one({"_id": key})
        if not doc:
            # legacy: шукати по назвах
            doc = directory.find_one(
                {
                    "otg_name": otg,
                    "city_name": city,
                    "street_name": street,
                }
            )

        # skip якщо цю вулицю вже перевіряли недавно (повторний dispatch)
        last_check = None
        if doc and doc.get("last_check_at"):
            last_check = doc["last_check_at"]
            if getattr(last_check, "tzinfo", None) is None:
                last_check = last_check.replace(tzinfo=timezone.utc)
        if last_check and (now - last_check) < skip_delta and key not in run_cache:
            # можна взяти snapshot з directory якщо є queue
            if doc and (doc.get("queue") is not None or doc.get("all_houses")):
                log.info("skip recent (%s min)", SKIP_RECENT_MINUTES)
                skipped_recent += 1
                # synthetic street_info з directory для per-house update без мережі
                street_info = {
                    "ok": True,
                    "queue": doc.get("queue") or "",
                    "subqueue": doc.get("subqueue") or "",
                    "houses": doc.get("houses") or "",
                    "note": doc.get("note") or "",
                    "all_houses": doc.get("all_houses") or [],
                    "house_map": doc.get("house_map") or {},
                    "rows": doc.get("rows") or [],
                    "matched_rem": doc.get("matched_rem"),
                    "matched_city": doc.get("matched_city"),
                    "matched_street": doc.get("matched_street"),
                    "_from_cache": True,
                }
                # якщо house_map порожній — без мережі точність по будинку низька; skip updates
                if not street_info.get("house_map") and not street_info.get("rows"):
                    continue
                run_cache[key] = street_info
            else:
                log.info("skip recent but no snapshot, will fetch")

        if key not in run_cache:
            try:
                street_info = find_queue(otg, city, street)
            except httpx.TimeoutException as e:
                log.warning("Timeout")
                directory.update_one(
                    {"_id": key},
                    {
                        "$set": {
                            "otg_name": otg,
                            "city_name": city,
                            "street_name": street,
                            "last_error": "timed out",
                            "last_check_at": now,
                        }
                    },
                    upsert=True,
                )
                skipped_temp += 1
                temp_msg_counts["timed out"] += 1
                continue
            except Exception as e:
                log.exception("Помилка запиту для %s: %s", label, e)
                directory.update_one(
                    {"_id": key},
                    {
                        "$set": {
                            "otg_name": otg,
                            "city_name": city,
                            "street_name": street,
                            "last_error": str(e),
                            "last_check_at": now,
                        }
                    },
                    upsert=True,
                )
                errors += 1
                error_msg_counts[str(e)] += 1
                continue

            if street_info.get("temporary"):
                msg = street_info.get("message") or street_info.get("error") or "temporary"
                log.warning("Тимчасово недоступно")
                directory.update_one(
                    {"_id": key},
                    {
                        "$set": {
                            "otg_name": otg,
                            "city_name": city,
                            "street_name": street,
                            "last_error": msg,
                            "last_check_at": now,
                        }
                    },
                    upsert=True,
                )
                skipped_temp += 1
                temp_msg_counts[str(msg)] += 1
                continue

            if street_info.get("error") and not street_info.get("ok"):
                msg = street_info.get("message") or street_info.get("error") or "unknown"
                log.warning("Не знайдено")
                directory.update_one(
                    {"_id": key},
                    {
                        "$set": {
                            "otg_name": otg,
                            "city_name": city,
                            "street_name": street,
                            "last_error": msg,
                            "last_check_at": now,
                        }
                    },
                    upsert=True,
                )
                errors += 1
                error_msg_counts[str(msg)] += 1
                continue

            # snapshot у directory (включно house_map для skip recent)
            # house_map values may have non-serializable stuff — store slim rows
            slim_map = {}
            for hk, row in (street_info.get("house_map") or {}).items():
                if isinstance(row, dict):
                    slim_map[hk] = {
                        "queue": row.get("queue") or "",
                        "subqueue": row.get("subqueue") or "",
                        "houses": row.get("houses") or "",
                        "note": row.get("note") or "",
                    }
            directory.update_one(
                {"_id": key},
                {
                    "$set": {
                        "otg_name": otg,
                        "city_name": city,
                        "street_name": street,
                        "queue": street_info.get("queue") or "",
                        "subqueue": street_info.get("subqueue") or "",
                        "houses": street_info.get("houses") or "",
                        "note": street_info.get("note") or "",
                        "all_houses": street_info.get("all_houses") or [],
                        "house_map": slim_map,
                        "matched_rem": street_info.get("matched_rem"),
                        "matched_city": street_info.get("matched_city"),
                        "matched_street": street_info.get("matched_street"),
                        "rem_id": street_info.get("rem_id"),
                        "city_id": street_info.get("city_id"),
                        "street_id": street_info.get("street_id"),
                        "updated_at": now,
                        "last_check_at": now,
                        "last_error": None,
                    }
                },
                upsert=True,
            )
            # підставити slim house_map назад
            street_info = {**street_info, "house_map": slim_map or street_info.get("house_map") or {}}
            run_cache[key] = street_info
            checked += 1
            log.info(
                "fetched houses=%s pairs=%s",
                len(street_info.get("all_houses") or []),
                len(pairs),
            )
        else:
            street_info = run_cache[key]

        # --- оновлення кожного user/addr на цій вулиці ---
        # group pairs by telegram_id to one write per user
        by_tid = {}
        for u, idx, a in pairs:
            tid = u.get("telegram_id")
            if tid is None:
                continue
            by_tid.setdefault(int(tid), {"user": u, "items": []})["items"].append((idx, a))

        for tid, pack in by_tid.items():
            user = pack["user"]
            addrs = list(
                user.get("addresses")
                or ([user["address"]] if user.get("address") else [])
            )
            # ensure list of dicts length
            if not isinstance(addrs, list):
                continue
            new_addrs = []
            # map index -> we only replace matching street addrs
            items_by_idx = {idx: a for idx, a in pack["items"]}
            notify_info = None
            notify_label_house = ""
            notify_old = {}
            touched = False

            for i, a in enumerate(addrs):
                if not isinstance(a, dict):
                    new_addrs.append(a)
                    continue
                if i not in items_by_idx and not (
                    _norm(a.get("otg_name", "")) == _norm(otg)
                    and _norm(a.get("city_name", "")) == _norm(city)
                    and _norm(a.get("street_name", "") or a.get("street", "")) == _norm(street)
                ):
                    new_addrs.append(a)
                    continue

                house = (a.get("house") or "").strip()
                info = apply_house_to_info(street_info, house)
                old_fp = a.get("queue_fp") or ""
                old_q = (a.get("queue") or "").strip()
                old_sq = (a.get("subqueue") or "").strip()
                new_q, new_sq = canonicalize_queue(
                    info.get("queue", ""),
                    info.get("subqueue", ""),
                )
                # порожній парсинг не затирає вже збережену чергу і не шле «зміну»
                if not new_q and old_q:
                    new_q, new_sq = canonicalize_queue(old_q, old_sq)
                new_fp = queue_fingerprint(
                    new_q,
                    new_sq,
                    house or (info.get("houses") or ""),
                    info.get("note", ""),
                )

                updated = {
                    **a,
                    "queue": new_q,
                    "subqueue": new_sq,
                    "no_gpv": new_q == "не вимикають, крім аварійних",
                    "queue_houses": info.get("houses", ""),
                    "queue_note": info.get("note", ""),
                    "queue_fp": new_fp,
                    "queue_updated_at": now.isoformat(),
                    "queue_key": key,
                }
                if house:
                    updated["house"] = house
                new_addrs.append(updated)
                touched = True

                if old_fp and queue_number_fp(old_q, old_sq) != queue_number_fp(new_q, new_sq):
                    changed += 1
                    changed_cities.add(city)
                    changed_user_ids.add(int(tid))
                    notify_info = {**info, "queue": new_q, "subqueue": new_sq}
                    notify_label_house = house
                    notify_old = {
                        "queue": old_q,
                        "subqueue": old_sq,
                        "note": a.get("queue_note") or "",
                    }
                    log.info("Зміна черги (адресу в лог не пишемо)")
                elif not old_fp and new_fp:
                    log.info("Перша фіксація черги (без notify, адресу в лог не пишемо)")

            if not touched:
                continue

            # Наступна вулиця цього юзера інакше бере знімок з початку прогону
            # і update_one затирає щойно записані черги.
            user["addresses"] = new_addrs
            users_col.update_one(
                {"telegram_id": tid},
                {"$set": {"addresses": new_addrs}, "$unset": {"address": ""}},
            )

            if notify_info is None or quiet_sync:
                continue
            if not _user_wants(user, "hourly"):
                continue

            house_bit = f", буд. {notify_label_house}" if notify_label_house else ""
            was = _short_queue_label(
                notify_old.get("queue", ""),
                notify_old.get("subqueue", ""),
                notify_old.get("note", ""),
            )
            now_lbl = _short_queue_label(
                notify_info.get("queue", ""),
                notify_info.get("subqueue", ""),
                notify_info.get("note", ""),
            )
            if was == now_lbl:
                log.info("Підпис не змінився, notify пропущено")
                continue
            text_msg = (
                "🔔 <b>Змінилася черга відключень:</b>\n\n"
                f"<b>{city}, {street}{house_bit}</b>\n\n"
                f"Було: <b>{was}</b>\n"
                f"Стало: <b>{now_lbl}</b>"
            )
            silent = _is_quiet(user)
            try:
                async def _send(chat_id=tid, body=text_msg, silent=silent):
                    await bot.send_message(
                        chat_id=chat_id,
                        text=body[:4096],
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        disable_notification=silent,
                    )

                asyncio.run(_send())
                notified_users += 1
                log.info("Надіслано silent=%s", silent)
                time.sleep(0.35)
            except Exception as e:
                log.error("Не вдалося надіслати сповіщення")

    if quiet_sync:
        cities = sorted(changed_cities, key=lambda s: s.lower())
        city_block = "\n".join(f"• {c}" for c in cities) if cities else "—"
        summary = (
            "✅ <b>Тихе оновлення черг</b>\n\n"
            f"Користувачів, у кого замінили чергу: <b>{len(changed_user_ids)}</b>\n"
            f"Адрес зі зміненою чергою: <b>{changed}</b>\n"
            f"Населених пунктів: <b>{len(cities)}</b>\n"
            f"{city_block}\n\n"
            "Користувачам нічого не надіслано.\n"
            f"Запитів на сайт: <b>{checked}</b>\n"
            f"Помилок: <b>{errors}</b>\n"
            f"Сайт тимчасово недоступний: <b>{skipped_temp}</b>"
        )
    else:
        summary = (
            f"✅ <b>Перевірка черг</b>\n\n"
            f"Підписників: <b>{len(users)}</b>\n"
            f"Унікальних вулиць: <b>{len(streets)}</b>\n"
            f"Запитів на сайт: <b>{checked}</b>\n"
            f"Пропущено (недавно): <b>{skipped_recent}</b>\n"
            f"Черга змінилась: <b>{changed}</b>\n"
            f"Сповіщено людей: <b>{notified_users}</b>\n"
            f"Сайт тимчасово недоступний: <b>{skipped_temp}</b>\n"
            f"Помилок: <b>{errors}</b>"
        )
    if error_msg_counts:
        summary += "\n\n<b>Помилки</b>\n" + "\n".join(
            f"{m}: {c}" for m, c in error_msg_counts.most_common()
        )
    if temp_msg_counts:
        summary += "\n\n<b>Тимчасово</b>\n" + "\n".join(
            f"{m}: {c}" for m, c in temp_msg_counts.most_common()
        )

    log.info(
        "Готово users=%s streets=%s fetched=%s skip_recent=%s changed=%s notified=%s temp=%s err=%s",
        len(users), len(streets), checked, skipped_recent, changed, notified_users, skipped_temp, errors,
    )
    send_log(summary, message_thread_id=QUEUE_LOG_THREAD_ID)
    return 0


if __name__ == "__main__":
    sys.exit(main())
