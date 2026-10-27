#!/usr/bin/env python3
"""
Аварійні / планові відключення ЖОЕ → підписники бота.

Оптимізації:
- один fetch на otg_id (без city), фільтр міста в пам'яті
- спільний httpx.Client
- bulk_write у Mongo замість поштучних update_one
- без подвійного retry empty+city

Cleanup (щоб база не роздувалась):
- outages: delete документи з updated_at старші за OUTAGES_RETENTION_HOURS (default 48)
- outages_cache: soft_fps обрізається до SOFT_FPS_MAX (default 3000);
  документи cache, які давно не оновлювались (OUTAGES_CACHE_RETENTION_DAYS, default 30),
  видаляються (застарілі групи адрес)

Env:
  OUTAGES_RETENTION_HOURS      — TTL повних записів у outages (default 48)
  SOFT_FPS_MAX                 — макс. розмір soft_fps у cache (default 3000)
  OUTAGES_CACHE_RETENTION_DAYS — TTL неактивних ключів cache (default 30)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import httpx
from pymongo import MongoClient, UpdateOne
from telegram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ztoe_outages_monitor")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_BASE = (os.getenv("ZTOE_API_BASE") or os.getenv("API_BASE") or "").rstrip("/")
if not API_BASE:
    raise SystemExit("Немає ZTOE_API_BASE")

# --- retention / caps (не чіпають queue_schedules, users, ГПВ) ---
OUTAGES_RETENTION_HOURS = int(os.getenv("OUTAGES_RETENTION_HOURS", "48"))
SOFT_FPS_MAX = int(os.getenv("SOFT_FPS_MAX", "3000"))
OUTAGES_CACHE_RETENTION_DAYS = int(os.getenv("OUTAGES_CACHE_RETENTION_DAYS", "30"))


def merge_soft_fps(current_soft: Set[str], prev_soft: Set[str], limit: int = SOFT_FPS_MAX) -> List[str]:
    """
    Об'єднує soft_fps, але не дає масиву рости безмежно.
    Поточні fingerprint'и з API завжди зберігаються;
    зі старих добираємо до limit.
    """
    if limit <= 0:
        return list(current_soft)
    if len(current_soft) >= limit:
        return list(current_soft)[:limit]
    extra = list(prev_soft - current_soft)
    need = limit - len(current_soft)
    return list(current_soft) + extra[:need]

TYPE_LABELS = {
    0: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
    1: ("Аварійне", "несподівана аварія в мережі"),
    2: ("Оперативне", "технічні перемикання в мережі"),
    3: ("Планове", "заплановані роботи (ремонт, обслуговування)"),
}

# switchSubReason з API ЖОЕ (як на сайті / у JS power-on):
#   0 — звичайне (дивись typeOff)
#   1 — ГАВ (аварійний графік)
#   2 — ГПВ (погодинні) ← НЕ аварія, не нотифікуємо
#   3 — СГАВ
SUBREASON_GPV = 2
SUBREASON_GAV = 1
SUBREASON_SGAV = 3


def type_family(type_off) -> str:
    try:
        t = int(type_off)
    except (TypeError, ValueError):
        return "x"
    if t == 1:
        return "emerg"
    if t in (0, 3):
        return "planned"
    if t == 2:
        return "oper"
    return str(t)


def _switch_sub_reason(outage: dict) -> int:
    try:
        return int(outage.get("switchSubReason") or 0)
    except (TypeError, ValueError):
        return 0


def is_gpv_outage(outage: dict) -> bool:
    """
    Графік погодинних відключень (ГПВ), не аварія.
    На сайті: «Графік погодинних відключень», switchSubReason=2.
    typeOff при цьому часто = 1 — через це раніше слали «🔴 Аварійне».
    """
    if _switch_sub_reason(outage) == SUBREASON_GPV:
        return True
    koment = (outage.get("koment") or "").upper()
    if "ГПВ" in koment:
        return True
    return False


def should_notify_outage(outage: dict) -> bool:
    """
    Сповіщати лише про реальні події поза звичайним ГПВ:
      - ГПВ (sub=2) → ні (це графік, не сюрприз)
      - планове / аварійне / оперативне / ГАВ / СГАВ → так
    """
    if is_gpv_outage(outage):
        return False
    return True


def outage_notify_label(outage: dict) -> tuple:
    """(icon, title, desc) для тексту сповіщення."""
    sub = _switch_sub_reason(outage)
    if is_gpv_outage(outage):
        return ("⏰", "Погодинне (ГПВ)", "графік погодинних відключень")
    if sub in (SUBREASON_GAV, SUBREASON_SGAV):
        return ("🔴", "Аварійне (ГАВ)" if sub == SUBREASON_GAV else "Аварійне (СГАВ)",
                "аварійний графік відключень")
    type_off = outage.get("typeOff")
    try:
        t = int(type_off)
    except (TypeError, ValueError):
        t = -1
    name, desc = TYPE_LABELS.get(t, (f"Тип {type_off}", ""))
    icon = {0: "🟠", 1: "🔴", 2: "🟡", 3: "🟠"}.get(t, "⚪")
    return (icon, name, desc)


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def _fetch_all_pages(
    client: httpx.Client,
    path: str,
    otg_id: int,
    city_id: Optional[int] = None,
) -> List[dict]:
    items: List[dict] = []
    page = 1
    while page <= 40:
        params: dict = {"page": page, "itemsPerPage": 100, "otg.id": otg_id}
        if city_id:
            params["city.id"] = city_id
        r = client.get(f"{API_BASE}{path}", params=params)
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
    return items


def fetch_all_outages(
    client: httpx.Client,
    otg_id: int,
    city_id: Optional[int] = None,
) -> List[dict]:
    accidents = _fetch_all_pages(client, "/pw_accidents", otg_id, city_id)
    try:
        planned = _fetch_all_pages(client, "/pw_accident_planeds", otg_id, city_id)
    except Exception as e:
        log.warning("planeds error otg=%s city=%s: %s", otg_id, city_id, e)
        planned = []
    for a in accidents:
        a["_source"] = "accidents"
    for p in planned:
        p["_source"] = "planeds"
    return accidents + planned


def filter_by_city(outages: List[dict], city_id: int) -> List[dict]:
    """Якщо city_id=0 — повертаємо все. Інакше лишаємо збіг або без city (плани часто без міста)."""
    if not city_id:
        return outages
    result = []
    for o in outages:
        cid = (o.get("city") or {}).get("id")
        if cid is None or int(cid) == int(city_id):
            result.append(o)
    return result


def make_outage_key(outage: dict) -> str:
    return f"{outage.get('_source', 'x')}:{outage.get('id')}"


def content_fingerprint(outage: dict) -> str:
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    start = (outage.get("dateEvent") or "")[:13]
    fam = type_family(outage.get("typeOff"))
    city = (outage.get("city") or {}).get("id") or ""
    return f"{city}|{sid}|{','.join(parts)}|{start}|{fam}"


def soft_fingerprint(outage: dict) -> str:
    street = outage.get("street") or {}
    sid = street.get("id") or (street.get("name") or "").strip().lower()
    buildings = (outage.get("buildingNames") or "").strip().lower()
    parts = sorted({p.strip() for p in buildings.replace(";", ",").split(",") if p.strip()})
    start = (outage.get("dateEvent") or "")[:13]
    city = (outage.get("city") or {}).get("id") or ""
    return f"{city}|{sid}|{','.join(parts)}|{start}"


def format_outage(outage: dict) -> str:
    city = (outage.get("city") or {}).get("name", "?")
    street = (outage.get("street") or {}).get("name", "?")
    buildings = (outage.get("buildingNames") or "").strip()
    date_event = (outage.get("dateEvent") or "")[:16].replace("T", " ")
    date_plan = (outage.get("datePlanIn") or "")[:16].replace("T", " ")
    icon, type_name, type_desc = outage_notify_label(outage)
    text = f"{icon} <b>{city}, {street}</b>\n📌 {type_name}"
    if type_desc:
        text += f" — {type_desc}"
    if buildings:
        text += f"\n🏘 Будинки: {buildings}"
    if date_event:
        text += f"\n⏰ З: {date_event}"
    if date_plan:
        text += f"\n⏳ До (план): {date_plan}"
    koment = (outage.get("koment") or "").strip()
    if koment:
        text += f"\n💬 {koment}"
    return text


def _norm_street(name: str) -> str:
    s = (name or "").strip().lower()
    for pfx in ("вул.", "вул ", "пров.", "провулок ", "проспект ", "пр.", "бульвар ", "пл.", "площа "):
        if s.startswith(pfx):
            s = s[len(pfx) :].strip()
    return s


def matches_street(outage: dict, street_id, street_name: str) -> bool:
    o_street = outage.get("street") or {}
    o_street_id = o_street.get("id")
    o_street_name = (o_street.get("name") or "").lower()
    if street_id and o_street_id is not None and str(o_street_id) == str(street_id):
        return True
    a = _norm_street(street_name)
    b = _norm_street(o_street_name)
    if a and b and (a in b or b in a):
        return True
    if street_name and street_name.lower() in o_street_name:
        return True
    if street_name and o_street_name and o_street_name in street_name.lower():
        return True
    return False


def matches_building(outage: dict, house: str) -> bool:
    house = (house or "").strip()
    if not house:
        return True
    buildings = (outage.get("buildingNames") or "").strip()
    if not buildings:
        return True
    tokens = {p.strip().lower() for p in buildings.replace(";", ",").split(",") if p.strip()}
    h = house.lower()
    if h in tokens:
        return True
    h2 = h.lstrip("0") or "0"
    return h2 in tokens or any(t.lstrip("0") == h2 for t in tokens)


def outage_to_mongo_doc(outage: dict, queue_key: str = "") -> dict:
    from outage_events import parse_queue_from_koment

    city = outage.get("city") or {}
    street = outage.get("street") or {}
    otg = outage.get("otg") or {}
    qk = queue_key or parse_queue_from_koment(outage.get("koment") or "") or ""
    ch, pid = "", ""
    if qk:
        parts = qk.replace("|", ".").split(".")
        ch = parts[0]
        pid = parts[1] if len(parts) > 1 else ""
    return {
        "source_id": outage.get("id"),
        "source": outage.get("_source"),
        "typeOff": outage.get("typeOff"),
        # 0 звичайне; 1 ГАВ; 2 ГПВ (не аварія!); 3 СГАВ — критично для /check
        "switchSubReason": outage.get("switchSubReason"),
        "city_id": city.get("id"),
        "city_name": city.get("name"),
        "street_id": street.get("id"),
        "street_name": street.get("name"),
        "otg_id": otg.get("id"),
        "otg_name": otg.get("name"),
        "buildingNames": outage.get("buildingNames"),
        "dateEvent": outage.get("dateEvent"),
        "datePlanIn": outage.get("datePlanIn"),
        "koment": outage.get("koment"),
        "queue_key": qk,
        "queue": ch,
        "subqueue": pid,
        "fingerprint": content_fingerprint(outage),
        "soft_fp": soft_fingerprint(outage),
        "updated_at": datetime.now(timezone.utc),
    }


def user_queue_key(addr: dict) -> str:
    q = (addr.get("queue") or "").strip()
    sq = (addr.get("subqueue") or "").strip()
    if not q:
        return ""
    if "." in q or "|" in q:
        return q.replace("|", ".")
    return f"{q}.{sq}" if sq else q


def main() -> int:
    mongo_uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not mongo_uri:
        log.error("Немає MONGO_URI")
        return 1
    db_name = os.getenv("ZTOE_MONGO_DB") or os.getenv("MONGO_DB") or "vidkluchennya"
    bot_token = os.getenv("ZTOE_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        log.error("Немає ZTOE_BOT_TOKEN")
        return 1

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    users_col = db["users"]
    outages_col = db["outages_cache"]
    outages_full = db["outages"]

    users = list(users_col.find({"notify": True}))
    groups: Dict[Tuple[int, int], List[Tuple[dict, dict]]] = {}
    for u in users:
        for addr in get_user_addresses(u):
            if not isinstance(addr, dict):
                continue
            otg_id = addr.get("otg_id") or addr.get("otgId")
            if not otg_id:
                continue
            city_id = addr.get("city_id") or addr.get("cityId") or 0
            try:
                otg_id = int(otg_id)
                city_id = int(city_id) if city_id else 0
            except (TypeError, ValueError):
                continue
            groups.setdefault((otg_id, city_id), []).append((u, addr))

    unique_otgs = sorted({otg for otg, _ in groups})
    log.info("users=%s groups=%s unique_otgs=%s", len(users), len(groups), len(unique_otgs))

    bot = Bot(token=bot_token)
    notified: Dict[int, list] = {}
    groups_checked = 0

    # Один HTTP-клієнт на весь прогін
    http = httpx.Client(
        timeout=30,
        headers={
            "Accept": "application/ld+json",
            "User-Agent": "ztoe-outages-monitor/1.3",
        },
    )

    # Кеш fetch по otg (без city) — головна економія при кількох city в одній ОТГ
    otg_outages_cache: Dict[int, List[dict]] = {}

    try:
        for otg_id in unique_otgs:
            try:
                otg_outages_cache[otg_id] = fetch_all_outages(http, otg_id, None)
                log.info("otg=%s fetched_total=%s", otg_id, len(otg_outages_cache[otg_id]))
            except Exception as e:
                log.error("Fetch otg=%s: %s", otg_id, e)
                otg_outages_cache[otg_id] = []

        for (otg_id, city_id), group_pairs in groups.items():
            raw = otg_outages_cache.get(otg_id) or []
            outages = filter_by_city(raw, city_id)

            groups_checked += 1
            current_ids = {make_outage_key(o) for o in outages}
            current_fps = {content_fingerprint(o) for o in outages}
            current_soft = {soft_fingerprint(o) for o in outages}

            cache_key = f"{otg_id}_{city_id or 0}"
            cache = outages_col.find_one({"_id": cache_key}) or {}
            prev_fps = set(cache.get("fps") or [])
            prev_soft = set(cache.get("soft_fps") or [])

            new_fps = current_fps - prev_fps
            new_outages = [o for o in outages if content_fingerprint(o) in new_fps]

            filtered = []
            for o in new_outages:
                # ГПВ (switchSubReason=2) — це графік, не «нова аварія»
                if not should_notify_outage(o):
                    log.info(
                        "skip GPV/non-notify id=%s sub=%s type=%s",
                        o.get("id"),
                        o.get("switchSubReason"),
                        o.get("typeOff"),
                    )
                    continue
                soft = soft_fingerprint(o)
                if type_family(o.get("typeOff")) == "planned" and soft in prev_soft:
                    log.info("skip planned after known soft=%s", soft)
                    continue
                filtered.append(o)
            new_outages = filtered

            log.info(
                "otg=%s city=%s fetched=%s cache_fps=%s new=%s",
                otg_id,
                city_id,
                len(outages),
                len(prev_fps),
                len(new_outages),
            )

            # Bulk upsert full docs (для /check)
            if outages:
                ops = []
                for o in outages:
                    doc = outage_to_mongo_doc(o)
                    ops.append(
                        UpdateOne(
                            {"source": doc["source"], "source_id": doc["source_id"]},
                            {"$set": doc},
                            upsert=True,
                        )
                    )
                if ops:
                    # chunk на випадок дуже великих списків
                    for i in range(0, len(ops), 500):
                        outages_full.bulk_write(ops[i : i + 500], ordered=False)

            matched_fps: set = set()

            if new_outages:
                for user, addr in group_pairs:
                    tid = user.get("telegram_id")
                    if not tid:
                        continue
                    street_id = addr.get("street_id") or addr.get("streetId")
                    street_name = (addr.get("street_name") or addr.get("street") or "").strip().lower()
                    house = addr.get("house") or addr.get("building") or ""
                    uqk = user_queue_key(addr)

                    # queue_key на full docs тільки для зматчених адрес
                    if uqk:
                        queue_ops = []
                        for outage in outages:
                            if not matches_street(outage, street_id, street_name):
                                continue
                            if not matches_building(outage, str(house)):
                                continue
                            doc = outage_to_mongo_doc(outage, queue_key=uqk)
                            queue_ops.append(
                                UpdateOne(
                                    {"source": doc["source"], "source_id": doc["source_id"]},
                                    {"$set": doc},
                                    upsert=True,
                                )
                            )
                        if queue_ops:
                            outages_full.bulk_write(queue_ops, ordered=False)

                    for outage in new_outages:
                        if not matches_street(outage, street_id, street_name):
                            continue
                        if not matches_building(outage, str(house)):
                            continue
                        if uqk:
                            outage["_user_queue"] = uqk
                        notified.setdefault(tid, [])
                        fp = content_fingerprint(outage)
                        if any(content_fingerprint(x) == fp for x in notified[tid]):
                            continue
                        notified[tid].append(outage)
                        matched_fps.add(fp)

                log.info(
                    "otg=%s city=%s matched_new=%s (addrs=%s)",
                    otg_id,
                    city_id,
                    len(matched_fps),
                    len(group_pairs),
                )
                if new_outages and not matched_fps:
                    sample = []
                    for o in new_outages[:5]:
                        st = (o.get("street") or {}).get("name") or "?"
                        bld = (o.get("buildingNames") or "")[:30]
                        sample.append(f"{st}|{bld}|type={o.get('typeOff')}")
                    log.info("no street match; API sample: %s", " || ".join(sample))
                    for user, addr in group_pairs:
                        log.info(
                            "  user street=%r id=%s house=%r",
                            addr.get("street_name") or addr.get("street"),
                            addr.get("street_id") or addr.get("streetId"),
                            addr.get("house"),
                        )

            # Кеш fps (soft_fps з капом, щоб документ не товстішав роками)
            if prev_fps or matched_fps or not new_outages:
                soft_stored = merge_soft_fps(current_soft, prev_soft, SOFT_FPS_MAX)
                outages_col.update_one(
                    {"_id": cache_key},
                    {
                        "$set": {
                            "ids": list(current_ids),
                            "fps": list(current_fps),
                            "soft_fps": soft_stored,
                            "updated_at": datetime.now(timezone.utc),
                            "count": len(current_ids),
                        }
                    },
                    upsert=True,
                )
            else:
                log.warning(
                    "otg=%s city=%s: new=%s but 0 matched — cache NOT updated (можна виправити адреси і повторити)",
                    otg_id,
                    city_id,
                    len(new_outages),
                )
    finally:
        http.close()

    # --- cleanup: тільки outages + outages_cache; ГПВ/users не чіпаємо ---
    try:
        if OUTAGES_RETENTION_HOURS > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=OUTAGES_RETENTION_HOURS)
            del_out = outages_full.delete_many({"updated_at": {"$lt": cutoff}})
            log.info(
                "outages cleanup: deleted=%s older_than=%sh",
                del_out.deleted_count,
                OUTAGES_RETENTION_HOURS,
            )
            # Сироти: typeOff=1 без switchSubReason (старий спам «аварій» 21–24 тощо).
            # Після upsert цього прогону актуальні вже мають switchSubReason.
            del_legacy = outages_full.delete_many(
                {
                    "typeOff": 1,
                    "$or": [
                        {"switchSubReason": {"$exists": False}},
                        {"switchSubReason": None},
                    ],
                }
            )
            log.info(
                "outages cleanup legacy typeOff=1 without switchSubReason: deleted=%s",
                del_legacy.deleted_count,
            )
        if OUTAGES_CACHE_RETENTION_DAYS > 0:
            cache_cutoff = datetime.now(timezone.utc) - timedelta(
                days=OUTAGES_CACHE_RETENTION_DAYS
            )
            del_cache = outages_col.delete_many({"updated_at": {"$lt": cache_cutoff}})
            log.info(
                "outages_cache cleanup: deleted=%s older_than=%sd",
                del_cache.deleted_count,
                OUTAGES_CACHE_RETENTION_DAYS,
            )
    except Exception as e:
        log.warning("cleanup failed (non-fatal): %s", e)


    sent = 0
    for tid, outage_list in notified.items():
        text = "⚠️ <b>Нові відключення по твоїх адресах:</b>\n\n"
        text += "\n\n".join(format_outage(o) for o in outage_list[:10])

        async def _send(chat_id=tid, body=text):
            await bot.send_message(
                chat_id=chat_id,
                text=body[:4096],
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

        try:
            asyncio.run(_send())
            sent += 1
            log.info("Надіслано %s (outages=%s)", tid, len(outage_list))
            time.sleep(0.4)
        except Exception as e:
            log.error("Send fail %s: %s", tid, e)

    n_events = sum(len(v) for v in notified.values())
    summary = (
        f"✅ Перевірка відключень\n"
        f"Підписників: {len(users)}\n"
        f"Локацій: {groups_checked}\n"
        f"Подій для розсилки: {n_events}\n"
        f"Сповіщень: {sent}"
    )
    log.info("%s", summary.replace("\n", " | "))

    log_chat = (
        os.getenv("TELEGRAM_LOG_CHANNEL_ID")
        or os.getenv("ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
    )
    if log_chat:
        try:

            async def _log():
                await bot.send_message(chat_id=int(log_chat), text=summary)

            asyncio.run(_log())
        except Exception as e:
            log.warning("log channel: %s", e)

    log.info("Готово groups=%s sent=%s events=%s", groups_checked, sent, n_events)
    return 0


if __name__ == "__main__":
    sys.exit(main())
