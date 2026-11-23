#!/usr/bin/env python3
"""
Тестовий «минулий» стан ремонтів для двох адрес, щоб ztoe_outages_monitor
побачив зміну періоду і надіслав «⏱ Зміна періоду».

Що робить:
  1) Знаходить підписників з адресами:
       - Коростень, вул. Білокоровицьке Шосе, 20
       - Вересна, вул. Лесі Українки, 3
  2) Для їх (otg_id, city_id) пише в outages_cache snapshot + plan_by_addr_day
     зі СТАРИМИ інтервалами (іншими, ніж зараз у API).
  3) Пише відповідні документи в outages (зліпок для /check і bootstrap).
  4) Чистить user_outage_notified по цих telegram_id (інакше дедуп з’їсть
     повторну розсилку).

НЕ чіпає: queue_schedules, users, ukrenergo*, accidents_summary, site_meta.

Після запуску обов’язково:
  python ztoe_outages_monitor.py
  (workflow «ZTOE outages»)

  python ztoe_outages_force_diff.py
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient, UpdateOne

# --- цільові адреси (нормалізовані) ---
# (city_substr, street_substr, house, fake_start, fake_end, day)
# fake_* — «було» у сповіщенні; API зараз віддає інші години → monitor побачить зміну.
TARGETS = [
    {
        "label": "Коростень, Білокоровицьке Шосе, 20",
        "city_sub": "коростень",
        "street_subs": ("білокоровицьк", "білокровицьк"),
        "house": "20",
        # API зараз ~08:00–18:00 (план 08–17 + accident до 18) → «було» коротше
        "fake_start": "2026-11-23T08:00:00",
        "fake_end": "2026-11-23T16:00:00",
        "day": "2026-11-23",
        # реалістичні id з API (для snapshot/outages)
        "fallback_city_id": 430,
        "fallback_otg_id": 7,
        "fallback_street_id": 7640,
        "fallback_street_name": "вул. Білокоровицьке Шосе",
        "fallback_city_name": "Коростень",
        "fallback_otg_name": "Коростенський РЕМ",
        "fallback_buildings": (
            "0, 1, 10, 12, 12 А, 12 Б, 14, 2, 20, 20 А, 21, 23, 23-А, 24, 25, "
            "27, 27 А, 29, 2А, 3, 31, 33, 35, 37, 4, 5, 6, 6 Б, 6-8, 7, 8, 8 А, КТП-206"
        ),
        "typeOff": 3,
    },
    {
        "label": "Вересна, Лесі Українки, 3",
        "city_sub": "вересна",
        "street_subs": ("лесі україн", "л.ukrain", "л.україн", "українк"),
        "house": "3",
        # API зараз 12:00–16:00 → «було» інше
        "fake_start": "2026-11-23T09:00:00",
        "fake_end": "2026-11-23T14:00:00",
        "day": "2026-11-23",
        "fallback_city_id": 105,
        "fallback_otg_id": 2,
        "fallback_street_id": 1486,
        "fallback_street_name": "вул. Лесі Українки",
        "fallback_city_name": "Вересна",
        "fallback_otg_name": "Баранівський РЕМ",
        "fallback_buildings": "3",
        "typeOff": 0,
    },
]


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    for pfx in (
        "вул.",
        "вул ",
        "вулиця ",
        "пров.",
        "провулок ",
        "проспект ",
        "пр.",
        "пр-к ",
        "бульвар ",
        "б-р ",
        "пл.",
        "площа ",
        "м.",
        "м ",
    ):
        if s.startswith(pfx):
            s = s[len(pfx) :].strip()
    s = s.replace("'", "").replace("’", "").replace("`", "")
    return " ".join(s.split())


def _parse_house(h: str) -> Tuple[str, str]:
    h = (h or "").strip().lower().replace(" ", "").replace("-", "")
    m = re.match(r"^0*(\d+)([а-яa-z]*)$", h, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)
    return h, ""


def house_match(user_house: str, target_house: str) -> bool:
    a_num, a_suf = _parse_house(user_house)
    b_num, b_suf = _parse_house(target_house)
    if a_num != b_num:
        return False
    if not b_suf or a_suf == b_suf:
        return True
    return False


def street_match(street_name: str, subs: Tuple[str, ...]) -> bool:
    n = _norm(street_name)
    return any(sub in n for sub in subs)


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def addr_day_key(
    city_id: Any,
    street_id: Any,
    street_name: str,
    buildings: str,
    day: str,
) -> str:
    sid = street_id or _norm(street_name)
    parts = sorted(
        {p.strip() for p in (buildings or "").replace(";", ",").split(",") if p.strip()}
    )
    return f"{city_id or ''}|{sid}|{','.join(p.lower() for p in parts)}|{day}"


def snapshot_rec(
    *,
    source: str,
    source_id: Any,
    type_off: int,
    city_id: int,
    city_name: str,
    street_id: int,
    street_name: str,
    otg_id: int,
    otg_name: str,
    buildings: str,
    date_event: str,
    date_plan: str,
) -> dict:
    key = f"{source}:{source_id}"
    return {
        "key": key,
        "id": source_id,
        "_source": source,
        "typeOff": type_off,
        "switchSubReason": 0,
        "city": {"id": city_id, "name": city_name},
        "street": {"id": street_id, "name": street_name},
        "otg": {"id": otg_id, "name": otg_name},
        "buildingNames": buildings,
        "dateEvent": date_event,
        "datePlanIn": date_plan,
        "koment": "тестовий попередній графік (force_diff)",
    }


def outage_mongo_doc(rec: dict) -> dict:
    city = rec.get("city") or {}
    street = rec.get("street") or {}
    otg = rec.get("otg") or {}
    return {
        "source_id": rec.get("id"),
        "source": rec.get("_source") or "planeds",
        "typeOff": rec.get("typeOff"),
        "switchSubReason": rec.get("switchSubReason") or 0,
        "city_id": city.get("id"),
        "city_name": city.get("name"),
        "street_id": street.get("id"),
        "street_name": street.get("name"),
        "otg_id": otg.get("id"),
        "otg_name": otg.get("name"),
        "buildingNames": rec.get("buildingNames") or "",
        "dateEvent": rec.get("dateEvent") or "",
        "datePlanIn": rec.get("datePlanIn") or "",
        "koment": rec.get("koment") or "",
        "queue_key": "",
        "queue": "",
        "subqueue": "",
        "fingerprint": (
            f"{city.get('id')}|{street.get('id')}|{rec.get('buildingNames')}|"
            f"{(rec.get('dateEvent') or '')[:13]}|planned"
        ),
        "soft_fp": (
            f"{city.get('id')}|{street.get('id')}|{rec.get('buildingNames')}|"
            f"{(rec.get('dateEvent') or '')[:13]}"
        ),
        "updated_at": datetime.now(timezone.utc),
    }


def main() -> int:
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB") or "vidkluchennya"
    if not uri:
        print("❌ Немає MONGO_URI", file=sys.stderr)
        return 1

    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    users_col = db["users"]
    cache_col = db["outages_cache"]
    outages_col = db["outages"]
    notified_col = db["user_outage_notified"]
    now = datetime.now(timezone.utc)
    now_s = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    users = list(users_col.find({}))
    print(f"[{now_s}] users у базі: {len(users)}")

    # (otg_id, city_id) → list of (target, user, addr)
    groups: Dict[Tuple[int, int], List[Tuple[dict, dict, dict]]] = {}
    matched_tids: set = set()
    matched_labels: List[str] = []

    for u in users:
        tid = u.get("telegram_id")
        for addr in get_user_addresses(u):
            if not isinstance(addr, dict):
                continue
            city_name = (addr.get("city_name") or addr.get("city") or "").strip()
            street_name = (addr.get("street_name") or addr.get("street") or "").strip()
            house = str(addr.get("house") or addr.get("building") or "").strip()
            otg_raw = addr.get("otg_id") or addr.get("otgId")
            city_raw = addr.get("city_id") or addr.get("cityId") or 0
            if not otg_raw:
                continue
            try:
                otg_id = int(otg_raw)
                city_id = int(city_raw) if city_raw else 0
            except (TypeError, ValueError):
                continue

            for t in TARGETS:
                if t["city_sub"] not in city_name.lower():
                    continue
                if not street_match(street_name, t["street_subs"]):
                    continue
                if not house_match(house, t["house"]):
                    continue
                groups.setdefault((otg_id, city_id), []).append((t, u, addr))
                if tid is not None:
                    matched_tids.add(int(tid))
                matched_labels.append(
                    f"  • tid={tid} {city_name}, {street_name}, {house} "
                    f"(otg={otg_id}, city={city_id}) ← {t['label']}"
                )

    if matched_labels:
        print("Знайдені підписники:")
        for line in matched_labels:
            print(line)
    else:
        print(
            "⚠️ Жодного підписника з цими адресами не знайдено. "
            "Сіємо fallback-ключі кешу (otg/city з API), щоб monitor "
            "усе одно мав «минулий» стан, якщо otg збіжиться."
        )

    # Завжди додаємо fallback-групи з API ids (на випадок роз’їзду otg у users)
    for t in TARGETS:
        key = (int(t["fallback_otg_id"]), int(t["fallback_city_id"]))
        groups.setdefault(key, [])

    seeded_cache = 0
    seeded_outages = 0

    for (otg_id, city_id), pairs in groups.items():
        # який target сіємо для цієї групи
        targets_for_group: List[dict] = []
        if pairs:
            seen = set()
            for t, _u, _a in pairs:
                if id(t) not in seen:
                    seen.add(id(t))
                    targets_for_group.append(t)
        else:
            # fallback: підібрати target за city_id
            for t in TARGETS:
                if int(t["fallback_city_id"]) == int(city_id) or int(t["fallback_otg_id"]) == int(
                    otg_id
                ):
                    targets_for_group.append(t)

        if not targets_for_group:
            continue

        cache_key = f"{otg_id}_{city_id or 0}"
        snapshot: List[dict] = []
        plan_by_addr_day: Dict[str, str] = {}
        ids: List[str] = []
        fps: List[str] = []
        soft_fps: List[str] = []
        outage_ops: List[UpdateOne] = []

        for t in targets_for_group:
            # унікальний fake source_id, щоб не колізити з живими id API
            fake_id = 900000000 + int(t["fallback_street_id"]) + int(t["house"] or "0")
            rec = snapshot_rec(
                source="planeds",
                source_id=fake_id,
                type_off=int(t["typeOff"]),
                city_id=int(t["fallback_city_id"]),
                city_name=t["fallback_city_name"],
                street_id=int(t["fallback_street_id"]),
                street_name=t["fallback_street_name"],
                otg_id=int(t["fallback_otg_id"]),
                otg_name=t["fallback_otg_name"],
                buildings=t["fallback_buildings"],
                date_event=t["fake_start"],
                date_plan=t["fake_end"],
            )
            snapshot.append(rec)
            ids.append(rec["key"])
            adk = addr_day_key(
                t["fallback_city_id"],
                t["fallback_street_id"],
                t["fallback_street_name"],
                t["fallback_buildings"],
                t["day"],
            )
            plan_by_addr_day[adk] = t["fake_end"][:16]
            soft = (
                f"{t['fallback_city_id']}|{t['fallback_street_id']}|"
                f"{','.join(sorted({p.strip().lower() for p in t['fallback_buildings'].replace(';', ',').split(',') if p.strip()}))}|"
                f"{t['fake_start'][:13]}"
            )
            soft_fps.append(soft)
            fps.append(soft + "|planned")

            doc = outage_mongo_doc(rec)
            outage_ops.append(
                UpdateOne(
                    {"source": doc["source"], "source_id": doc["source_id"]},
                    {"$set": doc},
                    upsert=True,
                )
            )

        cache_col.update_one(
            {"_id": cache_key},
            {
                "$set": {
                    "ids": ids,
                    "fps": fps,
                    "soft_fps": soft_fps,
                    "plan_by_addr_day": plan_by_addr_day,
                    "snapshot": snapshot,
                    "pending_cancels": [],
                    "updated_at": now,
                    "count": len(ids),
                    "_seed": "force_diff_test_prev_schedule",
                }
            },
            upsert=True,
        )
        seeded_cache += 1
        print(
            f"[{now_s}] cache[{cache_key}]: snapshot={len(snapshot)} "
            f"plan_by_addr_day={list(plan_by_addr_day.values())}"
        )

        if outage_ops:
            outages_col.bulk_write(outage_ops, ordered=False)
            seeded_outages += len(outage_ops)

    # Дедуп: прибрати «вже слали» для знайдених користувачів (повністю їх ключі)
    cleared_n = 0
    if matched_tids:
        r = notified_col.delete_many({"telegram_id": {"$in": list(matched_tids)}})
        cleared_n = r.deleted_count
        print(
            f"[{now_s}] user_outage_notified: видалено {cleared_n} "
            f"для tid={sorted(matched_tids)}"
        )
    else:
        print(
            f"[{now_s}] user_outage_notified: не чіпав (немає зматчених tid). "
            "Якщо сповіщення не прийдуть — очисти дедуп вручну або через повний wipe."
        )

    print(
        f"Готово: cache_keys={seeded_cache}, outages_upserts={seeded_outages}, "
        f"notified_cleared={cleared_n}."
    )
    print(
        "Далі запусти workflow «ZTOE outages» (ztoe_outages_monitor.py). "
        "Очікуй «⏱ Зміна періоду» з Було/Стало для цих адрес."
    )
    print(
        "Було (посіяно):\n"
        "  • Білокоровицьке Шосе 20: 08:00–16:00\n"
        "  • Лесі Українки 3: 09:00–14:00\n"
        "Стало — те, що зараз віддає API (агреговано)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
