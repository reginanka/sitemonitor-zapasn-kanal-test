#!/usr/bin/env python3
"""
Тестовий «минулий» стан ремонтів ТІЛЬКИ для двох адрес.

Мета: ztoe_outages_monitor побачив зміну періоду саме на них і надіслав
«⏱ Зміна періоду», без «нових графіків» усій базі.

ВАЖЛИВО:
  - НЕ перезаписує весь outages_cache локації з нуля.
  - Читає поточний cache, підміняє лише snapshot/plan_by_addr_day
    для цільових вулиць+дня, решту записів лишає як є.
  - Чистить user_outage_notified лише для зматчених telegram_id.

Адреси:
  - Коростень, вул. Білокоровицьке Шосе, 20
  - Вересна, вул. Лесі Українки, 3

НЕ чіпає: queue_schedules, users, ukrenergo*, accidents_summary, site_meta.

  python ztoe_outages_force_diff.py
  python ztoe_outages_monitor.py
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

from pymongo import MongoClient, UpdateOne

TARGETS = [
    {
        "label": "Коростень, Білокоровицьке Шосе, 20",
        "city_sub": "коростень",
        "street_subs": ("білокоровицьк", "білокровицьк"),
        "house": "20",
        # API зараз ширший інтервал → «було» коротше, щоб була зміна періоду
        "fake_start": "2026-11-23T08:00:00",
        "fake_end": "2026-11-23T16:00:00",
        "day": "2026-11-23",
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


def rec_matches_target(rec: dict, t: dict) -> bool:
    """Чи snapshot/outage-запис стосується цільової вулиці."""
    city = rec.get("city") if isinstance(rec.get("city"), dict) else {}
    street = rec.get("street") if isinstance(rec.get("street"), dict) else {}
    city_name = (city.get("name") or rec.get("city_name") or "").lower()
    street_name = street.get("name") or rec.get("street_name") or ""
    city_id = city.get("id") if city else rec.get("city_id")
    street_id = street.get("id") if street else rec.get("street_id")

    if t["city_sub"] not in city_name and int(city_id or 0) != int(t["fallback_city_id"]):
        return False
    if int(street_id or 0) == int(t["fallback_street_id"]):
        return True
    return street_match(street_name, t["street_subs"])


def get_user_addresses(user: dict) -> list:
    if user.get("addresses"):
        return user["addresses"]
    if user.get("address"):
        return [user["address"]]
    return []


def addr_day_key_from_parts(
    city_id: Any,
    street_id: Any,
    street_name: str,
    buildings: str,
    day: str,
) -> str:
    # Як у monitor v1.6+: city|street|day — без списку будинків
    sid = street_id if street_id not in (None, "") else _norm(street_name)
    return f"{city_id or ''}|{sid}|{day}"


def snapshot_rec(t: dict, *, fake_id: int) -> dict:
    return {
        "key": f"planeds:{fake_id}",
        "id": fake_id,
        "_source": "planeds",
        "typeOff": int(t["typeOff"]),
        "switchSubReason": 0,
        "city": {
            "id": int(t["fallback_city_id"]),
            "name": t["fallback_city_name"],
        },
        "street": {
            "id": int(t["fallback_street_id"]),
            "name": t["fallback_street_name"],
        },
        "otg": {
            "id": int(t["fallback_otg_id"]),
            "name": t["fallback_otg_name"],
        },
        "buildingNames": t["fallback_buildings"],
        "dateEvent": t["fake_start"],
        "datePlanIn": t["fake_end"],
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
            f"{city.get('id')}|{street.get('id')}|seed|"
            f"{(rec.get('dateEvent') or '')[:13]}|planned"
        ),
        "soft_fp": (
            f"{city.get('id')}|{street.get('id')}|seed|"
            f"{(rec.get('dateEvent') or '')[:13]}"
        ),
        "updated_at": datetime.now(timezone.utc),
    }


def patch_cache_doc(cache: dict, targets: List[dict], now: datetime) -> dict:
    """
    Зберігає всі чужі записи в snapshot/ids/fps/soft_fps/plan_by_addr_day.
    Прибирає лише ті, що стосуються TARGET вулиць, і додає fake «було».
    """
    cache = dict(cache or {})
    snapshot = list(cache.get("snapshot") or [])
    ids = list(cache.get("ids") or [])
    fps = list(cache.get("fps") or [])
    soft_fps = list(cache.get("soft_fps") or [])
    plan_by = {
        str(k): str(v)
        for k, v in (cache.get("plan_by_addr_day") or {}).items()
    }

    # 1) прибрати з snapshot записи цільових вулиць
    new_snapshot = []
    removed_keys: Set[str] = set()
    for rec in snapshot:
        if not isinstance(rec, dict):
            continue
        if any(rec_matches_target(rec, t) for t in targets):
            key = str(rec.get("key") or "")
            if key:
                removed_keys.add(key)
            continue
        new_snapshot.append(rec)

    ids = [i for i in ids if str(i) not in removed_keys]

    # plan_by_addr_day: прибрати ключі цільових вулиць
    cleaned_plan = {}
    for adk, plan in plan_by.items():
        parts = adk.split("|")
        sid = parts[1] if len(parts) > 1 else ""
        drop = False
        for t in targets:
            if sid and sid == str(t["fallback_street_id"]):
                drop = True
                break
            if sid and any(sub in _norm(sid) for sub in t["street_subs"]):
                drop = True
                break
        if not drop:
            cleaned_plan[adk] = plan
    plan_by = cleaned_plan

    # 2) додати fake snapshot + plan_by для кожного target
    for t in targets:
        fake_id = 900000000 + int(t["fallback_street_id"]) + int(t["house"] or "0")
        rec = snapshot_rec(t, fake_id=fake_id)
        new_snapshot.append(rec)
        ids.append(rec["key"])
        adk = addr_day_key_from_parts(
            t["fallback_city_id"],
            t["fallback_street_id"],
            t["fallback_street_name"],
            t["fallback_buildings"],
            t["day"],
        )
        plan_by[adk] = t["fake_end"][:16]
        soft = (
            f"{t['fallback_city_id']}|{t['fallback_street_id']}|seed|"
            f"{t['fake_start'][:13]}"
        )
        if soft not in soft_fps:
            soft_fps.append(soft)
        fp = soft + "|planned"
        if fp not in fps:
            fps.append(fp)

    return {
        "ids": ids,
        "fps": fps,
        "soft_fps": soft_fps,
        "plan_by_addr_day": plan_by,
        "snapshot": new_snapshot,
        "pending_cancels": list(cache.get("pending_cancels") or []),
        "updated_at": now,
        "count": len(ids),
        "_seed": "force_diff_patch_targets_only",
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

    group_targets: Dict[Tuple[int, int], List[dict]] = {}
    matched_tids: Set[int] = set()
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
                group_targets.setdefault((otg_id, city_id), [])
                if t not in group_targets[(otg_id, city_id)]:
                    group_targets[(otg_id, city_id)].append(t)
                if tid is not None:
                    matched_tids.add(int(tid))
                matched_labels.append(
                    f"  • tid={tid} {city_name}, {street_name}, {house} "
                    f"(otg={otg_id}, city={city_id}) ← {t['label']}"
                )

    if matched_labels:
        print("Знайдені підписники (тільки вони отримають тестову зміну):")
        for line in matched_labels:
            print(line)
    else:
        print(
            "⚠️ Підписників з цими адресами не знайдено. "
            "Патчимо лише fallback cache-ключі API — інші локації не чіпаємо."
        )

    for t in TARGETS:
        already = any(t in ts for ts in group_targets.values())
        if already:
            continue
        key = (int(t["fallback_otg_id"]), int(t["fallback_city_id"]))
        group_targets.setdefault(key, []).append(t)
        print(f"  fallback cache key {key[0]}_{key[1]} для {t['label']}")

    patched_keys = 0
    outage_upserts = 0

    for (otg_id, city_id), targets in group_targets.items():
        cache_key = f"{otg_id}_{city_id or 0}"
        existing = cache_col.find_one({"_id": cache_key}) or {}
        before_snap = len(existing.get("snapshot") or [])
        before_plan = len(existing.get("plan_by_addr_day") or {})

        patched = patch_cache_doc(existing, targets, now)
        after_snap = len(patched.get("snapshot") or [])
        after_plan = len(patched.get("plan_by_addr_day") or {})

        cache_col.update_one(
            {"_id": cache_key},
            {"$set": patched},
            upsert=True,
        )
        patched_keys += 1
        print(
            f"[{now_s}] PATCH cache[{cache_key}] "
            f"targets={[t['label'] for t in targets]} "
            f"snapshot {before_snap}→{after_snap} "
            f"plan_by_addr_day {before_plan}→{after_plan}"
        )

        ops = []
        for t in targets:
            fake_id = 900000000 + int(t["fallback_street_id"]) + int(t["house"] or "0")
            rec = snapshot_rec(t, fake_id=fake_id)
            doc = outage_mongo_doc(rec)
            ops.append(
                UpdateOne(
                    {"source": doc["source"], "source_id": doc["source_id"]},
                    {"$set": doc},
                    upsert=True,
                )
            )
        if ops:
            outages_col.bulk_write(ops, ordered=False)
            outage_upserts += len(ops)

    cleared_n = 0
    if matched_tids:
        r = notified_col.delete_many({"telegram_id": {"$in": list(matched_tids)}})
        cleared_n = r.deleted_count
        print(
            f"[{now_s}] user_outage_notified: видалено {cleared_n} "
            f"лише для tid={sorted(matched_tids)}"
        )
    else:
        print(f"[{now_s}] user_outage_notified: не чіпав (немає зматчених tid)")

    print(
        f"Готово: patched_cache_keys={patched_keys}, "
        f"outages_upserts={outage_upserts}, notified_cleared={cleared_n}."
    )
    print(
        "Інші адреси в cache не затирались — лише цільові вулиці "
        "в snapshot і plan_by_addr_day."
    )
    print(
        "Далі: python ztoe_outages_monitor.py\n"
        "Очікуй «⏱ Зміна періоду» лише для:\n"
        "  • Білокоровицьке Шосе 20: було 08:00–16:00\n"
        "  • Лесі Українки 3: було 09:00–14:00"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
