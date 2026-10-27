#!/usr/bin/env python3
"""
Резервний повний скид даних power-on API (ЖОЕ accidents/planeds).

Чистить:
  - outages_cache  — дедуп нотифікацій (повністю)
  - outages        — зліпок для /check (повністю)

НЕ чіпає:
  - queue_schedules (ГПВ з monitor/сайту)
  - users
  - ukrenergo2_states / regionstatuses
  - accidents_summary
  - site_meta / GridFS

Після цього обов'язково запусти ztoe_outages_monitor
(workflow «ZTOE outages»), щоб знову наповнити outages + cache з API.

  python ztoe_outages_force_diff.py
  python ztoe_outages_monitor.py
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient


def main() -> int:
    uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    db_name = (
        os.getenv("ZTOE_MONGO_DB")
        or os.getenv("MONGO_DB")
        or os.getenv("MAYACHOK_MONGO_DB")
        or "vidkluchennya"
    )
    if not uri:
        print("❌ Немає MAYACHOK_MONGO_URI / MONGO_URI", file=sys.stderr)
        return 1

    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    now_s = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # 1) outages_cache — повний wipe
    cache = db["outages_cache"]
    before_cache = cache.count_documents({})
    r1 = cache.delete_many({})
    print(
        f"[{now_s}] outages_cache: видалено {r1.deleted_count} "
        f"(було {before_cache})"
    )

    # 2) outages — повний wipe (усе з power-on API)
    outages = db["outages"]
    before_out = outages.count_documents({})
    r2 = outages.delete_many({})
    after_out = outages.count_documents({})
    print(
        f"[{now_s}] outages: видалено {r2.deleted_count} "
        f"(було {before_out}, лишилось {after_out})"
    )

    print(
        "Готово. Не чіпали: queue_schedules, users, ukrenergo*, "
        "accidents_summary, site_meta."
    )
    print(
        "Далі запусти workflow «ZTOE outages» (ztoe_outages_monitor.py), "
        "щоб заново заповнити outages і cache з API."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
