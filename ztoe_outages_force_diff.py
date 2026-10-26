#!/usr/bin/env python3
"""
Примусово очищає outages_cache (+ опційно старі outages),
щоб наступний ztoe_outages_monitor побачив події як нові.

НЕ чистить queue_schedules і users.

Використання:
  python ztoe_outages_force_diff.py
  # потім:
  python ztoe_outages_monitor.py
  (або workflow ztoe-outages.yml)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta

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
    now = datetime.now(timezone.utc)
    now_s = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    # 1) cache — завжди
    cache = db["outages_cache"]
    before_cache = cache.count_documents({})
    r1 = cache.delete_many({})
    print(f"[{now_s}] outages_cache: видалено {r1.deleted_count} (було {before_cache})")

    # 2) старі outages (старіше 3 днів) — щоб DM не тягнув сміття
    outages = db["outages"]
    cutoff = now - timedelta(days=3)
    before_out = outages.count_documents({})
    # updated_at або dateEvent
    r2 = outages.delete_many({
        "$or": [
            {"updated_at": {"$lt": cutoff}},
            {"dateEvent": {"$lt": (cutoff).strftime("%Y-%m-%d")}},
            {"date_from": {"$lt": (cutoff).strftime("%Y-%m-%d")}},
        ]
    })
    after_out = outages.count_documents({})
    print(
        f"[{now_s}] outages: видалено старих {r2.deleted_count} "
        f"(було {before_out}, лишилось {after_out})"
    )

    print("Тепер запусти ztoe_outages_monitor — він заново заповнить outages і cache.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
