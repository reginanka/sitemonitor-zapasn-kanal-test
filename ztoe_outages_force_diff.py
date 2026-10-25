#!/usr/bin/env python3
"""
Примусово очищає outages_cache, щоб наступний ztoe_outages_monitor
побачив усі поточні події як нові → перемалював PNG і надіслав сповіщення.

Аналог ukrenergo2_force_diff.py для тестового бота.

Використання:
  python ztoe_outages_force_diff.py

Потім запусти звичайний:
  python ztoe_outages_monitor.py
  (або workflow ztoe-outages.yml)
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
    col = db["outages_cache"]

    before = col.count_documents({})
    result = col.delete_many({})
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"[{now}] outages_cache: видалено {result.deleted_count} документів (було {before})")
    print("Тепер запусти ztoe_outages_monitor.py — він побачить усі outages як нові")
    print("і перемалює PNG з аварійними/плановими смугами.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
