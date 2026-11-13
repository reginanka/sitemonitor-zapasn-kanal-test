"""
Ручне перезаписування last_id для ztoe_channel_monitor — БЕЗ жодних перевірок.

Пише в ту саму колекцію й документ, що й монітор (імпортує звідти SOURCE_CHANNEL,
STATE_COLLECTION, save_last_id), тож _id гарантовано збігається.
Після цього наступний запуск монітора побачить усі пости з id > LAST_ID як нові.

Env:
  LAST_ID                      — число (default 1902)
  ZTOE_CHANNEL_URL             — потрібен, щоб визначити назву каналу (як у моніторі)
  MONGO_URI, MONGO_DB          — як у решти модулів
"""

from __future__ import annotations

import os
import sys

from mongo_store import get_db
from ztoe_channel_monitor import SOURCE_CHANNEL, STATE_COLLECTION, save_last_id


def main() -> int:
    raw = (os.getenv("LAST_ID") or "1872").strip()
    try:
        last_id = int(raw)
    except ValueError:
        print(f"❌ LAST_ID має бути числом, отримано: {raw!r}", file=sys.stderr)
        return 1

    col = get_db()[STATE_COLLECTION]
    before = col.find_one({"_id": SOURCE_CHANNEL})
    save_last_id(col, last_id)
    print(
        f"✅ {STATE_COLLECTION}[{SOURCE_CHANNEL}].last_id: "
        f"{before.get('last_id') if before else None} → {last_id}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
