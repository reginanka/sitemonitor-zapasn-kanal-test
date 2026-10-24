"""Примусово змінює дані в data/, щоб наступний запуск monitor.py
побачив різницю з live API і надіслав сповіщення.

Що робить:
  1. Читає data/current.json
  2. Інвертує кольори (white ↔ red) у КОЖНІЙ черзі
  3. Перераховує main_hashes / span_hashes
  4. Записує current.json + last_hash.json

Після цього просто запусти звичайний workflow «Monitor Schedule Updates».

Env (опційно):
  FORCE_QUEUES   — коми-розділений список черг, напр. "1.1,2.1"
                   порожньо / "all" = усі черги з current.json
  FORCE_MAX_FLIP — скільки інтервалів максимум перевернути НА КОЖНУ чергу
                   (за замовч. 8)
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

DATA_DIR = Path("data")
CURRENT_FILE = DATA_DIR / "current.json"
HASH_FILE = DATA_DIR / "last_hash.json"

# Усі черги монітора (1.1 … 6.2)
ALL_QUEUES = [f"{i}.{j}" for i in range(1, 7) for j in (1, 2)]


def calculate_hash(obj: Any) -> str:
    json_str = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def flip_colors(
    norm_by_queue: Dict[str, List[Dict]],
    queues: List[str],
    max_flip_per_queue: int,
) -> Dict[str, int]:
    """Інвертує white↔red. Повертає {черга: кількість змін}."""
    result: Dict[str, int] = {}

    for q in queues:
        items = norm_by_queue.get(q)
        if not items:
            print(f"⚠️  Черга {q} відсутня в current.json — пропускаю")
            continue

        flipped = 0
        for rec in items:
            if flipped >= max_flip_per_queue:
                break
            color = (rec.get("color") or "").strip().lower()
            if color == "red":
                rec["color"] = "white"
                flipped += 1
            elif color == "white":
                rec["color"] = "red"
                flipped += 1

        result[q] = flipped
        print(f"  ✓ {q}: перевернуто {flipped}")

    return result


def rebuild_hashes(
    norm_by_queue: Dict[str, List[Dict]],
) -> tuple[Dict[str, str], Dict[str, Dict[str, Dict[str, str]]]]:
    main_hashes: Dict[str, str] = {}
    span_hashes: Dict[str, Dict[str, Dict[str, str]]] = {}

    for queue_key, norm_list in norm_by_queue.items():
        main_hash_data = [
            {"date": r["date"], "span": r["span"], "color": r["color"]}
            for r in norm_list
        ]
        main_hashes[queue_key] = calculate_hash(main_hash_data)

        sh: Dict[str, Dict[str, str]] = {}
        for rec in norm_list:
            d = rec["date"]
            span = rec["span"]
            sh.setdefault(d, {})[span] = calculate_hash({"color": rec["color"]})
        span_hashes[queue_key] = sh

    return main_hashes, span_hashes


def main() -> None:
    print("=" * 50)
    print("🔧 FORCE DIFF · schedule (data/current.json)")
    print("=" * 50)

    if not CURRENT_FILE.exists():
        raise SystemExit(
            f"❌ Немає файлу {CURRENT_FILE}. Спочатку запусти звичайний monitor."
        )

    raw_queues = (os.getenv("FORCE_QUEUES") or "all").strip().lower()
    if not raw_queues or raw_queues in ("all", "*"):
        queues = ALL_QUEUES
    else:
        queues = [q.strip() for q in raw_queues.split(",") if q.strip()]

    max_flip = int(os.getenv("FORCE_MAX_FLIP") or "8")

    print(f"🎯 Черги: {queues}")
    print(f"🔢 Макс. переворотів на чергу: {max_flip}")

    norm_by_queue: Dict[str, List[Dict]] = load_json(CURRENT_FILE)
    if not norm_by_queue:
        raise SystemExit("❌ current.json порожній")

    per_queue = flip_colors(norm_by_queue, queues, max_flip)
    total = sum(per_queue.values())
    if total == 0:
        raise SystemExit("❌ Нічого не змінено (немає записів white/red)")

    print(f"✅ Всього перевернуто кольорів: {total} у {len(per_queue)} чергах")

    main_hashes, span_hashes = rebuild_hashes(norm_by_queue)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    save_json(norm_by_queue, CURRENT_FILE)
    save_json(
        {
            "timestamp": timestamp,
            "main_hashes": main_hashes,
            "span_hashes": span_hashes,
            "note": f"force-diff: flipped {total} colors in {list(per_queue.keys())}",
        },
        HASH_FILE,
    )

    print(f"💾 Оновлено {CURRENT_FILE}")
    print(f"💾 Оновлено {HASH_FILE}")
    print()
    print("➡️  Тепер запусти workflow «Monitor Schedule Updates» —")
    print("   він порівняє live API з «зламаним» станом і має надіслати сповіщення.")


if __name__ == "__main__":
    main()
