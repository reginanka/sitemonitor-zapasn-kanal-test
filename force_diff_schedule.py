"""Записує в data/ навмисно «не той» стан, щоб наступний monitor.py
порівняв його з live API і надіслав сповіщення.

Формат як у data/current.json після нормалізації monitor.py:
  черга "1.1" → список записів
  {cherga, pidcherga, queue_key, date: "DD.MM.YYYY", span: "HH:MM-HH:MM", color}

Якщо графік уже є — інвертує white↔red.
Якщо файл порожній — пише сьогодні і завтра, 48 інтервалів, усі red.
Живі білі слоти тоді стають «скасованими».
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

DATA_DIR = Path("data")
CURRENT_FILE = DATA_DIR / "current.json"
HASH_FILE = DATA_DIR / "last_hash.json"

ALL_QUEUES = [f"{i}.{j}" for i in range(1, 7) for j in (1, 2)]
# Київ взимку UTC+2. На раннері UTC, тому дати беремо і UTC, і UTC+2.
KYIV = timezone(timedelta(hours=2))


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


def selected_queues() -> List[str]:
    raw = (os.getenv("FORCE_QUEUES") or "all").strip().lower()
    if not raw or raw in ("all", "*"):
        return list(ALL_QUEUES)
    return [q.strip() for q in raw.split(",") if q.strip()]


def half_hour_spans() -> List[str]:
    spans = []
    for i in range(48):
        start_m = i * 30
        end_m = start_m + 30
        sh, sm = divmod(start_m, 60)
        eh, em = divmod(end_m, 60)
        spans.append(f"{sh:02d}:{sm:02d}-{eh:02d}:{em:02d}")
    return spans


def schedule_dates() -> List[str]:
    now_utc = datetime.now(timezone.utc)
    days = {
        now_utc.strftime("%d.%m.%Y"),
        (now_utc + timedelta(days=1)).strftime("%d.%m.%Y"),
        datetime.now(KYIV).strftime("%d.%m.%Y"),
        (datetime.now(KYIV) + timedelta(days=1)).strftime("%d.%m.%Y"),
    }
    return sorted(days, key=lambda s: datetime.strptime(s, "%d.%m.%Y"))


def flip_colors(
    norm_by_queue: Dict[str, List[Dict]],
    queues: List[str],
    max_flip_per_queue: int,
) -> Dict[str, int]:
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
        print(f"  ✅ {q}: перевернуто {flipped}")
    return result


def rebuild_hashes(
    norm_by_queue: Dict[str, List[Dict]],
) -> tuple[Dict[str, str], Dict[str, Dict[str, Dict[str, str]]]]:
    main_hashes: Dict[str, str] = {}
    span_hashes: Dict[str, Dict[str, Dict[str, str]]] = {}
    for queue_key, norm_list in norm_by_queue.items():
        ordered = sorted(norm_list, key=lambda r: (r["date"], r["span"]))
        norm_by_queue[queue_key] = ordered
        main_hash_data = [
            {"date": r["date"], "span": r["span"], "color": r["color"]}
            for r in ordered
        ]
        main_hashes[queue_key] = calculate_hash(main_hash_data)
        sh: Dict[str, Dict[str, str]] = {}
        for rec in ordered:
            sh.setdefault(rec["date"], {})[rec["span"]] = calculate_hash(
                {"color": rec["color"]}
            )
        span_hashes[queue_key] = sh
    return main_hashes, span_hashes


def fill_fake_schedule(queues: List[str]) -> Dict[str, List[Dict]]:
    dates = schedule_dates()
    spans = half_hour_spans()
    norm: Dict[str, List[Dict]] = {}
    for q in queues:
        cherga, pidcherga = q.split(".", 1)
        rows: List[Dict] = []
        for date in dates:
            for span in spans:
                rows.append(
                    {
                        "cherga": int(cherga),
                        "pidcherga": int(pidcherga),
                        "queue_key": q,
                        "date": date,
                        "span": span,
                        "color": "red",
                    }
                )
        norm[q] = rows
        print(f"  ✅ {q}: {len(rows)} інтервалів, дати {dates}, усі red")
    return norm


def main() -> None:
    print("=" * 50)
    print("🔧 FORCE DIFF · schedule (data/current.json)")
    print("=" * 50)

    queues = selected_queues()
    max_flip = int(os.getenv("FORCE_MAX_FLIP") or "8")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"🎯 Черги: {queues}")
    print(f"🔢 Макс. переворотів на чергу: {max_flip}")

    norm_by_queue: Dict[str, List[Dict]] = (
        load_json(CURRENT_FILE) if CURRENT_FILE.exists() else {}
    )
    if not isinstance(norm_by_queue, dict):
        norm_by_queue = {}

    has_rows = any(isinstance(v, list) and v for v in norm_by_queue.values())
    if not has_rows:
        print("📭 current.json порожній — пишу графік у форматі монітора")
        norm_by_queue = fill_fake_schedule(queues)
        note = "force-diff: fake all-red schedule, dates DD.MM.YYYY, spans HH:MM-HH:MM"
    else:
        per_queue = flip_colors(norm_by_queue, queues, max_flip)
        total = sum(per_queue.values())
        if total == 0:
            raise SystemExit("❌ Нічого не змінилось (немає white/red інтервалів)")
        print(f"🎨 Усього перевернуто кольорів: {total}")
        note = f"force-diff: flipped {total} colors in {list(per_queue.keys())}"

    main_hashes, span_hashes = rebuild_hashes(norm_by_queue)
    save_json(norm_by_queue, CURRENT_FILE)
    save_json(
        {
            "timestamp": timestamp,
            "main_hashes": main_hashes,
            "span_hashes": span_hashes,
            "note": note,
        },
        HASH_FILE,
    )
    print(f"✅ Записано {CURRENT_FILE}")
    print(f"✅ Записано {HASH_FILE}")
    print("Тепер запусти «Monitor Schedule Updates».")


if __name__ == "__main__":
    main()
