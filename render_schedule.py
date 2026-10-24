#!/usr/bin/env python3
"""
Рендер HTML-шаблону outage-schedule.html → PNG (Playwright).

Приклад:
  python render_schedule.py --queue 4.2 --out /tmp/q.png

Або з коду:
  from render_schedule import render_to_png, build_data
  png = render_to_png(build_data(...))
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

TEMPLATE = Path(__file__).with_name("outage-schedule.html")


def build_data(
    *,
    queue: str,
    status_title: str = "Статус Житомирської області",
    status_sub: str = "",
    period: str = "",
    operator: str = "Житомиробленерго",
    days: list | None = None,
) -> dict:
    return {
        "statusTitle": status_title,
        "statusSub": status_sub,
        "period": period,
        "queue": queue,
        "operator": operator,
        "days": days
        or [
            {"label": "Сьогодні", "offset": 0, "known": True, "events": []},
            {"label": "Завтра", "offset": 1, "known": False, "events": []},
        ],
    }


def events_from_slots(slots: list[dict], type_: str = "schedule") -> list[dict]:
    """slots: [{from, to}] → events для шаблону."""
    out = []
    for s in slots or []:
        fr = s.get("from") or s.get("start")
        to = s.get("to") or s.get("end")
        if fr and to:
            out.append(
                {
                    "type": s.get("type") or type_,
                    "from": fr,
                    "to": to,
                    "text": s.get("text") or "",
                }
            )
    return out


def render_to_png(data: dict, *, width: int = 420, scale: int = 2) -> bytes:
    """Повертає PNG bytes. Потрібен: pip install playwright && playwright install chromium."""
    from playwright.sync_api import sync_playwright

    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Немає шаблону: {TEMPLATE}")

    html = TEMPLATE.read_text(encoding="utf-8")
    # підміна DATA у скрипті
    marker = "var DATA = {"
    idx = html.find(marker)
    if idx < 0:
        raise RuntimeError("У шаблоні не знайдено var DATA = {")
    # знайти кінець об'єкта DATA (грубо: до }; перед TYPE_NAME)
    end_marker = "var TYPE_NAME"
    end = html.find(end_marker, idx)
    if end < 0:
        raise RuntimeError("Не знайдено кінець DATA")
    data_js = "var DATA = " + json.dumps(data, ensure_ascii=False) + ";\n\n"
    html = html[:idx] + data_js + html[end:]

    with tempfile.TemporaryDirectory() as tmp:
        html_path = Path(tmp) / "schedule.html"
        html_path.write_text(html, encoding="utf-8")
        out_path = Path(tmp) / "out.png"

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(
                viewport={"width": width, "height": 1200},
                device_scale_factor=scale,
            )
            page.goto(html_path.as_uri(), wait_until="networkidle")
            page.locator(".wrap").screenshot(path=str(out_path))
            browser.close()

        return out_path.read_bytes()


def render_and_store(
    queue: str,
    subqueue: str = "",
    *,
    status_title: str = "Статус Житомирської області",
    status_sub: str = "",
    period: str = "",
    slots: list | None = None,
    extra_events: list | None = None,
    known: bool = True,
) -> bytes:
    """Рендер + збереження в Mongo (queue_schedules + GridFS)."""
    from mongo_store import upsert_queue_schedule, save_schedule_png

    qlabel = f"{queue}.{subqueue}" if subqueue else str(queue)
    events = events_from_slots(slots or [])
    if extra_events:
        events.extend(extra_events)

    data = build_data(
        queue=qlabel,
        status_title=status_title,
        status_sub=status_sub,
        period=period,
        days=[
            {
                "label": "Сьогодні",
                "offset": 0,
                "known": known,
                "events": events,
            },
            {"label": "Завтра", "offset": 1, "known": False, "events": []},
        ],
    )
    png = render_to_png(data)
    upsert_queue_schedule(
        queue,
        subqueue,
        slots=slots or [],
        events=events,
        date="",
        known=known,
    )
    save_schedule_png(queue, subqueue, png)
    return png


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", required=True)
    ap.add_argument("--subqueue", default="")
    ap.add_argument("--out", default="schedule.png")
    ap.add_argument("--store", action="store_true", help="також записати в Mongo")
    ap.add_argument("--status-sub", default="Застосовані графіки погодинних відключень")
    args = ap.parse_args()

    demo_slots = [
        {"from": "06:00", "to": "09:00", "type": "schedule"},
        {"from": "18:00", "to": "21:00", "type": "schedule"},
    ]
    data = build_data(
        queue=f"{args.queue}.{args.subqueue}" if args.subqueue else args.queue,
        status_sub=args.status_sub,
        days=[
            {
                "label": "Сьогодні",
                "offset": 0,
                "known": True,
                "events": events_from_slots(demo_slots),
            },
            {"label": "Завтра", "offset": 1, "known": False, "events": []},
        ],
    )
    png = render_to_png(data)
    Path(args.out).write_bytes(png)
    print(f"OK → {args.out} ({len(png)} bytes)")
    if args.store:
        from mongo_store import upsert_queue_schedule, save_schedule_png

        upsert_queue_schedule(args.queue, args.subqueue, slots=demo_slots, known=True)
        save_schedule_png(args.queue, args.subqueue, png)
        print("OK → Mongo queue_schedules + GridFS")


if __name__ == "__main__":
    main()
