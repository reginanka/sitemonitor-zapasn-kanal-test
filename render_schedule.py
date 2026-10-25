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
from datetime import datetime
from pathlib import Path
from typing import Any

TEMPLATE = Path(__file__).with_name("outage-schedule.html")

# Відступи навколо .wrap при скріншоті (px CSS, до scale)
PADDING = 16


def _normalize_period(period: Any) -> dict | str:
    """period може бути рядком (старий формат) або dict {from, to, ...}."""
    if isinstance(period, dict):
        return period
    if isinstance(period, str) and period.strip():
        # спроба витягнути з рядка типу "06:00-23:59" або "Період: ..."
        s = period.strip()
        for sep in (" — ", " - ", "–", "-"):
            if sep in s and ":" in s:
                parts = s.split(sep)
                # беремо останні два токени з часом
                times = []
                for p in parts:
                    for tok in p.replace(",", " ").split():
                        if ":" in tok and tok[0].isdigit():
                            times.append(tok[:5])
                if len(times) >= 2:
                    return {"from": times[0], "to": times[1]}
        return {"from": "06:00", "to": "23:59"}
    return {"from": "06:00", "to": "23:59"}


def build_data(
    *,
    queue: str,
    status_title: str = "Статус Житомирської області",
    status_sub: str = "",
    period: Any = None,
    operator: str = "Житомиробленерго",
    updated: str = "",
    days: list | None = None,
) -> dict:
    if not updated:
        updated = datetime.now().strftime("%d.%m о %H:%M")
    return {
        "statusTitle": status_title,
        "statusSub": status_sub
        or "За розпорядженням НЕК Укренерго застосовано графіки погодинних відключень",
        "period": _normalize_period(period),
        "queue": queue,
        "operator": operator,
        "updated": updated,
        "days": days
        or [
            {"label": "Сьогодні", "offset": 0, "known": True, "events": []},
            {"label": "Завтра", "offset": 1, "known": False, "events": []},
        ],
    }


def events_from_slots(slots: list[dict], type_: str = "schedule") -> list[dict]:
    """slots: [{from, to}] → events для шаблону (поле note, не text)."""
    out = []
    for s in slots or []:
        fr = s.get("from") or s.get("start")
        to = s.get("to") or s.get("end")
        if fr and to:
            note = s.get("note") or s.get("text") or ""
            out.append(
                {
                    "type": s.get("type") or type_,
                    "from": fr,
                    "to": to,
                    "note": note,
                }
            )
    return out


def render_to_png(
    data: dict,
    *,
    width: int = 480,
    scale: int = 2,
    padding: int = PADDING,
) -> bytes:
    """Повертає PNG bytes. Потрібен: pip install playwright && playwright install chromium.

    padding — відступ (px) навколо контенту .wrap у фінальній картинці.
    """
    from playwright.sync_api import sync_playwright

    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Немає шаблону: {TEMPLATE}")

    html = TEMPLATE.read_text(encoding="utf-8")
    # підміна DATA у скрипті
    marker = "var DATA = {"
    idx = html.find(marker)
    if idx < 0:
        raise RuntimeError("У шаблоні не знайдено var DATA = {")
    # кінець DATA — перед var TAG (новий шаблон) або var TYPE_NAME (старий)
    end = html.find("var TAG", idx)
    if end < 0:
        end = html.find("var TYPE_NAME", idx)
    if end < 0:
        raise RuntimeError("Не знайдено кінець DATA (шукав var TAG / var TYPE_NAME)")
    data_js = "var DATA = " + json.dumps(data, ensure_ascii=False) + ";\n\n"
    html = html[:idx] + data_js + html[end:]

    with tempfile.TemporaryDirectory() as tmp:
        html_path = Path(tmp) / "schedule.html"
        html_path.write_text(html, encoding="utf-8")
        out_path = Path(tmp) / "out.png"

        with sync_playwright() as p:
            browser = p.chromium.launch()
            # viewport трохи ширший за контент + padding з обох боків
            page = browser.new_page(
                viewport={
                    "width": width + padding * 2,
                    "height": 1600,
                },
                device_scale_factor=scale,
            )
            page.goto(html_path.as_uri(), wait_until="networkidle")

            # Чекаємо, поки JS намалює контент
            page.wait_for_selector(".wrap", state="visible")

            # Скріншот з відступами: беремо bounding box .wrap і розширюємо
            box = page.locator(".wrap").bounding_box()
            if not box:
                raise RuntimeError("Не вдалося отримати bounding box .wrap")

            clip = {
                "x": max(0, box["x"] - padding),
                "y": max(0, box["y"] - padding),
                "width": box["width"] + padding * 2,
                "height": box["height"] + padding * 2,
            }
            page.screenshot(path=str(out_path), clip=clip)
            browser.close()

        return out_path.read_bytes()


def render_and_store(
    queue: str,
    subqueue: str = "",
    *,
    status_title: str = "Статус Житомирської області",
    status_sub: str = "",
    period: Any = None,
    slots: list | None = None,
    extra_events: list | None = None,
    known: bool = True,
    updated: str = "",
) -> bytes:
    """Рендер + збереження в Mongo (queue_schedules + GridFS)."""
    from mongo_store import upsert_queue_schedule, save_schedule_png

    qlabel = f"{queue}.{subqueue}" if subqueue else str(queue)
    events = events_from_slots(slots or [])
    if extra_events:
        # нормалізуємо text → note
        for e in extra_events:
            if "note" not in e and "text" in e:
                e = {**e, "note": e.get("text", "")}
            events.append(e)

    data = build_data(
        queue=qlabel,
        status_title=status_title,
        status_sub=status_sub,
        period=period,
        updated=updated,
        days=[
            {
                "label": "Сьогодні",
                "offset": 0,
                "known": known,
                "events": events,
            },
            {
                "label": "Завтра",
                "offset": 1,
                "known": False,
                "waitNote": "Розпорядження НЕК Укренерго очікується після 18:00",
                "events": [],
            },
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
    ap.add_argument(
        "--status-sub",
        default="За розпорядженням НЕК Укренерго застосовано графіки погодинних відключень",
    )
    ap.add_argument("--padding", type=int, default=PADDING, help="відступ навколо картинки (px)")
    args = ap.parse_args()

    demo_slots = [
        {"from": "10:00", "to": "14:00", "type": "planned", "note": "Ремонт"},
        {"from": "16:20", "to": "17:45", "type": "emerg"},
        {"from": "19:30", "to": "22:00", "type": "schedule"},
    ]
    data = build_data(
        queue=f"{args.queue}.{args.subqueue}" if args.subqueue else args.queue,
        status_sub=args.status_sub,
        period={"from": "06:00", "to": "23:59"},
        days=[
            {
                "label": "Сьогодні",
                "offset": 0,
                "known": True,
                "events": events_from_slots(demo_slots),
            },
            {
                "label": "Завтра",
                "offset": 1,
                "known": False,
                "waitNote": "Розпорядження НЕК Укренерго очікується після 18:00",
                "events": [],
            },
        ],
    )
    png = render_to_png(data, padding=args.padding)
    Path(args.out).write_bytes(png)
    print(f"OK → {args.out} ({len(png)} bytes)")
    if args.store:
        from mongo_store import upsert_queue_schedule, save_schedule_png

        upsert_queue_schedule(args.queue, args.subqueue, slots=demo_slots, known=True)
        save_schedule_png(args.queue, args.subqueue, png)
        print("OK → Mongo queue_schedules + GridFS")


if __name__ == "__main__":
    main()
