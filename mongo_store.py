"""
Запис у спільну Mongo БД vidkluchennya (бот ztoe + sitemonitor).

Env:
  MAYACHOK_MONGO_URI | MONGO_URI
  ZTOE_MONGO_DB | MONGO_DB | MAYACHOK_MONGO_DB  (default: vidkluchennya)

PNG по чергах більше НЕ зберігаються.
Залишено лише site screenshot (один скрін ЖОЕ).
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Optional

from pymongo import MongoClient
from gridfs import GridFS

DB_NAME = (
    os.getenv("ZTOE_MONGO_DB")
    or os.getenv("MONGO_DB")
    or os.getenv("MAYACHOK_MONGO_DB")
    or "vidkluchennya"
)


def get_db():
    uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("Немає MONGO_URI / MAYACHOK_MONGO_URI")
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    client.admin.command("ping")
    return client[DB_NAME]


def _fp(*parts: str) -> str:
    raw = "|".join((p or "").strip() for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


# ---------- system / ukrenergo (бот читає ukrenergo2_states) ----------

def upsert_ukrenergo_state(snapshot: dict, slug: str = "zhytomyr") -> None:
    """Сумісно з ukrenergo2_monitor.save_state — пише в ту ж колекцію."""
    db = get_db()
    doc = dict(snapshot)
    doc["slug"] = slug
    doc["updatedAt"] = datetime.now(timezone.utc)
    db["ukrenergo2_states"].update_one({"slug": slug}, {"$set": doc}, upsert=True)


# ---------- queue_schedules (тільки дані, без PNG) ----------

def upsert_queue_schedule(
    queue: str,
    subqueue: str = "",
    *,
    slots: list | None = None,
    events: list | None = None,
    text: str = "",
    date: str = "",
    known: bool = True,
    extra: dict | None = None,
) -> str:
    """
    slots/events: [{from, to, type?}]
    Повертає _id документа.
    """
    db = get_db()
    q = (queue or "").strip()
    sq = (subqueue or "").strip()
    _id = f"{q}|{sq}" if sq else q
    fp = _fp(q, sq, date, text, str(slots or events or ""))
    doc = {
        "queue": q,
        "subqueue": sq,
        "slots": slots or [],
        "events": events or [],
        "text": text,
        "date": date,
        "known": known,
        "fingerprint": fp,
        "updated_at": datetime.now(timezone.utc),
    }
    if extra:
        doc.update(extra)
    db["queue_schedules"].update_one({"_id": _id}, {"$set": doc}, upsert=True)
    return _id


def list_queue_schedule_ids() -> list[tuple[str, str]]:
    """Усі (queue, subqueue) з queue_schedules."""
    db = get_db()
    out = []
    for doc in db["queue_schedules"].find({}, {"queue": 1, "subqueue": 1}):
        q = str(doc.get("queue") or "").strip()
        sq = str(doc.get("subqueue") or "").strip()
        if q:
            out.append((q, sq))
    return out


# ---------- outages (повні документи для /check) ----------

def upsert_outage(doc: dict) -> str:
    """
    Очікувані поля:
      city_name, street_name, type / type_name,
      buildings, date_from, date_to, source_id
    """
    db = get_db()
    source_id = str(doc.get("source_id") or doc.get("id") or "")
    city = (doc.get("city_name") or "").strip()
    street = (doc.get("street_name") or "").strip()
    _id = source_id or _fp(
        city, street,
        str(doc.get("date_from") or ""),
        str(doc.get("type") or ""),
        str(doc.get("buildings") or ""),
    )
    payload = {
        **doc,
        "city_name": city,
        "street_name": street,
        "updated_at": datetime.now(timezone.utc),
    }
    db["outages"].update_one({"_id": _id}, {"$set": payload}, upsert=True)
    return _id


def outage_from_api_item(o: dict) -> dict:
    """Нормалізація елемента з power-on API → документ outages."""
    city = (o.get("city") or {}) if isinstance(o.get("city"), dict) else {}
    street = (o.get("street") or {}) if isinstance(o.get("street"), dict) else {}
    type_off = o.get("typeOff")
    type_map = {
        0: "Планове",
        1: "Аварійне",
        2: "Оперативне",
        3: "Планове",
    }
    return {
        "source_id": o.get("id"),
        "city_name": city.get("name") or "",
        "street_name": street.get("name") or "",
        "city_id": city.get("id"),
        "street_id": street.get("id"),
        "buildings": (o.get("buildingNames") or "").strip(),
        "date_from": o.get("dateEvent") or "",
        "date_to": o.get("datePlanIn") or "",
        "type": type_off,
        "type_name": type_map.get(type_off, f"Тип {type_off}"),
        "raw": {
            "typeOff": type_off,
            "id": o.get("id"),
        },
    }


# ---------- site screenshot (один скрін ЖОЕ для /check і каналу) ----------

SITE_SCREENSHOT_ID = "site_screenshot_latest"
SITE_SCREENSHOT_FILENAME = "site_screenshot_latest.png"


def save_site_screenshot(png_bytes: bytes, *, source_hash: str = "") -> Any:
    """
    Зберігає офіційний скрін сайту ЖОЕ в GridFS (bucket schedule_images)
    і метадані в колекції site_meta.
    """
    if not png_bytes:
        return None
    db = get_db()
    fs = GridFS(db, collection="schedule_images")

    # прибрати старий файл з тим самим ім'ям
    for old in fs.find({"filename": SITE_SCREENSHOT_FILENAME}):
        try:
            fs.delete(old._id)
        except Exception:
            pass

    file_id = fs.put(
        png_bytes,
        filename=SITE_SCREENSHOT_FILENAME,
        content_type="image/png",
        metadata={"kind": "site_screenshot", "hash": source_hash or ""},
    )
    db["site_meta"].update_one(
        {"_id": SITE_SCREENSHOT_ID},
        {
            "$set": {
                "image_file_id": file_id,
                "filename": SITE_SCREENSHOT_FILENAME,
                "hash": source_hash or "",
                "size": len(png_bytes),
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
    return file_id


def get_site_screenshot() -> Optional[bytes]:
    """Читає останній скрін ЖОЕ з GridFS."""
    db = get_db()
    meta = db["site_meta"].find_one({"_id": SITE_SCREENSHOT_ID}) or {}
    file_id = meta.get("image_file_id")
    fs = GridFS(db, collection="schedule_images")
    if file_id:
        try:
            return fs.get(file_id).read()
        except Exception:
            pass
    try:
        grid_out = fs.get_last_version(SITE_SCREENSHOT_FILENAME)
        return grid_out.read()
    except Exception:
        return None
