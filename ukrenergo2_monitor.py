"""Моніторинг статусу енергосистеми — Укренерго 2 (неофіційне джерело).

URL regions.json береться з секрету UKRENERGO2_REGIONS_URL (не хардкодиться).

Окремий скрипт від офіційного ukrenergo_monitor.py (API з Matrix-ключем).
Порівнює status + status_code для обраного регіону з MongoDB (колекція ukrenergo2_states).
Публічне сповіщення лише при зміні. Перший запуск лише зберігає базовий стан.

Файл data/ukrenergo2_state.json більше НЕ використовується і НЕ комітиться в git.
"""

from __future__ import annotations

import html
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import requests

from log_utils import log_to_buffer, send_log_to_channel
from telegram_handler import send_notification

# URL на regions.json — тільки через секрет (не хардкодити в репо)
REGIONS_URL = (os.getenv("UKRENERGO2_REGIONS_URL") or "").strip()
# За замовчуванням Житомир; можна перевизначити секретом UKRENERGO2_REGION_SLUG
REGION_SLUG = (os.getenv("UKRENERGO2_REGION_SLUG") or "zhytomyr").strip().lower()
REGION_NAME_OVERRIDE = (os.getenv("UKRENERGO2_REGION_NAME") or "").strip()
SUBSCRIBE = os.getenv("SUBSCRIBE", "")
STALE_HOURS = 2

# Mongo: той самий URI, що для Маячка (або окремий)
MONGO_URI = (
    os.getenv("MAYACHOK_MONGO_URI")
    or os.getenv("MONGO_URI")
    or ""
).strip()
MONGO_DB = (
    os.getenv("MAYACHOK_MONGO_DB")
    or os.getenv("MONGO_DB_NAME")
    or "powerbot"
).strip()
# Колекція стану Укренерго 2
COLLECTION = "ukrenergo2_states"
# Опційно дублюємо в regionstatuses (схема бота Маячок)
SYNC_REGIONSTATUS = (os.getenv("UKRENERGO2_SYNC_REGIONSTATUS") or "1").strip() not in (
    "0",
    "false",
    "no",
)

# Коди + тексти для каналу (стиль офіційних повідомлень Укренерго)
# code -> (title, body). body може містити {region}
STATUS_BY_CODE = {
    3: (
        "Електроенергії достатньо",
        "Користуйтеся електроприладами у звичайному режимі.",
    ),
    4: (
        "Будь ласка, споживайте електроенергію ощадливо",
        "Через пошкодження мереж та об'єктів генерації – енергосистемі складно "
        "забезпечити потреби всіх споживачів у вашому регіоні. Будь ласка, не вмикайте "
        "кілька потужних електроприладів одночасно. Кондиціонер, обігрівач, чайник, "
        "фен, пилосос, електроплиту та праску - використовуйте почергово.",
    ),
    9: (
        "Застосовані графіки погодинних відключень",
        "Внаслідок складної ситуації в енергосистемі – вимушено запроваджені графіки "
        "погодинних відключень. Будь ласка, не користуйтесь потужними електроприладами "
        "до скасування обмежень.",
    ),
    10: (
        "Застосовані аварійні відключення",
        "Ситуація в енергосистемі є дуже складною. У вашому регіоні застосовані аварійні "
        "відключення. Раніше оприлюднені графіки погодинних відключень наразі не діють. "
        "Якщо зараз у вас є електроенергія – споживайте її максимально ощадливо.",
    ),
    11: (
        "Знеструмлення – наслідок пошкодження мережі",
        "Живлення відсутнє через пошкодження мережі. Раніше оприлюднені графіки "
        "погодинних відключень наразі не діють. Якщо зараз у вас є електроенергія – "
        "споживайте її максимально ощадливо.",
    ),
}

# slug / API status key -> code + emoji (для snapshot)
STATUS_META = {
    "normal": {"code": 3, "emoji": "🟢", "title_uk": STATUS_BY_CODE[3][0]},
    "shortage": {"code": 4, "emoji": "🟡", "title_uk": STATUS_BY_CODE[4][0]},
    "outage_schedules": {"code": 9, "emoji": "🔴", "title_uk": STATUS_BY_CODE[9][0]},
    "emergency_outages": {"code": 10, "emoji": "🚨", "title_uk": STATUS_BY_CODE[10][0]},
    "grid_damage": {"code": 11, "emoji": "⚫️", "title_uk": STATUS_BY_CODE[11][0]},
}

UKRENERGO_SITE_URL = (os.getenv("UKRENERGO_SITE_URL") or "https://ua.energy").strip()


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _get_db():
    if not MONGO_URI:
        raise RuntimeError(
            "MAYACHOK_MONGO_URI (або MONGO_URI) не задано — "
            "стан Укренерго 2 зберігається в MongoDB"
        )
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[MONGO_DB]


def load_state() -> Dict:
    """Читає попередній знімок з Mongo по slug."""
    try:
        db = _get_db()
        doc = db[COLLECTION].find_one({"slug": REGION_SLUG}, {"_id": 0})
        if not doc:
            log_to_buffer(f"📋 Mongo: стану для slug={REGION_SLUG} ще немає")
            return {}
        log_to_buffer(
            f"📋 Mongo: завантажено стан "
            f"{doc.get('status')}/{doc.get('status_code')} "
            f"(checked_at={doc.get('checked_at')})"
        )
        return doc
    except Exception as e:
        log_to_buffer(f"⚠️ Не вдалось прочитати стан з Mongo: {e}")
        return {}


def save_state(snapshot: Dict) -> None:
    """Upsert знімка в Mongo (+ опційно regionstatuses для бота)."""
    db = _get_db()
    doc = dict(snapshot)
    doc["slug"] = REGION_SLUG
    doc["updatedAt"] = datetime.utcnow()

    db[COLLECTION].update_one(
        {"slug": REGION_SLUG},
        {"$set": doc},
        upsert=True,
    )
    log_to_buffer(
        f"💾 Mongo: стан збережено в {COLLECTION} "
        f"(slug={REGION_SLUG}, status={doc.get('status')})"
    )

    if SYNC_REGIONSTATUS:
        try:
            period = doc.get("period") or {}
            region_doc = {
                "slug": REGION_SLUG,
                "status": doc.get("status") or "",
                "statusCode": doc.get("status_code"),
                "emoji": doc.get("emoji"),
                "title": doc.get("title_uk") or doc.get("title"),
                "since": doc.get("since"),
                "period": {
                    "start": period.get("start") if isinstance(period, dict) else None,
                    "end": period.get("end") if isinstance(period, dict) else None,
                },
                "generatedAt": doc.get("generated_at"),
                "changedAt": doc.get("changed_at"),
                "fetchedAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow(),
            }
            db["regionstatuses"].update_one(
                {"slug": REGION_SLUG},
                {"$set": region_doc},
                upsert=True,
            )
            log_to_buffer("💾 Mongo: regionstatuses синхронізовано")
        except Exception as e:
            log_to_buffer(f"⚠️ regionstatuses sync: {e}")


def is_stale(generated_at: Optional[str]) -> bool:
    if not generated_at:
        return True
    try:
        ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - ts > timedelta(hours=STALE_HOURS)
    except Exception:
        return True


def fetch_regions() -> Dict[str, Any]:
    if not REGIONS_URL:
        raise RuntimeError(
            "UKRENERGO2_REGIONS_URL не задано "
            "(потрібен секрет з посиланням на regions.json)"
        )
    try:
        from urllib.parse import urlparse

        host = urlparse(REGIONS_URL).netloc or "…"
    except Exception:
        host = "…"
    log_to_buffer(f"📡 Завантажую regions.json ({host})")
    resp = requests.get(REGIONS_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    log_to_buffer(
        f"✓ generated_at={data.get('generated_at')}, "
        f"regions={len(data.get('regions') or [])}"
    )
    return data


def find_region(data: Dict[str, Any], slug: str) -> Dict[str, Any]:
    for r in data.get("regions") or []:
        if isinstance(r, dict) and (r.get("slug") or "").lower() == slug:
            return r
    raise RuntimeError(f"Регіон slug='{slug}' не знайдено в regions.json")


def fmt_kyiv(iso: Optional[str]) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        try:
            import pytz

            dt = dt.astimezone(pytz.timezone("Europe/Kyiv"))
        except Exception:
            dt = dt.astimezone(timezone(timedelta(hours=3)))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso


def build_snapshot(data: Dict[str, Any], region: Dict[str, Any]) -> Dict:
    status = region.get("status") or "unknown"
    status_code = region.get("status_code")
    meta = STATUS_META.get(status, {})
    emoji = region.get("emoji") or meta.get("emoji") or "❔"
    title = region.get("title_uk") or meta.get("title_uk") or status
    name_uk = REGION_NAME_OVERRIDE or region.get("name_uk") or slug_title(REGION_SLUG)

    return {
        "source": "ua-power-status",
        "slug": REGION_SLUG,
        "name_uk": name_uk,
        "status": status,
        "status_code": status_code,
        "emoji": emoji,
        "title_uk": title,
        "since": region.get("since"),
        "period": region.get("period"),
        "next_change": region.get("next_change"),
        "generated_at": data.get("generated_at"),
        "changed_at": data.get("changed_at"),
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }


def slug_title(slug: str) -> str:
    mapping = {
        "zhytomyr": "Житомирська область",
        "kyiv": "м. Київ",
        "kyiv-oblast": "Київська область",
    }
    return mapping.get(slug, slug)


def comparable(snapshot: Dict) -> Dict:
    """Що порівнюємо для рішення «чи змінилось»."""
    period = snapshot.get("period") or {}
    return {
        "status": snapshot.get("status"),
        "status_code": snapshot.get("status_code"),
        "title_uk": snapshot.get("title_uk"),
        "since": snapshot.get("since"),
        "period_start": period.get("start") if isinstance(period, dict) else None,
        "period_end": period.get("end") if isinstance(period, dict) else None,
    }


def format_message(prev: Dict, cur: Dict, stale: bool) -> str:
    """Повідомлення в канал у стилі офіційних інфо Укренерго."""
    code = cur.get("status_code")
    try:
        code = int(code) if code is not None else None
    except (TypeError, ValueError):
        code = None

    # fallback через status key
    if code not in STATUS_BY_CODE:
        meta = STATUS_META.get(cur.get("status") or "", {})
        code = meta.get("code")

    region = cur.get("name_uk") or slug_title(REGION_SLUG)

    if code in STATUS_BY_CODE:
        title, body = STATUS_BY_CODE[code]
    else:
        title = cur.get("title_uk") or cur.get("status") or "Оновлення статусу"
        body = "Стан енергосистеми оновлено."

    # Для кодів 3 і 9 у заголовку згадуємо область (як у твоєму прикладі)
    if code == 3:
        title_line = f"Електроенергії для {region} достатньо"
    elif code == 9:
        title_line = f"Застосовані графіки погодинних відключень для {region}."
    else:
        title_line = title

    lines = [
        "Укренерго інформує -",
        esc(title_line),
        "",
        esc(body),
    ]

    # Прогнозований період — не для «достатньо» (код 3)
    if code != 3:
        period_block = _format_period_block(cur.get("period"))
        if period_block:
            lines.append("")
            lines.extend(period_block)

    lines.append("")
    footer_parts = []
    if UKRENERGO_SITE_URL:
        footer_parts.append(
            f'<a href="{esc(UKRENERGO_SITE_URL)}">Сайт НЕК «Укренерго»</a>'
        )
    if SUBSCRIBE:
        footer_parts.append(f'<a href="{esc(SUBSCRIBE)}">⚡️ ПІДПИСАТИСЯ</a>')
    if footer_parts:
        lines.append(" | ".join(footer_parts))

    # Час повідомлення (Київ)
    now_str = _now_kyiv_hm()
    if now_str:
        lines.append(f"🕐 {esc(now_str)}")

    if stale:
        lines.append("")
        lines.append(
            f"⚠️ Дані можуть бути застарілими "
            f"(generated_at={esc(cur.get('generated_at'))})"
        )

    return "\n".join(lines)


def _now_kyiv_hm() -> str:
    try:
        import pytz

        now = datetime.now(pytz.timezone("Europe/Kyiv"))
    except Exception:
        now = datetime.now(timezone(timedelta(hours=3)))
    return now.strftime("%H:%M %d.%m")


def _format_period_block(period) -> list:
    """
    Один день:  Прогнозований період: 06:00-23:59
    Різні дні:  Прогнозований період:
                з 22:00 (16.10)
                до 01:00 (17.10)
    """
    if not isinstance(period, dict):
        return []
    start = period.get("start")
    end = period.get("end")
    if not start and not end:
        return []

    def _parts(iso: str):
        if not iso:
            return None, None
        try:
            dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            try:
                import pytz

                dt = dt.astimezone(pytz.timezone("Europe/Kyiv"))
            except Exception:
                dt = dt.astimezone(timezone(timedelta(hours=3)))
            return dt.strftime("%H:%M"), dt.strftime("%d.%m")
        except Exception:
            return None, None

    sh, sd = _parts(start) if start else (None, None)
    eh, ed = _parts(end) if end else (None, None)

    if not sh and not eh:
        return []

    # один календарний день
    if sd and ed and sd == ed and sh and eh:
        return [f"Прогнозований період: {sh}-{eh}"]

    lines = ["Прогнозований період:"]
    if sh and sd:
        lines.append(f"з {sh} ({sd})")
    elif sh:
        lines.append(f"з {sh}")
    if eh and ed:
        lines.append(f"до {eh} ({ed})")
    elif eh:
        lines.append(f"до {eh}")
    return lines



def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer(f"🚀 СТАРТ УКРЕНЕРГО 2 · slug={REGION_SLUG}")
    try:
        data = fetch_regions()
        region = find_region(data, REGION_SLUG)
        current = build_snapshot(data, region)
        stale = is_stale(current.get("generated_at"))

        log_to_buffer(
            f"✓ Зараз: {current.get('emoji')} {current.get('title_uk')} "
            f"(status={current.get('status')}, code={current.get('status_code')})"
        )
        if stale:
            log_to_buffer(
                f"⚠️ generated_at старший за {STALE_HOURS} год — дані можуть бути неактуальними"
            )

        previous = load_state()
        if not previous:
            save_state(current)
            log_to_buffer("📋 Базовий стан збережено в Mongo, сповіщення не шлю")
            return

        if comparable(previous) == comparable(current):
            # Оновлюємо лише checked_at, без сповіщення
            try:
                db = _get_db()
                db[COLLECTION].update_one(
                    {"slug": REGION_SLUG},
                    {"$set": {"checked_at": current.get("checked_at")}},
                )
            except Exception:
                pass
            log_to_buffer("✅ Статус регіону не змінився")
            return

        log_to_buffer(
            f"🔔 Зміна: {previous.get('status')}/{previous.get('status_code')} "
            f"→ {current.get('status')}/{current.get('status_code')}"
        )
        message = format_message(previous, current, stale)
        ok = send_notification(message)
        if not ok:
            log_to_buffer("❌ Не вдалось надіслати сповіщення, стан не перезаписую")
            return
        save_state(current)
        log_to_buffer("✅ Сповіщення надіслано, стан збережено в Mongo")
    except Exception as e:
        log_to_buffer(f"❌ Критична помилка Укренерго 2: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ ВИКОНАННЯ СКРИПТА · УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
