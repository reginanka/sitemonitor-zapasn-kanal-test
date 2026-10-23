"""Тест повідомлення в канал (Укренерго 2) — ПОВНІСТЮ автономний.

Не змінює Mongo. Не залежить від текстів у ukrenergo2_monitor.py:
усі шаблони повідомлень і розмітка живуть ТУТ.

Потрібні env (secrets):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, TELEGRAM_LOG_CHANNEL_ID
  UKRENERGO2_REGIONS_URL
  опційно: UKRENERGO2_REGION_SLUG, UKRENERGO2_REGION_NAME
  опційно: MAYACHOK_MONGO_URI, MAYACHOK_MONGO_DB (лише щоб показати «було» в логах)
  опційно: UKRENERGO_SITE_URL, SUBSCRIBE
"""

from __future__ import annotations

import html
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from log_utils import log_to_buffer, send_log_to_channel
from telegram_handler import send_notification

REGIONS_URL = (os.getenv("UKRENERGO2_REGIONS_URL") or "").strip()
REGION_SLUG = (os.getenv("UKRENERGO2_REGION_SLUG") or "zhytomyr").strip().lower()
REGION_NAME_OVERRIDE = (os.getenv("UKRENERGO2_REGION_NAME") or "").strip()
SUBSCRIBE = (os.getenv("SUBSCRIBE") or "").strip()
UKRENERGO_SITE_URL = (os.getenv("UKRENERGO_SITE_URL") or "").strip()
MONGO_URI = (
    os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI") or ""
).strip()
MONGO_DB = (
    os.getenv("MAYACHOK_MONGO_DB") or os.getenv("MONGO_DB_NAME") or "powerbot"
).strip()
COLLECTION = "ukrenergo2_states"
IMAGES_DIR = Path("assets/ukrenergo2")
STALE_HOURS = 2

# --- Шаблони каналу (як ти задала) ---
STATUS_BY_CODE = {
    # code: (emoji, title_template with {region_gen}/{region_loc}, body)
    # body містить HTML (<b>…) — передається без повного esc
    3: (
        "🟢",
        "Електроенергії для {region_gen} достатньо",
        "Користуйтеся електроприладами у звичайному режимі.",
    ),
    4: (
        "🟡",
        "Будь ласка, споживайте електроенергію ощадливо в {region_loc}",
        "Через пошкодження мереж та об'єктів генерації – енергосистемі складно "
        "забезпечити потреби всіх споживачів у вашому регіоні. Будь ласка, не вмикайте "
        "кілька потужних електроприладів одночасно. Кондиціонер, обігрівач, чайник, "
        "фен, пилосос, електроплиту та праску - використовуйте почергово.",
    ),
    9: (
        "🟠",
        "Застосовані графіки погодинних відключень для {region_gen}.",
        "Внаслідок складної ситуації в енергосистемі – вимушено запроваджені графіки "
        "погодинних відключень. Будь ласка, не користуйтесь потужними електроприладами "
        "до скасування обмежень.",
    ),
    10: (
        "🔴",
        "Застосовані аварійні відключення в {region_loc}",
        "‼️Раніше оприлюднені <b>графіки погодинних відключень</b> за розпорядженням «Укренерго» <b>наразі не діють</b>.\n\n"
        "Ситуація в енергосистемі є дуже складною. Якщо зараз у вас є електроенергія – споживайте її максимально ощадливо.",
    ),
    11: (
        "🚨",
        "Знеструмлення – наслідок пошкодження мережі в {region_loc}",
        "‼️Раніше оприлюднені <b>графіки погодинних відключень</b> за розпорядженням «Укренерго» <b>наразі не діють</b>.\n\n"
        "Значна кількість абонентів – знеструмлена внаслідок пошкодження елементів "
        "мережі, що забезпечували живлення побутових споживачів та об'єктів "
        "інфраструктури. Тривалість відновлювальних робіт залежить від "
        "безпекової ситуації у регіоні. Якщо зараз у вас є електроенергія – споживайте "
        "її максимально ощадливо.",
    ),
}

STATUS_META = {
    "normal": 3,
    "shortage": 4,
    "outage_schedules": 9,
    "emergency_outages": 10,
    "grid_damage": 11,
}


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def region_forms(name_uk: str, slug: str) -> tuple:
    by_slug = {
        "zhytomyr": ("Житомирської області", "Житомирській області"),
        "zhytomyrska": ("Житомирської області", "Житомирській області"),
        "kyiv": ("м. Києва", "м. Києві"),
        "kyiv-oblast": ("Київської області", "Київській області"),
    }
    if slug in by_slug:
        gen, loc = by_slug[slug]
        return name_uk, gen, loc
    n = (name_uk or "").strip()
    if n.endswith("ська область"):
        base = n[: -len("ська область")]
        return n, f"{base}ської області", f"{base}ській області"
    return n, n, n


def slug_title(slug: str) -> str:
    return {
        "zhytomyr": "Житомирська область",
        "kyiv": "м. Київ",
        "kyiv-oblast": "Київська область",
    }.get(slug, slug)


def _now_kyiv_hm() -> str:
    try:
        import pytz

        now = datetime.now(pytz.timezone("Europe/Kyiv"))
    except Exception:
        now = datetime.now(timezone(timedelta(hours=3)))
    return now.strftime("%H:%M %d.%m")


def _format_period_block(period) -> list:
    if not isinstance(period, dict):
        return []
    start, end = period.get("start"), period.get("end")
    if not start and not end:
        return []

    def _parts(iso: str):
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


def format_channel_message(cur: Dict, stale: bool = False) -> str:
    code = cur.get("status_code")
    try:
        code = int(code) if code is not None else None
    except (TypeError, ValueError):
        code = None
    if code not in STATUS_BY_CODE:
        code = STATUS_META.get(cur.get("status") or "")

    name_uk = cur.get("name_uk") or slug_title(REGION_SLUG)
    _, region_gen, region_loc = region_forms(name_uk, REGION_SLUG)

    if code in STATUS_BY_CODE:
        emoji, title_tpl, body = STATUS_BY_CODE[code]
        title_line = f"{emoji} {title_tpl.format(region_gen=region_gen, region_loc=region_loc)}"
    else:
        title_line = cur.get("title_uk") or cur.get("status") or "Оновлення статусу"
        body = "Стан енергосистеми оновлено."

    lines = [
        "Укренерго інформує -",
        f"<b>{esc(title_line)}</b>",
        "",
        body,
    ]

    if code != 3:
        period_block = _format_period_block(cur.get("period"))
        if period_block:
            lines.append("")
            if len(period_block) == 1 and period_block[0].startswith("Прогнозований період:"):
                label, _, rest = period_block[0].partition(":")
                lines.append(f"<b>{esc(label.strip())}:</b> {esc(rest.strip())}")
            else:
                lines.append(f"<b>{esc(period_block[0])}</b>")
                for row in period_block[1:]:
                    lines.append(esc(row))

    lines.append("")
    footer = []
    if UKRENERGO_SITE_URL:
        footer.append(f'<a href="{esc(UKRENERGO_SITE_URL)}">Сайт НЕК «Укренерго»</a>')
    if SUBSCRIBE:
        footer.append(f'<a href="{esc(SUBSCRIBE)}"><b>⚡️ ПІДПИСАТИСЯ</b></a>')
    if footer:
        lines.append(" | ".join(footer))

    lines.append(f"🕐 {esc(_now_kyiv_hm())}")

    if stale:
        lines.append("")
        lines.append(
            f"⚠️ Дані можуть бути застарілими (generated_at={esc(cur.get('generated_at'))})"
        )

    return "\n".join(lines)


def fetch_live() -> Dict[str, Any]:
    if not REGIONS_URL:
        raise RuntimeError("UKRENERGO2_REGIONS_URL не задано")
    resp = requests.get(REGIONS_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    region = None
    for r in data.get("regions") or []:
        if isinstance(r, dict) and (r.get("slug") or "").lower() == REGION_SLUG:
            region = r
            break
    if not region:
        raise RuntimeError(f"Регіон slug={REGION_SLUG} не знайдено")

    status = region.get("status") or "unknown"
    code = region.get("status_code")
    name_uk = REGION_NAME_OVERRIDE or region.get("name_uk") or slug_title(REGION_SLUG)
    return {
        "slug": REGION_SLUG,
        "name_uk": name_uk,
        "status": status,
        "status_code": code,
        "title_uk": region.get("title_uk"),
        "emoji": region.get("emoji"),
        "since": region.get("since"),
        "period": region.get("period"),
        "generated_at": data.get("generated_at"),
        "changed_at": data.get("changed_at"),
    }


def peek_mongo() -> Dict:
    """Тільки читання для лога. Нічого не пише."""
    if not MONGO_URI:
        return {}
    try:
        from pymongo import MongoClient

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        doc = client[MONGO_DB][COLLECTION].find_one({"slug": REGION_SLUG}, {"_id": 0})
        return doc or {}
    except Exception as e:
        log_to_buffer(f"⚠️ Mongo read skip: {e}")
        return {}


def is_stale(generated_at: Optional[str]) -> bool:
    if not generated_at:
        return True
    try:
        ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - ts > timedelta(hours=STALE_HOURS)
    except Exception:
        return True


def get_status_image(status_code) -> Path | None:
    try:
        code = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        code = None
    if code is None:
        return None
    for ext in (".jpg", ".png"):
        path = IMAGES_DIR / f"{code}{ext}"
        if path.exists():
            return path
    return None


def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer(f"🧪 TEST УКРЕНЕРГО 2 · slug={REGION_SLUG} (без запису в Mongo, свої шаблони)")
    try:
        prev = peek_mongo()
        if prev:
            log_to_buffer(
                f"📋 Mongo (лише лог): {prev.get('status')}/{prev.get('status_code')}"
            )
        else:
            log_to_buffer("📋 Mongo порожній або недоступний (ок для тесту)")

        current = fetch_live()
        log_to_buffer(
            f"✓ Live: code={current.get('status_code')} status={current.get('status')} "
            f"period={current.get('period')}"
        )

        body = format_channel_message(current, stale=is_stale(current.get("generated_at")))
        message = (
            "🧪 <b>ТЕСТ</b> (нічого не записано в базу)\n"
            "Так виглядатиме сповіщення при зміні статусу:\n\n"
            + body
        )

        image_path = get_status_image(current.get("status_code"))
        ok = send_notification(message, image_path=image_path)
        log_to_buffer("✅ Тест надіслано в канал" if ok else "❌ Не вдалось надіслати")
    except Exception as e:
        log_to_buffer(f"❌ Test: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ · TEST УКРЕНЕРГО 2")


if __name__ == "__main__":
    main()
