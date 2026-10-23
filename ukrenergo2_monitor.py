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
from pathlib import Path
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
IMAGES_DIR = Path("assets/ukrenergo2")
# Опційно дублюємо в regionstatuses (схема бота Маячок)
SYNC_REGIONSTATUS = (os.getenv("UKRENERGO2_SYNC_REGIONSTATUS") or "1").strip() not in (
    "0",
    "false",
    "no",
)

# Коди + тексти для каналу (стиль офіційних повідомлень Укренерго)
# code -> (title, body). body може містити {region}
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

# slug / API status key -> code (emoji/title беруться з STATUS_BY_CODE)
STATUS_META = {
    "normal": {"code": 3, "emoji": "🟢", "title_uk": "Електроенергії достатньо"},
    "shortage": {"code": 4, "emoji": "🟡", "title_uk": "Будь ласка, споживайте електроенергію ощадливо"},
    "outage_schedules": {"code": 9, "emoji": "🟠", "title_uk": "Застосовані графіки погодинних відключень"},
    "emergency_outages": {"code": 10, "emoji": "🔴", "title_uk": "Застосовані аварійні відключення"},
    "grid_damage": {"code": 11, "emoji": "🚨", "title_uk": "Знеструмлення – наслідок пошкодження мережі"},
}

UKRENERGO_SITE_URL = (os.getenv("UKRENERGO_SITE_URL") or "").strip()


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



def region_forms(name_uk: str, slug: str) -> tuple:
    """(називний, родовий для «для …», місцевий для «в …»)."""
    # slug → (genitive, locative)
    by_slug = {
        "zhytomyr": ("Житомирської області", "Житомирській області"),
        "kyiv": ("м. Києва", "м. Києві"),
        "kyiv-oblast": ("Київської області", "Київській області"),
        "vinnytsia": ("Вінницької області", "Вінницькій області"),
        "volyn": ("Волинської області", "Волинській області"),
        "dnipro": ("Дніпропетровської області", "Дніпропетровській області"),
        "donetsk": ("Донецької області", "Донецькій області"),
        "zhytomyrska": ("Житомирської області", "Житомирській області"),
        "zakarpattia": ("Закарпатської області", "Закарпатській області"),
        "zaporizhzhia": ("Запорізької області", "Запорізькій області"),
        "ivano-frankivsk": ("Івано-Франківської області", "Івано-Франківській області"),
        "kirovohrad": ("Кіровоградської області", "Кіровоградській області"),
        "luhansk": ("Луганської області", "Луганській області"),
        "lviv": ("Львівської області", "Львівській області"),
        "mykolaiv": ("Миколаївської області", "Миколаївській області"),
        "odesa": ("Одеської області", "Одеській області"),
        "poltava": ("Полтавської області", "Полтавській області"),
        "rivne": ("Рівненської області", "Рівненській області"),
        "sumy": ("Сумської області", "Сумській області"),
        "ternopil": ("Тернопільської області", "Тернопільській області"),
        "kharkiv": ("Харківської області", "Харківській області"),
        "kherson": ("Херсонської області", "Херсонській області"),
        "khmelnytskyi": ("Хмельницької області", "Хмельницькій області"),
        "cherkasy": ("Черкаської області", "Черкаській області"),
        "chernihiv": ("Чернігівської області", "Чернігівській області"),
        "chernivtsi": ("Чернівецької області", "Чернівецькій області"),
    }
    if slug in by_slug:
        gen, loc = by_slug[slug]
        return name_uk, gen, loc
    # евристика: «...ська область» → «...ської області» / «...ській області»
    n = (name_uk or "").strip()
    if n.endswith("ська область"):
        base = n[: -len("ська область")]
        return n, f"{base}ської області", f"{base}ській області"
    if n.endswith("зька область"):
        base = n[: -len("зька область")]
        return n, f"{base}зької області", f"{base}зькій області"
    if n.endswith("цька область"):
        base = n[: -len("цька область")]
        return n, f"{base}цької області", f"{base}цькій області"
    if n.endswith("а область"):
        base = n[: -len("а область")]
        return n, f"{base}ої області", f"{base}ій області"
    return n, n, n


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

    name_uk = cur.get("name_uk") or slug_title(REGION_SLUG)
    _, region_gen, region_loc = region_forms(name_uk, REGION_SLUG)

    if code in STATUS_BY_CODE:
        emoji, title_tpl, body = STATUS_BY_CODE[code]
        title_line = f"{emoji} {title_tpl.format(region_gen=region_gen, region_loc=region_loc)}"
    else:
        emoji = cur.get("emoji") or "⚡️"
        title_line = f"{emoji} {cur.get('title_uk') or cur.get('status') or 'Оновлення статусу'}"
        body = "Стан енергосистеми оновлено."

    # Читабельність: жирний заголовок статусу, підпис періоду; текст без змін
    lines = [
        "Укренерго інформує -",
        f"<b>{esc(title_line)}</b>",
        "",
        body,
    ]

    # Прогнозований період — не для «достатньо» (код 3)
    if code != 3:
        period_block = _format_period_block(cur.get("period"))
        if period_block:
            lines.append("")
            # перший рядок — підпис, решта значення
            if len(period_block) == 1 and period_block[0].startswith("Прогнозований період:"):
                # "Прогнозований період: 06:00-23:59"
                label, _, rest = period_block[0].partition(":")
                lines.append(f"<b>{esc(label.strip())}:</b> {esc(rest.strip())}")
            else:
                lines.append(f"<b>{esc(period_block[0])}</b>")
                for row in period_block[1:]:
                    lines.append(esc(row))

    lines.append("")
    footer_parts = []
    if UKRENERGO_SITE_URL:
        footer_parts.append(
            f'<a href="{esc(UKRENERGO_SITE_URL)}">Сайт НЕК «Укренерго»</a>'
        )
    if SUBSCRIBE:
        footer_parts.append(f'<a href="{esc(SUBSCRIBE)}"><b>⚡️ ПІДПИСАТИСЯ</b></a>')
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



def get_status_image(status_code) -> Path | None:
    """Повертає шлях до картинки статусу або None."""
    try:
        code = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        code = None
    if code is None:
        return None
    path = IMAGES_DIR / f"{code}.jpg"
    if path.exists():
        return path
    # fallback png
    path_png = IMAGES_DIR / f"{code}.png"
    if path_png.exists():
        return path_png
    return None


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
        image_path = get_status_image(current.get("status_code"))
        if image_path:
            log_to_buffer(f"🖼 Картинка: {image_path.name}")
        else:
            log_to_buffer("ℹ️ Картинки для цього статусу немає — шлю тільки текст")
        ok = send_notification(message, image_path=image_path)
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
