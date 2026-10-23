"""Моніторинг стану енергосистеми однієї області за API Укренерго.

Порівнює знімок (код стану + version + текст + рекомендація + заплановані зміни)
з data/ukrenergo_state.json. Публічне сповіщення лише якщо знімок змінився.
Перший запуск лише зберігає базовий стан. 
"""

import html
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from log_utils import log_to_buffer, send_log_to_channel
from telegram_handler import send_notification

API_BASE = (os.getenv("UKRENERGO_API_BASE") or "").strip().rstrip("/")
MATRIX_KEY = os.getenv("UKRENERGO_MATRIX_KEY", "")
def require_region_id() -> int:
    raw = (os.getenv("UKRENERGO_REGION_ID") or "").strip()
    if not raw.isdigit():
        raise RuntimeError("UKRENERGO_REGION_ID не задано")
    return int(raw)

REGION_NAME = (os.getenv("UKRENERGO_REGION_NAME") or "").strip()
SUBSCRIBE = os.getenv("SUBSCRIBE", "")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "ukrenergo_state.json"
IMAGES_DIR = Path("assets/ukrenergo2")  # спільні картинки зі статусами 3/4/9/10/11

FALLBACK_STATES = {
    3: ("Електроенергії достатньо", "Споживання в межах можливостей енергосистеми."),
    4: ("Будь ласка, споживайте електроенергію ощадливо", "Енергосистема працює з дефіцитом."),
    9: ("Застосовані графіки погодинних відключень", "Діють графіки обмеження споживання."),
    10: ("Застосовані аварійні відключення", "Застосовані екстрені/аварійні відключення."),
    11: ("Знеструмлення – наслідок пошкодження мережі", "Живлення відсутнє через пошкодження мережі."),
}


def api_get(path: str, params: Optional[Dict] = None) -> Any:
    if not MATRIX_KEY:
        raise RuntimeError("UKRENERGO_MATRIX_KEY не задано")
    if not API_BASE:
        raise RuntimeError("UKRENERGO_API_BASE не задано")
    endpoint = path.lstrip("/")
    url = API_BASE + "/" + endpoint
    try:
        resp = requests.get(
            url,
            params=params,
            headers={
                "Matrix": MATRIX_KEY,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "okhttp/4.12.0",
            },
            timeout=25,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"{endpoint}: мережева помилка ({type(e).__name__})") from None
    if resp.status_code >= 400:
        raise RuntimeError(f"{endpoint}: HTTP {resp.status_code}")
    try:
        return resp.json()
    except ValueError:
        raise RuntimeError(f"{endpoint}: відповідь не JSON") from None


def load_state() -> Dict:
    if not STATE_FILE.exists():
        return {}
    try:
        with STATE_FILE.open(encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log_to_buffer(f"⚠️ Не вдалось прочитати {STATE_FILE.name}: {e}")
        return {}


def save_state(snapshot: Dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


def index_by_id(items: List[Dict]) -> Dict[int, Dict]:
    out: Dict[int, Dict] = {}
    for item in items or []:
        if isinstance(item, dict) and item.get("id") is not None:
            out[int(item["id"])] = item
    return out


def build_snapshot() -> Dict:
    if not REGION_NAME:
        raise RuntimeError("UKRENERGO_REGION_NAME не задано")
    region_id = require_region_id()
    log_to_buffer(f"📡 Укренерго: {REGION_NAME}")
    states_payload = api_get("dictionary/getStates")
    recs_payload = api_get("dictionary/getRecommendations")
    current = api_get("regions/getRegionState", {"regionId": region_id})
    try:
        scheduled = api_get("regions/getScheduledChanges", {"regionId": region_id})
    except Exception as e:
        log_to_buffer(f"⚠️ getScheduledChanges недоступний: {e}")
        scheduled = []

    states = index_by_id((states_payload or {}).get("data") or [])
    recs = index_by_id((recs_payload or {}).get("data") or [])
    state_obj = (current or {}).get("state") or {}
    state_id = state_obj.get("id")
    state_version = state_obj.get("version")
    if state_id is None:
        raise RuntimeError(f"Немає state.id у відповіді: {current}")

    state_meta = states.get(int(state_id), {})
    name = state_meta.get("name") or FALLBACK_STATES.get(int(state_id), ("Невідомий стан", ""))[0]
    desc = state_meta.get("desc") or FALLBACK_STATES.get(int(state_id), ("", ""))[1]
    name_en = state_meta.get("name_eng") or ""
    rec_id = state_meta.get("recommendationId")
    recommendation = None
    if rec_id is not None and int(rec_id) in recs:
        rec = recs[int(rec_id)]
        recommendation = {
            "id": int(rec_id),
            "name": rec.get("name") or "",
            "desc": rec.get("desc") or "",
        }

    sched_norm = []
    if isinstance(scheduled, list):
        for item in scheduled:
            if not isinstance(item, dict):
                continue
            st = item.get("state") or {}
            sched_norm.append({
                "startTime": item.get("startTime"),
                "endTime": item.get("endTime"),
                "changeTime": item.get("changeTime"),
                "state_id": st.get("id") if isinstance(st, dict) else st,
                "state_version": st.get("version") if isinstance(st, dict) else None,
            })

    snapshot = {
        "region_id": region_id,
        "region_name": REGION_NAME,
        "state_id": int(state_id),
        "state_version": state_version,
        "name": name,
        "desc": desc,
        "name_en": name_en,
        "icon": state_meta.get("icon"),
        "color": state_meta.get("hexColor"),
        "recommendation": recommendation,
        "scheduled": sched_norm,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }
    log_to_buffer(
        f"✓ Зараз: id={snapshot['state_id']} v={snapshot['state_version']} «{name}»"
    )
    if recommendation:
        log_to_buffer(f"✓ Рекомендація #{recommendation['id']}: {recommendation['name']}")
    else:
        log_to_buffer("✓ Рекомендації для цього стану немає")
    log_to_buffer(f"✓ Запланованих змін: {len(sched_norm)}")
    return snapshot


def comparable(snapshot: Dict) -> Dict:
    return {
        "state_id": snapshot.get("state_id"),
        "state_version": snapshot.get("state_version"),
        "name": snapshot.get("name"),
        "desc": snapshot.get("desc"),
        "recommendation": snapshot.get("recommendation"),
        "scheduled": snapshot.get("scheduled"),
    }


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))



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


def region_forms(name: str) -> tuple:
    """(називний, родовий, місцевий) — проста евристика для офіційного монітора."""
    n = (name or "").strip()
    if n.endswith("ська область"):
        base = n[: -len("ська область")]
        return n, f"{base}ської області", f"{base}ській області"
    if n.endswith("зька область"):
        base = n[: -len("зька область")]
        return n, f"{base}зької області", f"{base}зькій області"
    if n.endswith("цька область"):
        base = n[: -len("цька область")]
        return n, f"{base}цької області", f"{base}цькій області"
    if "Київ" in n and "област" not in n.lower():
        return n, "м. Києва", "м. Києві"
    return n, n, n


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


def format_message(prev: Dict, cur: Dict) -> str:
    """Повідомлення в стилі ukrenergo2 (офіційні формулювання)."""
    code = cur.get("state_id")
    try:
        code = int(code) if code is not None else None
    except (TypeError, ValueError):
        code = None

    name = cur.get("region_name") or REGION_NAME or ""
    _, region_gen, region_loc = region_forms(name)

    if code in STATUS_BY_CODE:
        emoji, title_tpl, body = STATUS_BY_CODE[code]
        title_line = f"{emoji} {title_tpl.format(region_gen=region_gen, region_loc=region_loc)}"
    else:
        title_line = f"⚡️ {cur.get('name') or 'Оновлення статусу'}"
        body = cur.get("desc") or "Стан енергосистеми оновлено."

    lines = [
        "Укренерго інформує -",
        f"<b>{esc(title_line)}</b>",
        "",
        body,
    ]

    # Рекомендація з API (якщо є і відрізняється)
    rec = cur.get("recommendation") or {}
    if rec.get("desc") and code not in STATUS_BY_CODE:
        lines.append("")
        lines.append(esc(rec["desc"]))

    lines.append("")
    footer = []
    footer.append("Джерело: НЕК «Укренерго»")
    if SUBSCRIBE:
        footer.append(f'<a href="{esc(SUBSCRIBE)}"><b>⚡️ ПІДПИСАТИСЯ</b></a>')
    lines.append(" | ".join(footer) if len(footer) > 1 else footer[0])

    return "\n".join(lines)


def main() -> None:
    log_to_buffer("=" * 40)
    log_to_buffer("🚀 СТАРТ УКРЕНЕРГО")
    try:
        current = build_snapshot()
        previous = load_state()
        if not previous:
            save_state(current)
            log_to_buffer("📋 Базовий стан збережено, сповіщення не шлю")
            return
        if comparable(previous) == comparable(current):
            current["checked_at"] = previous.get("checked_at", current["checked_at"])
            # оновлюємо лише час тихої перевірки не треба комітити щоразу —
            # лишаємо файл як є, щоб не було порожніх комітів
            log_to_buffer("✅ Стан регіону не змінився")
            return
        log_to_buffer(
            f"🔔 Зміна: {previous.get('state_id')}/{previous.get('name')} "
            f"→ {current.get('state_id')}/{current.get('name')}"
        )
        message = format_message(previous, current)
        image_path = get_status_image(current.get("state_id"))
        if image_path:
            log_to_buffer(f"🖼 Картинка: {image_path.name}")
        ok = send_notification(message, image_path=image_path)
        if not ok:
            log_to_buffer("❌ Не вдалось надіслати сповіщення, стан не перезаписую")
            return
        save_state(current)
        log_to_buffer("✅ Сповіщення надіслано, стан збережено")
    except Exception as e:
        log_to_buffer(f"❌ Критична помилка Укренерго: {e}")
    finally:
        send_log_to_channel("📊 ЛОГ ВИКОНАННЯ СКРИПТА · УКРЕНЕРГО")


if __name__ == "__main__":
    main()
