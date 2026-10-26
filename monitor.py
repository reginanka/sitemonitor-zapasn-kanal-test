import os
import json
import hashlib
import shutil
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from log_utils import log_to_buffer, send_log_to_channel
from site_content import fetch_site_update_and_screenshot
from telegram_handler import send_notification

API_BASE_URL = os.getenv("API_BASE_URL")
URL = os.environ.get('URL')
SUBSCRIBE = os.environ.get('SUBSCRIBE')

QUEUES = [(i, j) for i in range(1, 7) for j in range(1, 2 + 1)]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

CURRENT_FILE = DATA_DIR / "current.json"
PREVIOUS_FILE = DATA_DIR / "previous.json"
HASH_FILE = DATA_DIR / "last_hash.json"


def fetch_schedule(cherga_id: int, pidcherga_id: int) -> Tuple[List[Dict], bool]:
    """
    Тягне графік для однієї черги.
    Повертає (дані, is_error).
    """
    resp: Optional[requests.Response] = None
    try:
        params = {"cherga_id": cherga_id, "pidcherga_id": pidcherga_id}
        resp = requests.get(API_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        text = resp.text.strip()
        if text.startswith("[") and text.endswith("]"):
            data = json.loads(text)
        else:
            if text.startswith("{"):
                text = f"[{text}]"
            data = json.loads(text)

        if isinstance(data, list):
            return data, False

        log_to_buffer(f"⚠️ Відповідь не список для {cherga_id}.{pidcherga_id}")
        return [], False

    except Exception as e:
        body = resp.text[:200] if resp is not None else ""
        log_to_buffer(
            f"❌ Помилка {cherga_id}.{pidcherga_id}: {e}. "
            f"Фрагмент відповіді: {body}"
        )
        return [], True


def fetch_all_schedules() -> Tuple[Dict[str, List[Dict]], Dict[str, bool]]:
    """Повертає (дані, словник помилок)."""
    all_schedules: Dict[str, List[Dict]] = {}
    has_error: Dict[str, bool] = {}

    log_to_buffer("📡 Завантажую графіки по всіх чергах...")
    for cherga_id, pidcherga_id in QUEUES:
        queue_key = f"{cherga_id}.{pidcherga_id}"
        schedule, is_error = fetch_schedule(cherga_id, pidcherga_id)
        all_schedules[queue_key] = schedule
        has_error[queue_key] = is_error

        error_note = " [помилка API]" if is_error else ""
        log_to_buffer(f" ✓ {queue_key}: {len(schedule)} записів{error_note}")

    return all_schedules, has_error


def save_json(data, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(path: Path):
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def calculate_hash(obj) -> str:
    json_str = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


def normalize_record(rec: Dict, cherga_id: int, pidcherga_id: int) -> Dict:
    """Нормалізація одного запису."""
    date = rec.get("date", "")
    span = rec.get("span", "")
    color = rec.get("color", "").strip().lower()

    return {
        "cherga": cherga_id,
        "pidcherga": pidcherga_id,
        "queue_key": f"{cherga_id}.{pidcherga_id}",
        "date": date,
        "span": span,
        "color": color,
    }


def build_state(
    raw_schedules: Dict[str, List[Dict]],
    has_error: Dict[str, bool],
) -> Tuple[
    Dict[str, List[Dict]], # norm_by_queue
    Dict[str, str], # main_hashes
    Dict[str, Dict[str, Dict[str, str]]] # span_hashes[queue][date][span]
]:
    """
    Будує нормалізований стан з хешами по інтервалах.
    """
    norm_by_queue: Dict[str, List[Dict]] = {}
    main_hashes: Dict[str, str] = {}
    span_hashes: Dict[str, Dict[str, Dict[str, str]]] = {}

    for queue_key, schedule in raw_schedules.items():
        if has_error.get(queue_key, False):
            continue

        cherga_id, pidcherga_id = map(int, queue_key.split("."))
        norm_list: List[Dict] = []

        for rec in schedule:
            nrec = normalize_record(rec, cherga_id, pidcherga_id)
            norm_list.append(nrec)

        norm_list.sort(key=lambda r: (r["date"], r["span"]))
        norm_by_queue[queue_key] = norm_list

        # Головний хеш черги — від color кожного інтервалу
        main_hash_data = [{"date": r["date"], "span": r["span"], "color": r["color"]} for r in norm_list]
        main_hashes[queue_key] = calculate_hash(main_hash_data)

        # Хеші по кожному інтервалу
        sh: Dict[str, Dict[str, str]] = {}
        for rec in norm_list:
            d = rec["date"]
            span = rec["span"]
            if d not in sh:
                sh[d] = {}
            sh[d][span] = calculate_hash({"color": rec["color"]})
        
        span_hashes[queue_key] = sh

    return norm_by_queue, main_hashes, span_hashes


def load_last_state():
    """Завантажує хеші з last_hash.json + дані з previous.json"""
    hash_data = load_json(HASH_FILE)
    prev_norm = load_json(PREVIOUS_FILE)
    
    return {
        "timestamp": hash_data.get("timestamp"),
        "main_hashes": hash_data.get("main_hashes", {}),
        "span_hashes": hash_data.get("span_hashes", {}),
        "norm_by_queue": prev_norm,
    }


def save_state(
    main_hashes: Dict[str, str],
    span_hashes: Dict[str, Dict[str, Dict[str, str]]],
    timestamp: str
) -> None:
    """Зберігає тільки хеші в last_hash.json"""
    data = {
        "timestamp": timestamp,
        "main_hashes": main_hashes,
        "span_hashes": span_hashes,
    }
    save_json(data, HASH_FILE)


def parse_span(span: str) -> Tuple[str, str]:
    """0000-0030 або 00:00-00:30 -> (00:00, 00:30)"""
    if not span or "-" not in span:
        return ("", "")
    start, end = span.split("-")
    # Якщо вже є двокрапка, повертаємо як є
    if ":" in start:
        return start, end
    return f"{start[:2]}:{start[2:]}", f"{end[:2]}:{end[2:]}"


def group_spans(spans_changes: List[Dict]) -> List[Dict]:
    """Групує сусідні інтервали з однаковим типом зміни."""
    result: List[Dict] = []
    current: Optional[Dict] = None

    for item in sorted(spans_changes, key=lambda x: x["span"]):
        start_time, end_time = parse_span(item["span"])

        if not current:
            current = {
                "start": start_time,
                "end": end_time,
                "change": item["change"],
            }
        else:
            if current["change"] == item["change"] and current["end"] == start_time:
                current["end"] = end_time
            else:
                result.append(current)
                current = {
                    "start": start_time,
                    "end": end_time,
                    "change": item["change"],
                }

    if current:
        result.append(current)

    return result


def build_diff(
    norm_by_queue: Dict[str, List[Dict]],
    main_hashes: Dict[str, str],
    span_hashes: Dict[str, Dict[str, Dict[str, str]]],
    last_state: Dict,
) -> Dict:
    last_main = last_state.get("main_hashes", {})
    last_span = last_state.get("span_hashes", {})
    last_norm = last_state.get("norm_by_queue", {})

    diff = {
        "queues": [],
        "per_queue": {},
        "new_dates": [],  # Глобальний список нових дат
    }

    for queue_key, cur_main_hash in main_hashes.items():
        old_main_hash = last_main.get(queue_key)
        
        if old_main_hash is None:
            log_to_buffer(f"ℹ️ Перший запуск для {queue_key}, пропускаємо")
            continue
        
        if old_main_hash == cur_main_hash:
            continue

        # Є зміни — шукаємо деталі
        log_to_buffer(f"🔍 Аналізую зміни для {queue_key}")

        cur_sh = span_hashes.get(queue_key, {})
        old_sh = last_span.get(queue_key, {})
        
        new_dates = []
        if not old_sh:
            # Порожній old_sh = це "перший запуск з даними"
            new_dates = sorted(cur_sh.keys())  # Всі поточні дати = нові!
            log_to_buffer(f"{queue_key}: new data from empty state!")
        else:
            new_dates = sorted(d for d in cur_sh.keys() if d not in old_sh)
        
        if new_dates:
            log_to_buffer(f" 📅 Нові дати: {new_dates}")
            # Додаємо до глобального списку
            for nd in new_dates:
                if nd not in diff["new_dates"]:
                    diff["new_dates"].append(nd)
        
        changed_dates = {}
        cur_items = norm_by_queue.get(queue_key, [])
        old_items_list = last_norm.get(queue_key, [])

        for d in cur_sh.keys():
            if d in new_dates:
                continue
            
            # Порівнюємо хеші інтервалів для цієї дати
            cur_spans = cur_sh.get(d, {})
            old_spans = old_sh.get(d, {})
            
            changes_for_date = []
            
            for span, cur_span_hash in cur_spans.items():
                old_span_hash = old_spans.get(span)
                if old_span_hash == cur_span_hash:
                    continue
                
                # Хеш інтервалу змінився
                log_to_buffer(f" 🔄 Інтервал {span} дата {d}: хеш змінився")
                
                # Знаходимо старий і новий запис
                new_rec = next((r for r in cur_items if r["date"] == d and r["span"] == span), None)
                old_rec = next((r for r in old_items_list if r["date"] == d and r["span"] == span), None)
                
                if new_rec and old_rec:
                    log_to_buffer(f" Старий: color={old_rec['color']}, Новий: color={new_rec['color']}")
                    if new_rec["color"] != old_rec["color"]:
                        change = "added" if new_rec["color"] == "red" else "removed"
                        changes_for_date.append({"span": span, "change": change})
                        log_to_buffer(f" ✅ Зміна: {change}")
                else:
                    log_to_buffer(f" ⚠️ Не знайдено запис: new_rec={bool(new_rec)}, old_rec={bool(old_rec)}")

            if changes_for_date:
                grouped = group_spans(changes_for_date)
                changed_dates[d] = grouped
                log_to_buffer(f" ✅ Для дати {d} знайдено {len(changes_for_date)} змін")

        if new_dates or changed_dates:
            diff["queues"].append(queue_key)
            diff["per_queue"][queue_key] = {
                "new_dates": new_dates,
                "changed_dates": changed_dates,
            }
            log_to_buffer(f"✅ Додано {queue_key} до diff")
        else:
            log_to_buffer(f"⚠️ Хеш змінився для {queue_key}, але конкретні зміни не виявлені")

    return diff


def build_changes_notification(
    diff: Dict,
    url: str,
    subscribe: str,
    update_str: str
) -> str:
    """Повідомлення про зміни в ІСНУЮЧИХ датах"""
    
    # Беремо тільки черги що мають changed_dates
    queues_with_changes = []
    for q in sorted(diff["queues"]):
        info = diff["per_queue"].get(q, {})
        if info.get("changed_dates"):
            queues_with_changes.append(q)
    
    if not queues_with_changes:
        return ""
    
    queues = queues_with_changes
    
    parts = []
    parts.append(f"Для черг {', '.join(queues)} 🔔 ОНОВЛЕННЯ ГРАФІКА ВІДКЛЮЧЕНЬ!")
    parts.append("⬇️⬇️⬇️\n")
    
    # Дата оновлення
    update_date_str = ""
    if update_str:
        match = re.search(r'(\d{2}:\d{2})\s+(\d{2}\.\d{2})\.\d{4}', update_str)
        if match:
            update_date_str = f"🕐 {match.group(1)} {match.group(2)}"
    
    # Збираємо тільки дати зі ЗМІНАМИ (не нові)
    dates_with_changes = set()
    for q in queues:
        info = diff["per_queue"].get(q, {})
        for d in info.get("changed_dates", {}).keys():
            dates_with_changes.add(d)
    
    # Обробляємо кожну дату
    for date in sorted(dates_with_changes):
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            formatted_date = dt.strftime("%d.%m.%Y")
        except ValueError:
            formatted_date = date
        
        parts.append(f"🗓 {formatted_date}\n")
        
        for queue_key in sorted(queues, key=lambda x: tuple(map(int, x.split(".")))):
            queue_info = diff["per_queue"].get(queue_key, {})
            
            if date not in queue_info.get("changed_dates", {}):
                continue
            
            parts.append(f"▶️ Черга {queue_key}:")
            
            ranges = queue_info["changed_dates"][date]
            for r in ranges:
                start = r['start'].lstrip('0') or '0:00'
                end = r['end'].lstrip('0') or '0:00'
                if start.startswith(':'):
                    start = '0' + start
                if end.startswith(':'):
                    end = '0' + end
                if r["change"] == "added":
                    action = "🪫 додали відключення"
                    parts.append(f"{start}-{end} {action}")
                else:
                    action = "🔋 скасували відключення"
                    parts.append(f"<s>{start}-{end}</s> {action}")
            
            parts.append("")  # Порожній рядок після КОЖНОЇ черги
        
        parts.append("\n")
    
    # Посилання
    parts.append(
        f'<a href="{url}">🔗 Сайт "ЖОЕ"</a> | '
        f'<a href="{subscribe}">⚡️ ПІДПИСАТИСЯ</a>'
    )
    if update_date_str:
        parts.append(update_date_str)
    
    return "\n".join(parts)


def build_new_schedule_notification(
    diff: Dict,
    norm_by_queue: Dict[str, List[Dict]],
    url: str,
    subscribe: str,
    update_str: str
) -> str:
    """Компактне повідомлення про НОВИЙ графік"""

    # Беремо тільки черги що мають нові дати
    queues_with_new_dates = []
    for q in sorted(diff["queues"]):
        info = diff["per_queue"].get(q, {})
        if info.get("new_dates"):
            queues_with_new_dates.append(q)

    if not queues_with_new_dates:
        return ""

    parts = []
    parts.append("🔔 Додано новий графік!")
    parts.append("⬇️⬇️⬇️\n")

    # Дата оновлення
    update_date_str = ""
    if update_str:
        match = re.search(r'(\d{2}:\d{2})\s+(\d{2}\.\d{2})\.\d{4}', update_str)
        if match:
            update_date_str = f"🕐 {match.group(1)} {match.group(2)}"

    # Обробляємо тільки НОВІ дати
    for date in sorted(diff.get("new_dates", [])):
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            formatted_date = dt.strftime("%d.%m.%Y")
        except ValueError:
            formatted_date = date

        parts.append(f"🗓 {formatted_date}\n")

        # Отримуємо всі черги що мають відключення на цю дату
        for queue_key in sorted(
            queues_with_new_dates, key=lambda x: tuple(map(int, x.split(".")))
        ):
            records = norm_by_queue.get(queue_key, [])
            outages = [
                r for r in records
                if r["date"] == date and r["color"] == "red"
            ]

            if outages:
                grouped = group_spans(
                    [{"span": o["span"], "change": "added"} for o in outages]
                )

                # Форматуємо часи компактно
                time_ranges = []
                for g in grouped:
                    start = g["start"].lstrip("0") or "0:00"
                    end = g["end"].lstrip("0") or "0:00"
                    if start.startswith(":"):
                        start = "0" + start
                    if end.startswith(":"):
                        end = "0" + end
                    time_ranges.append(f"{start}-{end}")

                times_str = ", ".join(time_ranges)
                parts.append(f"Черга {queue_key}: \n🪫{times_str}")
                parts.append("")  # Порожній рядок після КОЖНОЇ черги

        parts.append("")  # Додатковий відступ після всіх черг дати

    # Посилання
    parts.append(
        f'<a href="{url}">🔗 Сайт "ЖОЕ"</a> | '
        f'<a href="{subscribe}">⚡️ ПІДПИСАТИСЯ </a>'
    )
    if update_date_str:
        parts.append(update_date_str)

    return "\n".join(parts)


def send_notification_safe(message: str, img_path=None) -> bool:
    """Надсилає повідомлення з перевіркою лімітів Telegram"""
    CAPTION_LIMIT = 1024  # Ліміт для caption з фото
    TEXT_LIMIT = 4096     # Ліміт для звичайного text повідомлення
    
    msg_len = len(message)
    log_to_buffer(f"📝 Довжина повідомлення: {msg_len} символів")
    
    # Якщо є фото і текст не влазить в caption
    if img_path and msg_len > CAPTION_LIMIT:
        log_to_buffer(f"⚠️ Текст {msg_len} > {CAPTION_LIMIT} (ліміт caption), надсилаю спочатку фото, потім текст")
        # Спочатку надсилаємо фото без тексту
        send_notification("📸", img_path)
        # Потім надсилаємо текст окремим повідомленням
        if msg_len > TEXT_LIMIT:
            log_to_buffer(f"⚠️ Текст {msg_len} > {TEXT_LIMIT}, обрізаю")
            message = message[:TEXT_LIMIT-100] + "\n\n... (текст скорочено)"
        return send_notification(message, None)
    
    # Якщо немає фото, але текст завеликий для text повідомлення
    if not img_path and msg_len > TEXT_LIMIT:
        log_to_buffer(f"⚠️ Текст {msg_len} > {TEXT_LIMIT}, обрізаю")
        message = message[:TEXT_LIMIT-100] + "\n\n... (текст скорочено)"
    
    return send_notification(message, img_path)



def normalize_date_key(date_str: str) -> str:
    """YYYY-MM-DD / DD.MM.YYYY / DD.MM.YY → YYYY-MM-DD для порівняння."""
    s = (date_str or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def dates_equal(a: str, b: str) -> bool:
    na, nb = normalize_date_key(a), normalize_date_key(b)
    return bool(na) and na == nb


def records_to_slots_for_date(records: List[Dict], date: str) -> List[Dict]:
    """Червоні інтервали за дату → [{from, to, type}] (сусідні злиті через group_spans).

    date у current.json часто як DD.MM.YYYY, а today у sync — YYYY-MM-DD.
    Порівнюємо через normalize_date_key.
    """
    outages = [
        r for r in (records or [])
        if dates_equal(r.get("date") or "", date)
        and (r.get("color") or "").strip().lower() == "red"
    ]
    if not outages:
        return []
    grouped = group_spans(
        [{"span": o["span"], "change": "added"} for o in outages]
    )
    slots: List[Dict] = []
    for g in grouped:
        start = g.get("start") or ""
        end = g.get("end") or ""
        if start and end:
            slots.append({"from": start, "to": end, "type": "schedule"})
    return slots


def _queue_parts(queue_key: str) -> Tuple[str, str]:
    parts = (queue_key or "").split(".")
    if len(parts) != 2:
        return (queue_key or "").strip(), ""
    return parts[0].strip(), parts[1].strip()


def addr_matches_queue(addr: dict, queue_key: str) -> bool:
    """Чи адреса користувача належить до черги 4.2 / queue=4 subqueue=2."""
    q = (addr.get("queue") or "").strip()
    sq = (addr.get("subqueue") or "").strip()
    if not q:
        return False
    ch, pid = _queue_parts(queue_key)
    variants = {
        queue_key,
        f"{ch}.{pid}" if pid else ch,
        f"{ch}|{pid}" if pid else ch,
        ch,
    }
    if q in variants:
        if not sq or not pid:
            return True
        return sq == pid or sq == queue_key
    if sq and f"{q}.{sq}" == queue_key:
        return True
    if sq and f"{q}|{sq}" == queue_key:
        return True
    return False


def sync_schedules_to_mongo(
    norm_by_queue: Dict[str, List[Dict]],
    queue_keys: Optional[List[str]] = None,
) -> None:
    """
    Пише слоти у Mongo (queue_schedules) лише для вказаних черг.
    Викликати ТІЛЬКИ коли є зміни в графіку.
    """
    mongo_uri = os.getenv("MAYACHOK_MONGO_URI") or os.getenv("MONGO_URI")
    if not mongo_uri:
        log_to_buffer("⚠️ MONGO_URI / MAYACHOK_MONGO_URI не задано — пропуск запису графіків у БД")
        return

    try:
        from mongo_store import upsert_queue_schedule
    except Exception as e:
        log_to_buffer(f"⚠️ Не вдалося імпортувати mongo_store: {e}")
        return

    keys = list(queue_keys) if queue_keys is not None else list(norm_by_queue.keys())
    if not keys:
        log_to_buffer("ℹ️ Немає черг для запису в Mongo")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


    saved = 0
    errors = 0

    for queue_key in sorted(keys):
        records = norm_by_queue.get(queue_key) or []
        try:
            cherga, pidcherga = _queue_parts(queue_key)
            if not cherga:
                continue

            slots_today = records_to_slots_for_date(records, today)
            slots_tomorrow = records_to_slots_for_date(records, tomorrow)
            text_parts = [f"{s['from']}–{s['to']}" for s in slots_today]
            text = ", ".join(text_parts) if text_parts else "немає відключень"
            extra = {
                "slots_tomorrow": slots_tomorrow,
                "queue_key": queue_key,
                "source": "monitor.py",
            }

            # Ключ 4|2 і alias 4.2 — бот шукає обидва
            upsert_queue_schedule(
                cherga, pidcherga,
                slots=slots_today, events=slots_today,
                text=text, date=today, known=True, extra=extra,
            )
            upsert_queue_schedule(
                queue_key, "",
                slots=slots_today, events=slots_today,
                text=text, date=today, known=True, extra=extra,
            )
            saved += 1
        except Exception as e:
            errors += 1
            log_to_buffer(f"⚠️ Mongo queue {queue_key}: {e}")

    log_to_buffer(
        f"🗄 Mongo: збережено {saved} черг (зі змінами), помилок={errors}"
    )


def _extract_changes_for_dm(diff: Dict) -> Dict[str, List[dict]]:
    """
    З diff['per_queue'] збирає для DM:
      { "6.1": [ {"change": "added"|"removed", "start": "12:00", "end": "13:00"}, ... ] }
    Бере змінені дати (не new_dates), усі діапазони.
    """
    out: Dict[str, List[dict]] = {}
    for q, info in (diff.get("per_queue") or {}).items():
        items = []
        for date, ranges in (info.get("changed_dates") or {}).items():
            for r in ranges:
                items.append({
                    "change": r.get("change") or "added",
                    "start": r.get("start") or "",
                    "end": r.get("end") or "",
                    "date": date,
                })
        # нові дати — з реальною датою (для header і розкладу на завтра)
        for nd in info.get("new_dates") or []:
            items.append({
                "change": "new",
                "start": "",
                "end": "",
                "date": nd,
            })
        if items:
            out[q] = items
    return out


def notify_bot_users(
    changed_queues: List[str],
    channel_message: str,
    diff: Optional[Dict] = None,
) -> None:
    """
    Сповіщення підписникам бота ztoe (users.notify=true),
    у яких адреса з чергою з changed_queues.

    Новий короткий формат + один скрін ЖОЕ.
    """
    try:
        from bot_notify import notify_bot_users_with_images
        from datetime import datetime as _dt

        header = "🔔 Оновлення графіка відключень"
        msg = (channel_message or "").strip()
        if msg.startswith("🔔"):
            header = msg.split("\n")[0].strip()

        changes = _extract_changes_for_dm(diff or {})
        log_to_buffer(
            f"📬 DM prepare: queues={changed_queues} "
            f"changes_keys={list(changes.keys())} "
            f"has_ztoe_token={bool(os.getenv('ZTOE_BOT_TOKEN'))}"
        )

        notify_bot_users_with_images(
            msg,
            queue_keys=changed_queues or [],
            all_user_queues=False,
            header=header,
            date_str=_dt.now().strftime("%d.%m.%Y"),
            personalize=True,
            changes=changes,
        )
    except Exception as e:
        log_to_buffer(f"❌ notify_bot_users: {e}")



def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_to_buffer("=" * 60)
    log_to_buffer(f"🚀 СТАРТ [{timestamp}]")
    log_to_buffer("=" * 60)

    try:
        # 1. Завантажити графіки з API
        current_schedules, has_error = fetch_all_schedules()
        if not current_schedules:
            log_to_buffer("❌ Не вдалось завантажити жоден графік")
            return

        # 2. Побудувати поточний стан
        norm_by_queue, current_main_hashes, current_span_hashes = build_state(
            current_schedules, has_error
        )
        log_to_buffer(f"🔐 Витягнено хеші для {len(current_main_hashes)} черг")

        if not current_main_hashes:
            log_to_buffer(
                "❌ Усі черги з помилкою API — current.json і last_hash.json не чіпаю"
            )
            return

        # 3. Зберегти поточні нормалізовані дані (локально для diff)
        if CURRENT_FILE.exists():
            shutil.copy(CURRENT_FILE, PREVIOUS_FILE)
            log_to_buffer("📋 Попередній current.json скопійовано в previous.json")

        save_json(norm_by_queue, CURRENT_FILE)
        log_to_buffer("💾 Нормалізовані дані збережено в data/current.json")

        # 4. Завантажити попередній стан
        last_state = load_last_state()
        log_to_buffer("📋 Завантажено попередній стан")

        # 5. Побудувати diff
        diff = build_diff(norm_by_queue, current_main_hashes, current_span_hashes, last_state)

        if not diff["queues"] and not diff["new_dates"]:
            log_to_buffer("✅ Дані по всіх чергах не змінилися — Mongo/DM не чіпаємо")
            save_state(current_main_hashes, current_span_hashes, timestamp)
            return

        log_to_buffer(f"🔔 Зміни виявлено для: {', '.join(diff['queues'])}")


        # 6+7. Дата + скрін одним проходом Chromium (тільки при змінах)
        date_content, screenshot_path, screenshot_hash = fetch_site_update_and_screenshot()
        if not screenshot_path:
            log_to_buffer("⚠️ Не вдалося створити скріншот")

        from pathlib import Path as _Path
        img_path = _Path(screenshot_path) if screenshot_path else None

        # 7b. Зберегти скрін у Mongo (для /check у боті)
        if img_path and img_path.exists():
            try:
                from mongo_store import save_site_screenshot
                png_bytes = img_path.read_bytes()
                fid = save_site_screenshot(png_bytes, source_hash=screenshot_hash or "")
                log_to_buffer(f"🗄 Скрін ЖОЕ збережено в Mongo ({len(png_bytes)} bytes, id={fid})")
            except Exception as e:
                log_to_buffer(f"⚠️ Не вдалося зберегти скрін у Mongo: {e}")

        # 8. Визначаємо типи змін
        has_new_dates = bool(diff.get("new_dates"))
        has_changes = any(
            q_info.get("changed_dates") 
            for q_info in diff["per_queue"].values()
        )

        # 9. Канал + збір тексту для DM бота (лише при змінах)
        dm_text = ""

        # Випадок 1: Є ТІЛЬКИ зміни (без нових дат)
        if has_changes and not has_new_dates:
            log_to_buffer("📤 Надсилаю повідомлення про зміни + фото")
            changes_msg = build_changes_notification(
                diff, URL, SUBSCRIBE, date_content or ""
            )
            if changes_msg:
                dm_text = changes_msg
                ok = send_notification_safe(changes_msg, img_path)
                if ok:
                    log_to_buffer("✅ Повідомлення про зміни відправлено в канал")
                else:
                    log_to_buffer("❌ Помилка надсилання повідомлення про зміни")
            else:
                log_to_buffer("⚠️ Немає черг зі змінами для відправки")

        # Випадок 2: Є ТІЛЬКИ новий графік
        elif has_new_dates and not has_changes:
            log_to_buffer("📤 Надсилаю повідомлення про новий графік + фото")
            new_msg = build_new_schedule_notification(
                diff, norm_by_queue, URL, SUBSCRIBE, date_content or ""
            )
            if new_msg:
                dm_text = new_msg
                ok = send_notification_safe(new_msg, img_path)
                if ok:
                    log_to_buffer("✅ Повідомлення про новий графік відправлено в канал")
                else:
                    log_to_buffer("❌ Помилка надсилання повідомлення про новий графік")
            else:
                log_to_buffer("⚠️ Немає черг з новими датами для відправки")

        # Випадок 3: Є І зміни, І новий графік
        elif has_changes and has_new_dates:
            log_to_buffer("📤 Надсилаю повідомлення про зміни + фото")
            changes_msg = build_changes_notification(
                diff, URL, SUBSCRIBE, date_content or ""
            )
            if changes_msg:
                dm_text = changes_msg
                ok1 = send_notification_safe(changes_msg, img_path)
                if ok1:
                    log_to_buffer("✅ Повідомлення про зміни відправлено в канал")
                else:
                    log_to_buffer("❌ Помилка надсилання повідомлення про зміни")

            log_to_buffer("📤 Надсилаю повідомлення про новий графік (без фото)")
            new_msg = build_new_schedule_notification(
                diff, norm_by_queue, URL, SUBSCRIBE, date_content or ""
            )
            if new_msg:
                dm_text = (dm_text + "\n\n" + new_msg) if dm_text else new_msg
                ok2 = send_notification_safe(new_msg, None)
                if ok2:
                    log_to_buffer("✅ Повідомлення про новий графік відправлено в канал")
                else:
                    log_to_buffer("❌ Помилка надсилання повідомлення про новий графік")

        # 9b. Тільки при змінах: Mongo (слоти) і DM підписникам бота
        changed_queues = list(diff.get("queues") or [])
        try:
            sync_schedules_to_mongo(norm_by_queue, queue_keys=changed_queues)
        except Exception as e:
            log_to_buffer(f"❌ sync_schedules_to_mongo: {e}")

        try:
            notify_bot_users(
                changed_queues,
                dm_text or "🔔 Оновлення графіка відключень",
                diff=diff,
            )
        except Exception as e:
            log_to_buffer(f"❌ notify_bot_users: {e}")

        # 10. Оновити тільки хеші
        save_state(current_main_hashes, current_span_hashes, timestamp)
        log_to_buffer("💾 Хеші оновлено в data/last_hash.json")

    except Exception as e:
        log_to_buffer(f"❌ Критична помилка: {e}")
    finally:
        send_log_to_channel()
        log_to_buffer("🏁 Завершення роботи скрипта")


if __name__ == "__main__":
    main()
