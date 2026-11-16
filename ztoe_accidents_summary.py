import base64
import json
import os
from datetime import datetime, timezone
import requests

# Налаштування MongoDB (приблизна структура, адаптуйте під ваше існуюче підключення)
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# client = MongoClient(MONGO_URI)
# db = client["ztoe_db"]
# accidents_collection = db["accidents"]

def push_outages_to_github(outages_data: dict, summary_data: dict = None, raions_data: list = None) -> None:
    """
    Оновлює файл docs/data/outages.json у репозиторії zh-map через GitHub Contents API.
    Працює незалежно від того, критичний рівень чи ні — синхронізує стан з мапою.
    """
    repo = os.getenv("MAP_GITHUB_REPO", "reginanka/zh-map").strip()
    token = os.getenv("MAP_GITHUB_TOKEN", "").strip()
    path = os.getenv("MAP_GITHUB_PATH", "docs/data/outages.json").strip()
    
    if not repo or not token:
        print("⚠️ MAP_GITHUB_REPO або MAP_GITHUB_TOKEN не налаштовані в середовищі. Оновлення карти на GitHub пропущено.")
        return

    # Формуємо структуру згідно з вимогами zh-map
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": outages_data
    }
    if summary_data:
        payload["summary"] = summary_data
    if raions_data:
        payload["raions"] = raions_data

    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        # 1. Отримуємо актуальний sha існуючого файлу на GitHub (вимагається GitHub API для оновлення)
        res = requests.get(url, headers=headers, timeout=15)
        sha = res.json().get("sha") if res.status_code == 200 else None

        # 2. Кодуємо JSON у base64
        json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        encoded_content = base64.b64encode(json_bytes).decode("ascii")

        body = {
            "message": f"chore(data): автоматичне оновлення карти від бота ({datetime.now(timezone.utc).strftime('%H:%M %d.%m')})",
            "content": encoded_content
        }
        if sha:
            body["sha"] = sha

        # 3. Надсилаємо PUT запит на GitHub
        put_res = requests.put(url, headers=headers, json=body, timeout=20)
        put_res.raise_for_status()
        print("✅ Дані відключень успішно оновлені на GitHub Pages (zh-map)!")
    except Exception as e:
        print(f"❌ Помилка оновлення карти через GitHub API: {e}")

def main():
    print("🚀 Запуск скрипта збору аварій та оновлення...")
    
    # --- 1. Ваша існуюча логіка збору даних з обленерго ---
    # raw_accidents = fetch_ztoe_accidents()

    # --- 2. Ваша існуюча логіка збереження в MongoDB ---
    # accidents_collection.insert_many(raw_accidents)
    # print("💾 Дані успішно збережено в MongoDB.")

    # --- 3. Приклад підготовки даних для мапи zh-map ---
    current_outages_data = {
        "житомир": 10.0,
        "бердичів": 63.1,
        "коростень": 0.0,
        "коростишів": 32.5,
        "звягель": 0.0
    }

    current_summary = {
        "off_percent": 15.2,
        "off_streets": 180,
        "total_streets": 15000,
        "off_settlements": 34
    }

    current_raions = [
        {
            "name": "Житомирський",
            "off_percent": 18.4,
            "off_streets": 65,
            "off_settlements": 14,
            "off_settlements_percent": 14.0
        },
        {
            "name": "Бердичівський",
            "off_percent": 63.1,
            "off_streets": 45,
            "off_settlements": 8,
            "off_settlements_percent": 30.0
        }
    ]

    # --- 4. Оновлення карти на GitHub (працює завжди при кожному запуску крона) ---
    push_outages_to_github(
        outages_data=current_outages_data,
        summary_data=current_summary,
        raions_data=current_raions
    )

if __name__ == "__main__":
    main()
