import os
import hashlib
from io import BytesIO
from typing import Tuple, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from PIL import Image
from log_utils import log_to_buffer

URL = os.getenv("URL")


def fetch_site_update_and_screenshot() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Один запуск Chromium:
      - дата оновлення (текст)
      - скрін між 'Дата оновлення інформації' і 'робіт'
    Повертає: (update_date, screenshot_path, screenshot_hash)
    """
    if not URL:
        log_to_buffer("❌ URL не задано")
        return None, None, None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1920, "height": 3080})
            page.goto(URL, wait_until="domcontentloaded", timeout=25000)
            page.wait_for_timeout(1500)

            page_content = page.content()
            soup = BeautifulSoup(page_content, "html.parser")
            for br in soup.find_all("br"):
                br.replace_with("\n")

            update_date = None
            for elem in soup.find_all(["div", "span", "p", "h2", "h3", "h4", "h5"]):
                text = elem.get_text(strip=False)
                if "Дата" in text and update_date is None:
                    lines = [line.strip() for line in text.split("\n") if line.strip()]
                    update_date = "\n".join(lines)
                    log_to_buffer(f"✅ Знайдено дату оновлення: {update_date}")
                    break

            if not update_date:
                log_to_buffer("⚠️ Дата оновлення не знайдена")

            screenshot_path = None
            screenshot_hash = None
            try:
                log_to_buffer("📸 Створюю скріншот...")
                date_element = page.locator("text=/Дата оновлення інформації/").first
                end_element = page.locator("text=/робіт/").last

                if date_element.count() == 0:
                    log_to_buffer("❌ Не знайдено 'Дата оновлення інформації'")
                else:
                    date_box = date_element.bounding_box()
                    end_box = end_element.bounding_box() if end_element.count() > 0 else None

                    if date_box:
                        x = 0
                        width = 1920
                        start_y = date_box["y"] + date_box["height"]
                        full_screenshot = page.screenshot()
                        image = Image.open(BytesIO(full_screenshot))

                        if end_box:
                            end_y = end_box["y"] + end_box["height"] + 5
                            log_to_buffer(f"📐 Обрізка до 'робіт': y={start_y}-{end_y}")
                        else:
                            end_y = image.height
                            log_to_buffer("📐 Обрізка на всю висоту (робіт не знайдено)")

                        height = end_y - start_y
                        if height > 0:
                            cropped = image.crop((x, start_y, x + width, end_y))
                            screenshot_path = "screenshot.png"
                            cropped.save(screenshot_path)
                            screenshot_hash = hashlib.md5(cropped.tobytes()).hexdigest()
                            log_to_buffer(f"✅ Скріншот створено. Хеш: {screenshot_hash}")
                        else:
                            log_to_buffer("❌ Некоректна висота області скріна")
            except Exception as e:
                log_to_buffer(f"⚠️ Помилка скріншота: {e}")

            browser.close()
            return update_date, screenshot_path, screenshot_hash

    except Exception as e:
        log_to_buffer(f"❌ Помилка Playwright: {e}")
        return None, None, None
