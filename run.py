import gspread
import schedule
import time
import requests
import logging
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
SHEET_NAME = 'Wialon'
API_URL = os.getenv("CM_API_URL")
CM_API_KEY = os.getenv("CM_API_KEY")

HEADERS = {
    'User-Agent': 'google sheets',
    'X-API-KEY': CM_API_KEY,
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

# Настройка логирования только в консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Подключение к Google Sheets
def get_sheet():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_FILE,
            scope
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        logger.info("Успешно подключено к Google Sheets")
        return spreadsheet. worksheet(SHEET_NAME)

    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {str(e)}")
        raise


# Основная функция для обновления данных
def update_sheet():

    logger.info(f"Начало обновления листа в {datetime.now()}")

    sheet = get_sheet()

    try:
        # ── Запрос к API ────────────────────────────────────────
        URL = f'{API_URL}report_generator/generate-report'
        payload = {
            "report_name": "axenta",
            "send_to_mail": False
        }

        logger.info(f"POST → {URL}")
        response = requests.post(
            URL,
            json=payload,
            headers=HEADERS,
            timeout=180               # 3 минуты — на всякий случай
        )
        response.raise_for_status()

        data_json = response.json()
        logger.info(f"Успешный ответ API, статус {response.status_code}")

        # ── Парсинг ответа ──────────────────────────────────────
        # Предполагаем, что приходит либо один объект, либо список
        if isinstance(data_json, list) and data_json:
            report = data_json[0]
        else:
            report = data_json

        if "json_result" not in report:
            logger.error("В ответе нет поля 'json_result'")
            return

        json_result = report["json_result"]

        if "headers" not in json_result or "field" not in json_result:
            logger.error("В json_result нет 'headers' и/или 'field'")
            return

        headers = json_result["headers"]
        field_data = json_result["field"]

        logger.info(f"Заголовков: {len(headers)}, строк данных: {len(field_data)}")

        if not field_data:
            logger.warning("Поле 'field' пустое → лист очищен")

            for attempt in range(3):
                try:
                    logger.info(f'Попытка очистки пустого листа #{attempt + 1}')
                    sheet.clear()
                    logger.info('Лист успешно очищен')
                    break

                except Exception as e:
                    logger.error(f'Ошибка при очистке пустого листа {e}')

                    if attempt < 2:
                        logger.info('Ждём 15 секунд перед повтором')
                        time.sleep(15)
                    else:
                        logger.error('Не удалось очистить пустой лист после 3-х попыток')
                        raise
            return

        # Подготовка данных
        data_to_write = [headers] + field_data

        logger.info(f"К записи: {len(data_to_write)} строк (вкл. заголовки)")

        # ── Обновление Google Sheets ────────────────────────────
        for attempt in range(3):
            try:
                logger.info(f'Попытка очистки листа #{attempt + 1}')
                sheet.clear()
                logger.info('Лист успешно очищен')
                break
            except Exception as e:
                logger.error(f'Ошибка при очистке листа {e}')

                if attempt < 2:
                    logger.info('Ждём 15 секунд перед повтором')
                    time.sleep(15)
                else:
                    logger.error('Не удалось очистить лист после 3-х попыток')
                    raise

        for attempt in range(3):
            try:
                logger.info(f'Попытка обновления листа #{attempt + 1}')
                sheet.update(
                    range_name='A1',
                    values=data_to_write
                )
                logger.info('Лист успешно обновлён')
                break
            except Exception as e:
                logger.error(f'Ошибка при обновлении листа {e}')

                if attempt < 2:
                    logger.info('Ждём 15 секунд перед повтором')
                    time.sleep(15)
                else:
                    logger.error('Не удалось обновить лист после 3-х попыток')
                    raise


        logger.info(f"Успешно записано {len(data_to_write)} строк")
        logger.info("Обновление завершено успешно")

    except requests.HTTPError as http_err:
        logger.error(f"HTTP {http_err.response.status_code}: {http_err.response.text}")
        raise
    except requests.RequestException as req_err:
        logger.error(f"Ошибка запроса: {str(req_err)}")
        raise
    except gspread.exceptions.GSpreadException as gs_err:
        logger.error(f"Google Sheets ошибка: {str(gs_err)}")
        raise
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {str(e)}")
        raise

# Планирование ежедневного выполнения
schedule.every().day.at("13:00").do(update_sheet)  # Выполнять в 13:00
schedule.every().day.at("18:00").do(update_sheet)  # Выполнять в 13:00

# Запуск цикла планировщика
if __name__ == "__main__":
    logger.info("Скрипт запущен")
    try:
        update_sheet()  # Выполнить один раз при запуске для теста
    except Exception as e:
        logger.error(f"Начальный тестовый запуск не удался: {str(e)}")
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f'Ошибка во время выполнения задачи по расписанию:{e}')
        time.sleep(60)