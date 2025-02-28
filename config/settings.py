import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv(dotenv_path='.env', override=True)

# Основные настройки
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Токен Telegram-бота
ADMIN_ID = 455284316  # ID администратора для уведомлений

# Пути к директориям
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMP_DIR = os.path.join(BASE_DIR, "temp")  # Директория для временных файлов
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")  # Директория для шаблонов

# Настройки файлов
PAYD_INPUT_FILE = os.path.join(TEMP_DIR, "исходник (Payd).csv")
COMPLETED_INPUT_FILE = os.path.join(TEMP_DIR, "исходник (Completed).csv")
JDT_OUTPUT_FILE = os.path.join(TEMP_DIR, "jdt.csv")
OJDT_OUTPUT_FILE = os.path.join(TEMP_DIR, "ojdt.csv")
