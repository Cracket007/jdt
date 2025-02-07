import telebot
import os
from dotenv import load_dotenv
import pandas as pd
import signal
import sys
import time
import requests
import urllib3
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError
from http.client import RemoteDisconnected

# Импортируем функции из наших модулей
from payd import process_jdt as process_payd_jdt
from payd import process_ojdt as process_payd_ojdt
from completed import process_jdt as process_completed_jdt
from completed import process_ojdt as process_completed_ojdt

# Загружаем токен из .env
load_dotenv(dotenv_path='.env', override=True)
TOKEN = os.getenv("BOT_TOKEN")

# Создаем экземпляр бота
bot = telebot.TeleBot(TOKEN)

def signal_handler(sig, frame):
    print('\nЗавершение работы бота...')
    # Очищаем временные файлы при выходе
    if os.path.exists("temp/исходник (Payd).csv"):
        os.remove("temp/исходник (Payd).csv")
    sys.exit(0)

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "Привет! Я бот для обработки файлов. Отправьте мне файл, и я обработаю его для вас.")

@bot.message_handler(content_types=['document'])
def handle_file(message):
    input_file = None  # Инициализируем переменную
    try:
        # Создаем временные директории
        os.makedirs("temp", exist_ok=True)
        os.makedirs("templates", exist_ok=True)

        # Проверяем расширение файла
        if not message.document.file_name.endswith('.csv'):
            bot.reply_to(message, "Пожалуйста, отправьте файл в формате CSV")
            return

        # Скачиваем файл
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        # Сначала сохраняем как временный файл
        with open("temp/исходник.csv", 'wb') as new_file:
            new_file.write(downloaded_file)

        # Теперь читаем для определения типа
        df = pd.read_csv('temp/исходник.csv')
        report_type = determine_report_type(df)
        
        # Формируем сообщение о процессе обработки
        report_type_msg = "COMPLETED" if report_type == 'completed' else "PAYD"
        process_msg = (
            f"📥 Получен файл типа: {report_type_msg}\n"
            f"⚙️ Формирую JDT и OJDT отчеты для {report_type_msg}...\n"
            f"📤 Отправляю готовые отчеты..."
        )
        bot.send_message(message.chat.id, process_msg)
        
        print(f"\n{'='*50}")
        print(f"Получен файл: {message.document.file_name}")
        print(f"Тип отчета: {report_type.upper()}")
        print(f"Количество строк: {len(df)}")
        print(f"{'='*50}\n")
        
        # Определяем имя входного файла
        input_file = "temp/исходник (Completed).csv" if report_type == 'completed' else "temp/исходник (Payd).csv"
        
        # Переименовываем файл
        os.rename("temp/исходник.csv", input_file)

        # Обрабатываем файлы в зависимости от типа
        if report_type == 'completed':
            process_completed_jdt('temp/jdt.csv')
            process_completed_ojdt('temp/ojdt.csv')
        else:
            process_payd_jdt('temp/jdt.csv')
            process_payd_ojdt('temp/ojdt.csv')

        # Отправляем файлы
        max_retries = 3
        retry_delay = 5  # секунд

        for attempt in range(max_retries):
            try:
                with open('temp/jdt.csv', 'rb') as jdt_file:
                    bot.send_document(message.chat.id, jdt_file)
                with open('temp/ojdt.csv', 'rb') as ojdt_file:
                    bot.send_document(message.chat.id, ojdt_file)
                break
            except (RemoteDisconnected, ConnectionError) as e:
                if attempt < max_retries - 1:  # Если это не последняя попытка
                    print(f"Ошибка отправки файла (попытка {attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                else:
                    raise  # Если все попытки исчерпаны, пробрасываем ошибку дальше

    except Exception as e:
        bot.reply_to(message, f"Ошибка: {str(e)}")
        print(f"Ошибка обработки файла: {str(e)}")

    finally:
        # Очищаем временные файлы
        files_to_remove = [
            "temp/исходник.csv",
            "temp/jdt.csv",
            "temp/ojdt.csv"
        ]
        if input_file:
            files_to_remove.append(input_file)
            
        for file in files_to_remove:
            if os.path.exists(file):
                os.remove(file)

def determine_report_type(df):
    """
    Определяет тип отчета на основе наличия характерных колонок
    """
    # Выводим список колонок для отладки
    print("\nКолонки входного файла:")
    for col in df.columns:
        print(f"- {col}")
    print()
    
    # Очищаем названия колонок от лишних пробелов
    df.columns = df.columns.str.strip()
    
    # Проверяем наличие колонки 'Reseller Fee EUR' с учетом возможных вариантов написания
    reseller_fee_variants = [
        'Reseller Fee EUR',
        'Reseller\nFee EUR',
        'Reseller\r\nFee EUR',
        'ResellerFee EUR',
        'Reseller\nFee\nEUR'
    ]
    
    # Проверяем наличие хотя бы одного варианта колонки
    has_reseller_fee = any(col in df.columns for col in reseller_fee_variants)
    
    if has_reseller_fee:
        print("Определен тип отчета: completed")
        return 'completed'
    else:
        print("Определен тип отчета: payd")
        return 'payd'

def check_internet():
    try:
        requests.get("https://api.telegram.org", timeout=5)
        return True
    except (RequestException, HTTPError):
        return False

def run_bot():
    while True:
        try:
            if not check_internet():
                print("Нет подключения к интернету. Ожидание подключения...")
                while not check_internet():
                    time.sleep(10)
                print("Подключение восстановлено!")

            print("Бот запущен (для завершения нажмите Ctrl+C)")
            bot.polling(none_stop=True, timeout=60, interval=3)
            
        except telebot.apihelper.ApiException as e:
            print(f"Ошибка API Telegram: {str(e)}")
            time.sleep(5)
            continue
            
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ConnectTimeout) as e:
            print(f"Ошибка сети: {str(e)}")
            print("Ожидание восстановления соединения...")
            time.sleep(10)
            continue
            
        except Exception as e:
            print(f"Неизвестная ошибка: {str(e)}")
            print("Перезапуск бота через 5 секунд...")
            time.sleep(5)
            continue

if __name__ == "__main__":
    # Устанавливаем обработчик Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    # Запускаем бота в бесконечном цикле
    run_bot()
