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

# Загружаем токен из .env
load_dotenv(dotenv_path='.env', override=True)
TOKEN = os.getenv("BOT_TOKEN")

# Создаем экземпляр бота
bot = telebot.TeleBot(TOKEN)

# Словари для маппинга провайдеров на их номера
DEBIT_MAPPING = {
    'astropay': '141009',
    'Eupago': '141011',
    'Fibo PNC': '141002',
    'Intergiro': '141012',
    'Paysafe': '141007',
    'PNC': '141002',
    'PNC VISA': '141002',
    'RPD': '141013',
    'TRU': '141006',
    'Truevo PayOn': '141006',
    'volt': '141005',
    'voltx': '141005',
    'wire_transfer': '141001'
}

CREDIT_MAPPING = {
    'astropay': '210009',
    'Eupago': '210011',
    'Fibo PNC': '210002',
    'Intergiro': '210012',
    'Paysafe': '210007',
    'PNC': '210002',
    'PNC VISA': '210002',
    'RPD': '210013',
    'TRU': '210006',
    'Truevo PayOn': '210006',
    'volt': '210005',
    'voltx': '210005',
    'wire_transfer': '210001'
}

def signal_handler(sig, frame):
    print('\nЗавершение работы бота...')
    # Очищаем временные файлы при выходе
    if os.path.exists("temp/исходник (Payd).csv"):
        os.remove("temp/исходник (Payd).csv")
    sys.exit(0)

# Обработчик команды /start
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "Привет! Я бот для обработки файлов. Отправьте мне файл, и я обработаю его для вас.")

# Обработчик файлов
@bot.message_handler(content_types=['document'])
def handle_file(message):
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
        
        with open("temp/исходник (Payd).csv", 'wb') as new_file:
            new_file.write(downloaded_file)

        # Обрабатываем файлы и сохраняем в новые файлы
        process_jdt('temp/jdt.csv')
        process_ojdt('temp/ojdt.csv')

        # Отправляем оба файла
        with open('temp/jdt.csv', 'rb') as jdt_file:
            bot.send_document(message.chat.id, jdt_file)
        with open('temp/ojdt.csv', 'rb') as ojdt_file:
            bot.send_document(message.chat.id, ojdt_file)

    except Exception as e:
        bot.reply_to(message, f"Ошибка: {str(e)}")

    finally:
        # Очищаем временные файлы
        files_to_remove = [
            "temp/исходник (Payd).csv",
            "temp/jdt.csv",
            "temp/ojdt.csv"
        ]
        for file in files_to_remove:
            if os.path.exists(file):
                os.remove(file)

def process_jdt(output_file):
    # Читаем исходный файл
    df = pd.read_csv('temp/исходник (Payd).csv')
    
    # Читаем шаблон и сохраняем только первую строку
    template_df = pd.read_csv('templates/jdt_template.csv', nrows=1)
    
    print(f"Количество колонок в jdt: {len(template_df.columns)}")
    
    # Создаем пустой список для строк результата
    debit_rows = []  # Отдельный список для debit строк
    credit_rows = [] # Отдельный список для credit строк
    
    # Создаем debit строки с их нумерацией
    for index, row in df.iterrows():
        # Используем номер из DEBIT_MAPPING
        provider_number = DEBIT_MAPPING.get(row['Payment Provider'], row['Payment Provider'])
        
        debit_row = {
            'ParentKey': index + 1,
            'JdtNum': index + 1,
            'LineNum': '',
            'Debit': row['Fval EUR'],
            'Credit': '',
            'DueDate': row['Paid'],
            'ShortName': provider_number,  # Номер для debit
            'Reference1': row['Name'],
            'Reference2': row['Order '],
            'TaxDate': row['Paid'],
            'ReferenceDate1': row['Paid']
        }
        debit_rows.append(debit_row)
    
    # Создаем credit строки с отдельной нумерацией
    for index, row in df.iterrows():
        # Используем номер из CREDIT_MAPPING
        provider_number = CREDIT_MAPPING.get(row['Payment Provider'], row['Payment Provider'])
        
        credit_row = {
            'ParentKey': index + 1,  # Начинаем нумерацию заново с 1
            'JdtNum': index + 1,     # Оставляем как есть
            'LineNum': '1',
            'Debit': '',
            'Credit': row['Fval EUR'],
            'DueDate': row['Paid'],
            'ShortName': provider_number,  # Номер для credit
            'Reference1': row['Name'],
            'Reference2': row['Order '],
            'TaxDate': row['Paid'],
            'ReferenceDate1': row['Paid']
        }
        credit_rows.append(credit_row)
    
    # Объединяем списки: сначала все debit, потом все credit
    result_rows = debit_rows + credit_rows
    
    # Создаем DataFrame из результата
    result_df = pd.DataFrame(result_rows)
    
    # Создаем пустой DataFrame с колонками из шаблона
    final_df = pd.DataFrame(columns=template_df.columns)
    
    # Копируем данные
    for col in result_df.columns:
        if col in final_df.columns:
            final_df[col] = result_df[col]
    
    # Объединяем заголовки и данные
    final_df = pd.concat([template_df, final_df], ignore_index=True)
    
    # Сохраняем результат
    final_df.to_csv(output_file, index=False)

def process_ojdt(output_file):
    # Читаем исходный файл
    df = pd.read_csv('temp/исходник (Payd).csv')
    
    # Читаем шаблон и сохраняем только первую строку
    template_df = pd.read_csv('templates/ojdt_template.csv', nrows=1)
    
    print(f"Количество колонок в ojdt: {len(template_df.columns)}")
    
    # Создаем список для строк результата
    result_rows = []
    
    # Проходим по всем строкам исходного файла
    for index, row in df.iterrows():
        ojdt_row = {
            'JdtNum': index + 1,  # Используем JdtNum вместо JDT_NUM
            'ReferenceDate': row['Paid'],
            'Reference': row['Name'],
            'Reference2': row['Order '],
            'TaxDate': row['Paid'],
            'DueDate': row['Paid']
        }
        result_rows.append(ojdt_row)
    

    # Создаем DataFrame с данными
    data_df = pd.DataFrame(result_rows, columns=template_df.columns)
    
    # Объединяем заголовки и данные
    final_df = pd.concat([template_df, data_df], ignore_index=True)
    
    # Сохраняем результат
    final_df.to_csv(output_file, index=False)

def check_internet():
    try:
        # Пробуем подключиться к надежному сервису
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
                    time.sleep(10)  # Проверяем каждые 10 секунд
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
    # Импортируем time для задержки перед перезапуском
    import time
    
    # Устанавливаем обработчик Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    # Запускаем бота в бесконечном цикле
    run_bot()
