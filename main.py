import telebot
import os
from dotenv import load_dotenv
import pandas as pd
import signal
import sys
import time
import requests
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

# В начале файла добавим константу
ADMIN_ID = 455284316

# Создаем экземпляр бота
bot = telebot.TeleBot(TOKEN)

def signal_handler(sig, frame):
    print('\nЗавершение работы бота...')
    # Очищаем временные файлы при выходе
    if os.path.exists("temp/исходник (Payd).csv"):
        os.remove("temp/исходник (Payd).csv")
    sys.exit(0)

# Сначала идут все обработчики команд
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "Привет! Я бот для обработки файлов. Отправьте мне файл, и я обработаю его для вас.")

@bot.message_handler(commands=['help'])
def help_command(message):
    help_text = (
        "🤖 *Как пользоваться ботом:*\n\n"
        "1. Отправьте CSV файл с отчетом\n"
        "2. Бот автоматически определит тип файла (PAYD или COMPLETED)\n"
        "3. Создаст и отправит вам JDT и OJDT отчеты\n\n"
        "По всем вопросам обращайтесь к администратору"
    )
    bot.reply_to(message, help_text, parse_mode='Markdown')

@bot.message_handler(commands=['format'])
def format_command(message):
    format_text = (
        "📋 *Требования к формату файла:*\n\n"
        "*COMPLETED файл должен содержать колонки:*\n"
        "- Completed\n"
        "- Payment Provider\n"
        "- Total Fee EUR\n"
        "- Reseller Fee EUR\n"
        "- Net Fee EUR\n"
        "- Name\n"
        "- Order\n\n"
        "*PAYD файл должен содержать колонки:*\n"
        "- Paid\n"
        "- Payment Method\n"
        "- Total Fee EUR\n"
        "- Name\n"
        "- Order"
    )
    bot.reply_to(message, format_text, parse_mode='Markdown')

@bot.message_handler(commands=['info'])
def info_command(message):
    info_text = (
        "📊 <b>Структура проводок в SAP</b>\n\n"
        "<b>PAYD файл (простые платежи):</b>\n"
        "• Дебет = Кредит (Total Fee EUR)\n"
        "• Счета дебета: 141xxx (поступление)\n"
        "• Счета кредита: 210xxx (обязательства)\n\n"
        
        "<b>COMPLETED файл (с комиссиями):</b>\n"
        "1. Основная проводка:\n"
        "• Дебет: Total Fee EUR\n"
        "• Кредит1: Reseller Fee EUR (комиссия)\n"
        "• Кредит2: Net Fee EUR (чистая сумма)\n\n"
        
        "<b>Специальные счета:</b>\n"
        "• 207001 - Reseller Fee\n"
        "• 420001 - Net Fee (wire_transfer)\n"
        "• 420002 - Net Fee (CC/APM)\n"
        "• 420003 - Additional Fee\n\n"
        
        "<b>Пример COMPLETED:</b>\n"
        "Транзакция 100 EUR:\n"
        "• Дебет: 100 EUR (счет 210xxx)\n"
        "• Кредит1: 30 EUR (счет 207001) - комиссия\n"
        "• Кредит2: 70 EUR (счет 420xxx) - чистая сумма\n\n"
        
        "При наличии Additional Fee создается отдельная проводка:\n"
        "• Дебет: сумма (счет по провайдеру)\n"
        "• Кредит: сумма (счет 420003)"
    )
    bot.reply_to(message, info_text, parse_mode='HTML')

# Затем идет обработчик файлов
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
            f"📥 Получен файл типа: {report_type_msg}\n\n"
            f"⚙️ Формирую jdt и ojdt отчеты для {report_type_msg}..."
        )
        bot.send_message(message.chat.id, process_msg)

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

        # После успешной отправки отчетов оповещаем админа об успешном завершении
        success_msg = (
            f"✅ Отчет успешно сформирован и отправлен\n\n"
            f"Тип: {report_type_msg}\n"
            f"Для пользователя: @{message.from_user.username}" or "Неизвестный пользователь"
        )
        bot.send_message(ADMIN_ID, success_msg)

    except Exception as e:
        error_msg = (
            f"❌ Ошибка при формировании отчета\n\n"
            f"Пользователь: @{message.from_user.username} or 'Неизвестный пользователь'\n"
            f"Ошибка: {str(e)}"
        )
        bot.send_message(ADMIN_ID, error_msg)
        bot.reply_to(message, f"Ошибка: {str(e)}")

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

# В конце идет обработчик текстовых сообщений
@bot.message_handler(content_types=['text'])
def handle_text(message):
    # Пропускаем команды
    if message.text.startswith('/'):
        return
        
    try:
        forward_msg = (
            f"📩 Новое сообщение\n\n"
            f"От: @{message.from_user.username}" or "Неизвестный пользователь\n"
            f"ID: {message.from_user.id}\n"
            f"Текст: {message.text}"
        )
        bot.send_message(ADMIN_ID, forward_msg)
    except Exception as e:
        error_msg = (
            f"❌ Ошибка при пересылке сообщения\n\n"
            f"От: @{message.from_user.username} or 'Неизвестный пользователь'\n"
            f"Ошибка: {str(e)}"
        )
        bot.send_message(ADMIN_ID, error_msg)

def determine_report_type(df):
    """
    Определяет тип отчета и проверяет валидность файла
    """
    # Очищаем названия колонок от лишних пробелов
    df.columns = df.columns.str.strip()
    
    # Проверяем наличие колонки 'Reseller Fee EUR' или её вариантов
    reseller_fee_variants = [
        'Reseller Fee EUR',
        'Reseller\nFee EUR',
        'Reseller\r\nFee EUR',
        'ResellerFee EUR',
        'Reseller\nFee\nEUR'
    ]
    
    # Если есть хотя бы один вариант Reseller Fee - это completed
    if any(col in df.columns for col in reseller_fee_variants):
        return 'completed'
    
    # Если нет Reseller Fee - проверяем на payd
    if 'Paid' in df.columns:
        return 'payd'
    
    # Если не подходит ни один формат
    raise ValueError(
        "Неверный формат файла!\n\n"
        "Файл должен содержать следующие колонки:\n"
        "Для COMPLETED:\n"
        "- Completed\n"
        "- Payment Provider\n"
        "- Total Fee EUR\n"
        "- Reseller Fee EUR\n"
        "- Net Fee EUR\n"
        "- Name\n"
        "- Order\n\n"
        "Для PAYD:\n"
        "- Paid\n"
        "- Payment Method\n"
        "- Total Fee EUR\n"
        "- Name\n"
        "- Order"
    )

def check_internet():
    try:
        requests.get("https://api.telegram.org", timeout=5)
        return True
    except (RequestException, HTTPError):
        return False

def register_commands():
    """
    Регистрирует команды бота
    """
    commands = [
        telebot.types.BotCommand("start", "Запустить бота и получить инструкции"),
        telebot.types.BotCommand("help", "Показать справку по использованию"),
        telebot.types.BotCommand("format", "Информация о формате файлов"),
        telebot.types.BotCommand("info", "Информация о структуре проводок")
    ]
    
    try:
        bot.delete_my_commands()
        bot.set_my_commands(commands)
    except Exception as e:
        print(f"Ошибка при регистрации команд: {e}")

def run_bot():
    # Регистрируем команды при запуске
    register_commands()
    
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
