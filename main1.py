import os
import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.types import BotCommand
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import F
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from dotenv import load_dotenv
import asyncio

# Импортируем функции из ваших модулей
from payd import process_jdt as process_payd_jdt
from payd import process_ojdt as process_payd_ojdt
from completed import process_jdt as process_completed_jdt
from completed import process_ojdt as process_completed_ojdt

# Загружаем токен из .env
load_dotenv(dotenv_path='.env', override=True)
TOKEN = os.getenv("BOT_TOKEN")

# Константа для ID администратора
ADMIN_ID = 455284316

# Создаем экземпляр бота и диспетчера
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Функция для определения типа отчета
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

# Команда /start
@dp.message(F.text == "/start")
async def start_command(message: types.Message):
    await message.answer("Привет! Я бот для обработки файлов. Отправьте мне файл, и я обработаю его для вас.")

# Команда /help
@dp.message(F.text == "/help")
async def help_command(message: types.Message):
    help_text = (
        "🤖 *Как пользоваться ботом:*\n\n"
        "1. Отправьте CSV файл с отчетом\n"
        "2. Бот автоматически определит тип файла (PAYD или COMPLETED)\n"
        "3. Создаст и отправит вам JDT и OJDT отчеты\n\n"
        "По всем вопросам обращайтесь к администратору"
    )
    await message.answer(help_text, parse_mode='Markdown')

# Команда /format
@dp.message(F.text == "/format")
async def format_command(message: types.Message):
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
    await message.answer(format_text, parse_mode='Markdown')

# Обработчик файлов
@dp.message(F.document)
async def handle_file(message: types.Message):
    input_file = None  # Инициализируем переменную
    try:
        # Создаем временные директории
        os.makedirs("temp", exist_ok=True)
        os.makedirs("templates", exist_ok=True)

        # Проверяем расширение файла
        if not message.document.file_name.endswith('.csv'):
            await message.reply("Пожалуйста, отправьте файл в формате CSV")
            return

        # Скачиваем файл
        file = await bot.download(message.document)

        # Сохраняем файл
        with open("temp/исходник.csv", "wb") as f:
            f.write(file.read())  # Извлекаем байты из объекта BytesIO

        # Теперь читаем для определения типа
        df = pd.read_csv('temp/исходник.csv')
        report_type = determine_report_type(df)

        # Формируем сообщение о процессе обработки
        report_type_msg = "COMPLETED" if report_type == 'completed' else "PAYD"
        process_msg = (
            f"📥 Получен файл типа: {report_type_msg}\n\n"
            f"⚙️ Формирую jdt и ojdt отчеты для {report_type_msg}..."
        )
        await message.answer(process_msg)

        # Определяем имя входного файла
        input_file = "temp/исходник (Completed).csv" if report_type == 'completed' else "temp/исходник (Payd).csv"

        # Переименовываем файл
        os.rename("temp/исходник.csv", input_file)

        # Обрабатываем файлы в зависимости от типа
        # Обрабатываем файлы в зависимости от типа
        if report_type == 'completed':
            process_completed_jdt('temp/jdt.csv')  # Передаем путь к выходному файлу
            process_completed_ojdt('temp/ojdt.csv')  # Передаем путь к выходному файлу
        else:
            process_payd_jdt('temp/jdt.csv')  # Передаем путь к выходному файлу
            process_payd_ojdt('temp/ojdt.csv')  # Передаем путь к выходному файлу

        # Отправляем файлы
        await message.answer_document(types.FSInputFile("temp/jdt.csv"))
        await message.answer_document(types.FSInputFile("temp/ojdt.csv"))

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")

    finally:
        # Удаляем временные файлы
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

# Регистрация команд
async def register_commands():
    commands = [
        BotCommand(command="/start", description="Запустить бота и получить инструкции"),
        BotCommand(command="/help", description="Показать справку по использованию"),
        BotCommand(command="/format", description="Информация о формате файлов"),
    ]
    await bot.set_my_commands(commands)

# Основная функция
async def main():
    await register_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())