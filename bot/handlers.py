import os
import pandas as pd
from aiogram import F, Router, Bot
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile
from aiogram.exceptions import TelegramNetworkError
from processing.payd import process_jdt as process_payd_jdt, process_ojdt as process_payd_ojdt
from processing.completed import process_jdt as process_completed_jdt, process_ojdt as process_completed_ojdt
from bot.utils import clean_temp_directory
from config.mappings import DEBIT_MAPPING_COMPLETED, DEBIT_MAPPING_PAYD, CREDIT_MAPPING_PEYD
from config.settings import ADMIN_ID


router = Router()


@router.message(Command("start"))
async def start(message: Message):
    await message.reply("Привет! Я бот для обработки файлов. Отправьте мне файл, и я обработаю его для вас.")

@router.message(Command("help"))
async def help_command(message: Message):
    help_text = (
        "🤖 *Как пользоваться ботом:*\n\n"
        "1. Отправьте CSV файл с отчетом\n"
        "2. Бот автоматически определит тип файла (PAYD или COMPLETED)\n"
        "3. Создаст и отправит вам JDT и OJDT отчеты\n\n"
        "По всем вопросам обращайтесь к администратору"
    )
    await message.reply(help_text, parse_mode='Markdown')

@router.message(Command("format"))
async def format_command(message: Message):
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
    await message.reply(format_text, parse_mode='Markdown')

@router.message(Command("info"))
async def info_command(message):
    # Создаем строки для отображения мапингов PAYD
    payd_debit_mappings = "\n".join([f"    - {k}: {v}" for k, v in DEBIT_MAPPING_PAYD.items()])
    payd_credit_mappings = "\n".join([f"    - {k}: {v}" for k, v in CREDIT_MAPPING_PEYD.items()])
    
    # Создаем строки для отображения мапингов COMPLETED
    completed_debit_mappings = "\n".join([f"    - {k}: {v}" for k, v in DEBIT_MAPPING_COMPLETED.items()])

    info_text = (
        "📊 <b>Структура проводок в SAP</b>\n\n"
        "<b>PAYD файл (простые платежи):</b>\n"
        "• Дебет = Кредит (Total Fee EUR)\n"
        "• Счета дебета:\n"
        f"{payd_debit_mappings}\n"
        "• Счета кредита:\n"
        f"{payd_credit_mappings}\n\n"
        
        "<b>COMPLETED файл (с комиссиями):</b>\n"
        "• Счета дебета:\n"
        f"{completed_debit_mappings}\n\n"
        
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

    await message.reply(info_text, parse_mode='HTML')

@router.message(F.document)
async def handle_file(message: Message, bot: Bot):
    try:
        # Создаем временные директории
        os.makedirs("temp", exist_ok=True)

        # Проверяем расширение файла
        if not message.document.file_name.endswith('.csv'):
            await message.reply("Пожалуйста, отправьте файл в формате CSV")
            return

        # Скачиваем файл
        file_info = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)

        # Сохраняем файл во временную директорию
        input_file = "temp/исходник.csv"
        with open(input_file, 'wb') as new_file:
            new_file.write(downloaded_file.read())  # .read() для BytesIO объекта

        # Определяем тип отчета
        df = pd.read_csv(input_file)
        report_type = await determine_report_type(df)

        # Формируем сообщение о процессе обработки
        report_type_msg = "COMPLETED" if report_type == 'completed' else "PAYD"
        bot_message = await message.answer(f"📥 Получен файл типа: {report_type_msg}\n⚙️ Обрабатываю...")
        bot_message_for_admin = await bot.send_message(ADMIN_ID, f"📥 Получен файл типа: {report_type_msg}")

        # Переименовываем файл для обработки
        renamed_file = f"temp/исходник ({report_type_msg}).csv"
        os.rename(input_file, renamed_file)

        output_jdt = "temp/jdt.csv"
        output_ojdt = "temp/ojdt.csv"

        # Обрабатываем файлы
        if report_type == 'completed':
            await process_completed_jdt(renamed_file, output_jdt)
            await process_completed_ojdt(renamed_file, output_ojdt)
        else:
            await process_payd_jdt(renamed_file, output_jdt)
            await process_payd_ojdt(renamed_file, output_ojdt)

        # Отправляем файлы пользователю с повторными попытками
        # Редактируем предыдущее сообщение о процессе обработки
        await bot_message.edit_text(f"{report_type_msg}\n✅ Успешно обработан")
        await send_file_with_retry(message, output_jdt, "jdt.csv", bot)
        await send_file_with_retry(message, output_ojdt, "ojdt.csv", bot)
        await bot_message_for_admin.edit_text(f"{report_type_msg}\n✅ Успешно обработан\n\nДля: @{message.from_user.username or 'Неизвестный пользователь'}")

    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        await message.reply(f"❌ Ошибка при обработке файла: \nтип и сообщение об ошибке отправлен разработчику")
        if ADMIN_ID:
            try:
                await bot.send_message(ADMIN_ID, f"❌ Ошибка при обработке файла: {error_type}: {error_msg}")
            except Exception as admin_error:
                print(f"Не удалось отправить сообщение администратору: {admin_error}")
    finally:
        await clean_temp_directory(directory="temp")

async def send_file_with_retry(message, file_path, filename, bot, max_retries=3):
    """Отправляет файл с повторными попытками при сетевых ошибках."""
    for attempt in range(max_retries):
        try:
            # Используем FSInputFile правильно - передаем путь к файлу, а не открытый фа
            await bot.send_document(message.chat.id, FSInputFile(file_path, filename=filename))
            return True  # Успешно отправлено
        except TelegramNetworkError as e:
            if attempt < max_retries - 1:
                # Если это не последняя попытка, ждем и пробуем снова
                retry_delay = 2 * (attempt + 1)  # Увеличиваем задержку с каждой попыткой
                await message.answer(f"⚠️ Проблема с сетью, повторная попытка через {retry_delay} сек...")
            else:
                # Если все попытки исчерпаны, сообщаем об ошибке
                await message.answer(f"❌ Не удалось отправить файл после {max_retries} попыток. Ошибка сети.")
                if ADMIN_ID:
                    try:
                        await bot.send_message(ADMIN_ID, f"❌ Сетевая ошибка при отправке файла {filename}: {str(e)}")
                    except:
                        pass  # Игнорируем ошибки при отправке сообщения администратору
                return False
        except Exception as e:
            try:
                await bot.send_message(ADMIN_ID, f"❌ Ошибка при отправке файла {filename}: {type(e).__name__}: {str(e)}")
            except:
                pass
        return False

async def determine_report_type(df):
    """Определяет тип отчета и проверяет валидность файла."""
    df.columns = df.columns.str.strip()
    if 'Reseller Fee EUR' in df.columns or 'Reseller\nFee EUR' in df.columns:
        return 'completed'
    elif 'Paid' in df.columns:
        return 'payd'
    else:
        raise ValueError("Неверный формат файла. Проверьте колонки.")