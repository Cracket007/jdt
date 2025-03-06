import os
import logging


async def clean_temp_directory(directory="temp"):
    """Удаляет все файлы из указанной временной директории. """
    if os.path.exists(directory):
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                logging.error(f"Ошибка при удалении файла {file_path}: {e}")

async def ensure_directories_exist(directories):
    """Убеждается, что указанные директории существуют.Если директория не существует, она будет создана."""
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception as e:
            logging.error(f"Ошибка при создании директории {directory}: {e}")

async def format_error_message(user, error):
    """Форматирует сообщение об ошибке для отправки администратору."""
    return (
        f"❌ Ошибка при обработке запроса\n\n"
        f"Пользователь: @{user.username if user.username else 'Неизвестный пользователь'}\n"
        f"Ошибка: {str(error)}"
    )