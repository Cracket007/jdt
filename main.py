import logging
import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from config.settings import BOT_TOKEN
from bot.handlers import router


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


async def register_commands():
    # Создаем список команд вручную
    commands = []
    commands.append(BotCommand(command="start", description="Запустить бота и получить инструкции"))
    commands.append(BotCommand(command="help", description="Показать справку"))
    commands.append(BotCommand(command="format", description="Информация о формате файлов"))
    commands.append(BotCommand(command="info", description="Информация о боте"))

    # Устанавливаем команды
    await bot.set_my_commands(commands)

async def main():
    await register_commands()  # Регистрируем команды
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")
