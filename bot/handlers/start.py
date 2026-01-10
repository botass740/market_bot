#bot/handlers/start.py
from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 <b>Бот мониторинга скидок WB</b>\n\n"
        "📢 Отслеживаю 3000 товаров и публикую лучшие скидки в канал.\n\n"
        "<b>Команды:</b>\n"
        "/start — это сообщение\n"
        "/myid — ваш Telegram ID\n"
        "/stats — статистика бота\n"
        "/settings — текущие настройки\n"
        "/help_admin — команды администратора",
        parse_mode="HTML"
    )