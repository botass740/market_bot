# bot/handlers/admin.py

import logging
log = logging.getLogger(__name__)

from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

from bot.db.models.settings import BotSettings
from bot.services.settings_manager import SettingsManager

from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func
from bot.db.models import Product, PriceHistory

router = Router()

# Глобальная переменная для settings_manager (инициализируется в main.py)
settings_manager: SettingsManager | None = None


def set_settings_manager(manager: SettingsManager) -> None:
    """Устанавливает settings_manager для использования в хендлерах."""
    global settings_manager
    settings_manager = manager


async def check_admin(message: Message) -> bool:
    """Проверяет права админа и отвечает если нет прав."""
    if not settings_manager:
        await message.answer("❌ Сервис настроек не инициализирован")
        return False
    
    if not await settings_manager.is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав администратора")
        return False
    
    return True


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    """Показывает текущие настройки."""
    if not await check_admin(message):
        return
    
    settings = await settings_manager.get_all_settings()
    
    categories = settings["categories"]
    categories_str = ", ".join(categories[:5])
    if len(categories) > 5:
        categories_str += f" ... (+{len(categories) - 5})"
    
    text = f"""
⚙️ <b>Текущие настройки</b>

💰 <b>Фильтры цены:</b>
• Мин. цена: <code>{settings['min_price']:.0f}</code> ₽
• Макс. цена: <code>{settings['max_price']:.0f}</code> ₽ (0 = без ограничения)
• Мин. скидка: <code>{settings['min_discount']:.0f}</code>%

📢 <b>Пороги публикации:</b>
• Падение цены: <code>{settings['min_price_drop']:.1f}</code>%
• Рост скидки: <code>{settings['min_discount_increase']:.1f}</code>%

📦 <b>Категории ({len(categories)}):</b>
{categories_str}

👤 <b>Админы:</b> {len(settings['admin_ids'])} чел.

<i>Используйте /help_admin для списка команд</i>
"""
    await message.answer(text, parse_mode="HTML")


@router.message(Command("help_admin"))
async def cmd_help_admin(message: Message):
    """Показывает справку по админ-командам."""
    if not await check_admin(message):
        return
    
    text = """
🔧 <b>Команды администратора</b>

<b>Фильтры:</b>
/set_min_price 500 — мин. цена 500₽
/set_max_price 50000 — макс. цена (0 = без ограничения)
/set_min_discount 10 — мин. скидка 10%

<b>Пороги публикации:</b>
/set_price_drop 5 — публиковать если цена упала на 5%+
/set_discount_increase 15 — публиковать если скидка выросла на 15%+

<b>Категории:</b>
/categories — список категорий
/add_category телевизор — добавить
/remove_category игрушки — убрать

<b>Админы:</b>
/add_admin 123456789 — добавить админа по ID
/admins — список админов

<b>Прочее:</b>
/settings — все настройки
/stats — статистика
"""
    await message.answer(text, parse_mode="HTML")


@router.message(Command("set_min_price"))
async def cmd_set_min_price(message: Message):
    """Устанавливает минимальную цену."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите цену: /set_min_price 500")
        return
    
    try:
        value = float(args[1].replace(",", "."))
        await settings_manager.set(BotSettings.KEY_MIN_PRICE, str(value))
        await message.answer(f"✅ Минимальная цена: <b>{value:.0f} ₽</b>", parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Неверный формат числа")


@router.message(Command("set_max_price"))
async def cmd_set_max_price(message: Message):
    """Устанавливает максимальную цену."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите цену: /set_max_price 50000")
        return
    
    try:
        value = float(args[1].replace(",", "."))
        await settings_manager.set(BotSettings.KEY_MAX_PRICE, str(value))
        msg = f"✅ Максимальная цена: <b>{value:.0f} ₽</b>" if value > 0 else "✅ Максимальная цена: <b>без ограничения</b>"
        await message.answer(msg, parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Неверный формат числа")


@router.message(Command("set_min_discount"))
async def cmd_set_min_discount(message: Message):
    """Устанавливает минимальную скидку для фильтрации."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите процент: /set_min_discount 10")
        return
    
    try:
        value = float(args[1].replace(",", ".").replace("%", ""))
        await settings_manager.set(BotSettings.KEY_MIN_DISCOUNT, str(value))
        await message.answer(f"✅ Минимальная скидка для фильтра: <b>{value:.0f}%</b>", parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Неверный формат числа")


@router.message(Command("set_price_drop"))
async def cmd_set_price_drop(message: Message):
    """Устанавливает порог падения цены для публикации."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите процент: /set_price_drop 5")
        return
    
    try:
        value = float(args[1].replace(",", ".").replace("%", ""))
        await settings_manager.set(BotSettings.KEY_MIN_PRICE_DROP, str(value))
        await message.answer(f"✅ Публикация при падении цены на: <b>{value:.1f}%+</b>", parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Неверный формат числа")


@router.message(Command("set_discount_increase"))
async def cmd_set_discount_increase(message: Message):
    """Устанавливает порог роста скидки для публикации."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите процент: /set_discount_increase 15")
        return
    
    try:
        value = float(args[1].replace(",", ".").replace("%", ""))
        await settings_manager.set(BotSettings.KEY_MIN_DISCOUNT_INCREASE, str(value))
        await message.answer(f"✅ Публикация при росте скидки на: <b>{value:.1f}%+</b>", parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Неверный формат числа")


@router.message(Command("categories"))
async def cmd_categories(message: Message):
    """Показывает список категорий."""
    if not await check_admin(message):
        return
    
    categories = await settings_manager.get_list(BotSettings.KEY_CATEGORIES)
    
    if not categories:
        await message.answer("📦 Категории не заданы")
        return
    
    text = f"📦 <b>Категории ({len(categories)}):</b>\n\n"
    text += "\n".join(f"• {cat}" for cat in categories)
    
    await message.answer(text, parse_mode="HTML")


@router.message(Command("add_category"))
async def cmd_add_category(message: Message):
    """Добавляет категорию."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите категорию: /add_category телевизор")
        return
    
    category = args[1].strip()
    categories = await settings_manager.add_to_list(BotSettings.KEY_CATEGORIES, category)
    
    await message.answer(f"✅ Категория добавлена: <b>{category}</b>\nВсего категорий: {len(categories)}", parse_mode="HTML")


@router.message(Command("remove_category"))
async def cmd_remove_category(message: Message):
    """Удаляет категорию."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите категорию: /remove_category игрушки")
        return
    
    category = args[1].strip()
    categories = await settings_manager.remove_from_list(BotSettings.KEY_CATEGORIES, category)
    
    await message.answer(f"✅ Категория удалена: <b>{category}</b>\nОсталось категорий: {len(categories)}", parse_mode="HTML")


@router.message(Command("add_admin"))
async def cmd_add_admin(message: Message):
    """Добавляет администратора."""
    if not await check_admin(message):
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите ID пользователя: /add_admin 123456789")
        return
    
    try:
        user_id = int(args[1].strip())
        await settings_manager.add_admin(user_id)
        await message.answer(f"✅ Админ добавлен: <code>{user_id}</code>", parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Неверный ID пользователя")


@router.message(Command("admins"))
async def cmd_admins(message: Message):
    """Показывает список админов."""
    if not await check_admin(message):
        return
    
    admin_ids = await settings_manager.get_admin_ids()
    
    if not admin_ids:
        await message.answer("👤 Админы не заданы (доступ открыт всем)")
        return
    
    text = f"👤 <b>Администраторы ({len(admin_ids)}):</b>\n\n"
    text += "\n".join(f"• <code>{aid}</code>" for aid in admin_ids)
    
    await message.answer(text, parse_mode="HTML")


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    """Показывает ID пользователя."""
    await message.answer(f"Ваш ID: <code>{message.from_user.id}</code>", parse_mode="HTML")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Показывает статистику бота."""
    if not await check_admin(message):
        return
    
    if not settings_manager:
        await message.answer("❌ Сервис не инициализирован")
        return
    
    try:
        # Получаем статистику из БД
        async with settings_manager._session_factory() as session:
            # Количество товаров
            products_result = await session.execute(
                select(func.count(Product.id))
            )
            total_products = products_result.scalar() or 0
            
            # Товары с ценой
            priced_result = await session.execute(
                select(func.count(Product.id)).where(Product.current_price.isnot(None))
            )
            priced_products = priced_result.scalar() or 0
            
            # Изменения за последние 24 часа
            day_ago = datetime.now(timezone.utc) - timedelta(days=1)
            changes_result = await session.execute(
                select(func.count(PriceHistory.id)).where(
                    PriceHistory.checked_at >= day_ago
                )
            )
            changes_24h = changes_result.scalar() or 0
            
            # Изменения за последний час
            hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            changes_hour_result = await session.execute(
                select(func.count(PriceHistory.id)).where(
                    PriceHistory.checked_at >= hour_ago
                )
            )
            changes_1h = changes_hour_result.scalar() or 0
        
        # Настройки
        all_settings = await settings_manager.get_all_settings()
        
        text = f"""
📊 <b>Статистика бота</b>

<b>Товары:</b>
• Всего в базе: <code>{total_products}</code>
• С ценой: <code>{priced_products}</code>

<b>Активность:</b>
• Изменений за час: <code>{changes_1h}</code>
• Изменений за 24ч: <code>{changes_24h}</code>

<b>Пороги публикации:</b>
• Падение цены: <code>{all_settings['min_price_drop']:.1f}%</code>
• Рост скидки: <code>{all_settings['min_discount_increase']:.1f}%</code>

<b>Фильтры:</b>
• Мин. цена: <code>{all_settings['min_price']:.0f}</code> ₽
• Макс. цена: <code>{all_settings['max_price']:.0f}</code> ₽
• Мин. скидка: <code>{all_settings['min_discount']:.0f}%</code>

<i>Обновлено: {datetime.now().strftime('%H:%M:%S')}</i>
"""
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        log.exception("Failed to get stats")
        await message.answer(f"❌ Ошибка: {e}")

@router.message(Command("clear_categories"))
async def cmd_clear_categories(message: Message):
    """Очищает список категорий."""
    if not await check_admin(message):
        return
    
    await settings_manager.set(BotSettings.KEY_CATEGORIES, "")
    settings_manager.clear_cache()
    await message.answer(
        "✅ Список категорий очищен\n\n"
        "<i>Категории используются только для добора товаров (refill), "
        "не для фильтрации публикаций.</i>",
        parse_mode="HTML"
    )

@router.message(Command("dm_slugs"))
async def cmd_dm_slugs(message: Message):
    """Показывает список Detmir slug'ов (для COLLECT)."""
    if not await check_admin(message):
        return

    slugs = await settings_manager.get_list(BotSettings.KEY_DETMIR_SLUGS)

    if not slugs:
        await message.answer("🟢 DM slugs не заданы")
        return

    text = f"🟢 <b>Detmir slugs ({len(slugs)}):</b>\n\n"
    text += "\n".join(f"• {s}" for s in slugs)
    await message.answer(text, parse_mode="HTML")


@router.message(Command("dm_add_slug"))
async def cmd_dm_add_slug(message: Message):
    """Добавляет Detmir slug."""
    if not await check_admin(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите slug: /dm_add_slug obuv")
        return

    slug = args[1].strip()
    slugs = await settings_manager.add_to_list(BotSettings.KEY_DETMIR_SLUGS, slug)

    await message.answer(
        f"✅ DM slug добавлен: <b>{slug}</b>\nВсего: {len(slugs)}",
        parse_mode="HTML",
    )


@router.message(Command("dm_remove_slug"))
async def cmd_dm_remove_slug(message: Message):
    """Удаляет Detmir slug."""
    if not await check_admin(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Укажите slug: /dm_remove_slug obuv")
        return

    slug = args[1].strip()
    slugs = await settings_manager.remove_from_list(BotSettings.KEY_DETMIR_SLUGS, slug)

    await message.answer(
        f"✅ DM slug удалён: <b>{slug}</b>\nОсталось: {len(slugs)}",
        parse_mode="HTML",
    )

@router.message(Command("dm_refresh"))
async def cmd_dm_refresh(message: Message):
    """Запрашивает принудительную ротацию DM (20%) в следующем цикле."""
    if not await check_admin(message):
        return

    try:
        from pathlib import Path
        Path(".last_rotation_detmir").write_text("0")

        await message.answer(
            "✅ Ротация DM (20%) запрошена.\n"
            "Она выполнится в следующем цикле DM (планировщик).",
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось поставить метку ротации: {e}")

@router.message(Command("wb_refresh"))
async def cmd_wb_refresh(message: Message):
    if not await check_admin(message):
        return

    try:
        from pathlib import Path
        Path(".last_rotation_wb").write_text("0")
        await message.answer(
            "✅ Ротация WB (20%) запрошена. Выполнится в следующем цикле WB.",
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось поставить метку ротации: {e}")


@router.message(Command("ozon_refresh"))
async def cmd_ozon_refresh(message: Message):
    """Запрашивает принудительную ротацию OZON (20%) в следующем цикле."""
    if not await check_admin(message):
        return
    try:
        from pathlib import Path
        Path(".last_rotation_ozon").write_text("0")
        await message.answer(
            "✅ Ротация OZON (20%) запрошена. Выполнится в следующем цикле OZON.",
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось поставить метку ротации OZON: {e}")