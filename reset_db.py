#reset_db.py
"""
Очищает базу по платформам (WB / OZON / DM) или целиком.
"""

import asyncio
from sqlalchemy import delete, select

from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker
from bot.db.models import Product, PriceHistory, Platform, PlatformCode
from bot.services.product_manager import ProductManager
from bot.services.settings_manager import SettingsManager


async def _get_platform(session, code: PlatformCode) -> Platform | None:
    result = await session.execute(select(Platform).where(Platform.code == code))
    return result.scalar_one_or_none()


async def _delete_platform_data(session, platform: Platform) -> int:
    """
    Удаляет PriceHistory и Product для конкретной платформы.
    Возвращает количество удалённых товаров.
    """
    # Удаляем историю цен по товарам платформы
    platform_product_ids = select(Product.id).where(Product.platform_id == platform.id)

    await session.execute(
        delete(PriceHistory).where(PriceHistory.product_id.in_(platform_product_ids))
    )
    # Удаляем товары платформы
    res = await session.execute(
        delete(Product).where(Product.platform_id == platform.id)
    )
    return res.rowcount or 0


async def main():
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)

    settings_manager = SettingsManager(session_factory)
    product_manager = ProductManager(session_factory, settings_manager=settings_manager)

    # 1. Показываем текущее состояние
    print(f"\n{'='*60}")
    print("Текущее состояние базы:")

    for platform_code in [PlatformCode.WB, PlatformCode.OZON, PlatformCode.DM]:
        count = await product_manager.get_product_count(platform_code)
        print(f"  {platform_code.value}: {count} товаров")

    # 2. Показываем категории (общие)
    categories = await product_manager.get_refill_categories()
    print(f"\nКатегории для добора ({len(categories)}):")
    print(f"  {', '.join(categories[:10])}...")

    # 3. Выбор действия
    print(f"\n{'='*60}")
    print("Выберите действие:")
    print("  1 - Очистить только OZON")
    print("  2 - Очистить только WB")
    print("  3 - Очистить только DM")
    print("  4 - Очистить ВСЁ (WB + OZON + DM)")
    print("  0 - Отмена")

    choice = input("\nВведите номер: ").strip()

    if choice == "0":
        print("Отменено.")
        await engine.dispose()
        return

    if choice not in ["1", "2", "3", "4"]:
        print("Неверный выбор. Отменено.")
        await engine.dispose()
        return

    # 4. Подтверждение
    print("\n⚠️  ВНИМАНИЕ: Данные будут удалены!")
    confirm = input("Введите 'YES' для подтверждения: ").strip()

    if confirm != "YES":
        print("Отменено.")
        await engine.dispose()
        return

    async with session_factory() as session:
        if choice == "1":
            platform = await _get_platform(session, PlatformCode.OZON)
            if not platform:
                print("❌ Платформа OZON не найдена в базе")
            else:
                deleted = await _delete_platform_data(session, platform)
                await session.commit()
                print(f"✅ OZON очищен: удалено {deleted} товаров")

        elif choice == "2":
            platform = await _get_platform(session, PlatformCode.WB)
            if not platform:
                print("❌ Платформа WB не найдена в базе")
            else:
                deleted = await _delete_platform_data(session, platform)
                await session.commit()
                print(f"✅ WB очищен: удалено {deleted} товаров")

        elif choice == "3":
            platform = await _get_platform(session, PlatformCode.DM)
            if not platform:
                print("❌ Платформа DM не найдена в базе")
            else:
                deleted = await _delete_platform_data(session, platform)
                await session.commit()
                print(f"✅ DM очищен: удалено {deleted} товаров")

        elif choice == "4":
            await session.execute(delete(PriceHistory))
            res = await session.execute(delete(Product))
            await session.commit()
            print(f"✅ Вся база очищена: удалено {res.rowcount or 0} товаров")

    # 5. Проверяем результат
    print(f"\n{'='*60}")
    print("Состояние после очистки:")

    for platform_code in [PlatformCode.WB, PlatformCode.OZON, PlatformCode.DM]:
        count = await product_manager.get_product_count(platform_code)
        print(f"  {platform_code.value}: {count} товаров")

    print(f"\n{'='*60}")
    print("Готово! Запустите бота — он автоматически наполнит базу")
    print("по категориям/логике платформ при первом цикле.")
    print(f"{'='*60}\n")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())