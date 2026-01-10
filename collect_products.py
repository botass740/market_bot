import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)

from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker, init_db
from bot.db.models import PlatformCode
from bot.services.catalog_parser import collect_products_for_monitoring
from bot.services.product_manager import ProductManager


async def main():
    print("="*70)
    print("СБОР ТОВАРОВ WB ДЛЯ МОНИТОРИНГА")
    print("="*70)
    
    # Поисковые запросы / категории
    # Можешь изменить под нужды заказчика
    QUERIES = [
        "смартфон",
        "ноутбук",
        "наушники",
        "платье",
        "кроссовки",
        "сумка",
        "часы",
        "парфюм",
        "игрушки",
        "косметика",
    ]
    
    TARGET_COUNT = 3000
    
    print(f"\nЗапросы: {QUERIES}")
    print(f"Цель: {TARGET_COUNT} товаров")
    
    # 1. Собираем артикулы
    print("\n" + "="*70)
    print("ШАГ 1: Сбор артикулов из каталога")
    print("="*70)
    
    product_ids = await collect_products_for_monitoring(
        queries=QUERIES,
        target_count=TARGET_COUNT,
    )
    
    print(f"\n✅ Собрано: {len(product_ids)} уникальных артикулов")
    
    # 2. Сохраняем в БД
    print("\n" + "="*70)
    print("ШАГ 2: Сохранение в базу данных")
    print("="*70)
    
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)
    await init_db(engine)
    
    manager = ProductManager(session_factory)
    
    added, skipped = await manager.add_products(
        platform=PlatformCode.WB,
        external_ids=[str(pid) for pid in product_ids],
    )
    
    print(f"✅ Добавлено: {added}")
    print(f"⏭️  Пропущено (уже есть): {skipped}")
    
    total = await manager.get_product_count(PlatformCode.WB)
    print(f"📊 Всего в БД: {total}")
    
    await engine.dispose()
    
    print("\n" + "="*70)
    print("ГОТОВО!")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())