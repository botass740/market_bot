# migrate_add_no_image_count.py
"""
Добавляет поле no_image_fail_count в таблицу products.
"""

import asyncio
from sqlalchemy import text
from bot.config import load_settings
from bot.db import create_engine


async def main():
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    
    async with engine.begin() as conn:
        # Проверяем, есть ли уже колонка
        try:
            await conn.execute(text(
                "ALTER TABLE products ADD COLUMN no_image_fail_count INTEGER NOT NULL DEFAULT 0"
            ))
            print("✅ Колонка no_image_fail_count добавлена")
        except Exception as e:
            if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
                print("ℹ️ Колонка no_image_fail_count уже существует")
            else:
                print(f"❌ Ошибка: {e}")
    
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())