# migrate_add_stability.py
"""
Миграция: добавление полей стабильности в таблицу products.
Запустить один раз: python migrate_add_stability.py
"""

import asyncio
from sqlalchemy import text
from bot.db import create_engine


async def migrate():
    engine = create_engine("sqlite+aiosqlite:///./parser.db")

    async with engine.begin() as conn:
        # Проверяем, есть ли уже колонки
        result = await conn.execute(text("PRAGMA table_info(products)"))
        columns = {row[1] for row in result.fetchall()}

        if "stable_parse_count" not in columns:
            print("Adding stable_parse_count...")
            await conn.execute(text(
                "ALTER TABLE products ADD COLUMN stable_parse_count INTEGER DEFAULT 0 NOT NULL"
            ))

        if "is_stable" not in columns:
            print("Adding is_stable...")
            await conn.execute(text(
                "ALTER TABLE products ADD COLUMN is_stable BOOLEAN DEFAULT 0 NOT NULL"
            ))

        if "baseline_price" not in columns:
            print("Adding baseline_price...")
            await conn.execute(text(
                "ALTER TABLE products ADD COLUMN baseline_price NUMERIC(12, 2)"
            ))

        if "baseline_discount" not in columns:
            print("Adding baseline_discount...")
            await conn.execute(text(
                "ALTER TABLE products ADD COLUMN baseline_discount FLOAT"
            ))

        if "baseline_set_at" not in columns:
            print("Adding baseline_set_at...")
            await conn.execute(text(
                "ALTER TABLE products ADD COLUMN baseline_set_at DATETIME"
            ))

        print("Migration complete!")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(migrate())