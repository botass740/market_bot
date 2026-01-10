# fill_products.py

import asyncio
from bot.db import create_engine, create_sessionmaker, init_db
from bot.db.models import PlatformCode
from bot.config import load_settings
from bot.services.product_manager import ProductManager

async def main():
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)
    await init_db(engine)
    
    manager = ProductManager(session_factory)
    
    print("Collecting 3000 products...")
    added, total = await manager.refill_products(PlatformCode.WB, target_count=3000)
    print(f"Added {added} products, total: {total}")
    
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())