import asyncio
from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker
from bot.db.models import PlatformCode
from bot.services.settings_manager import SettingsManager
from bot.services.product_manager import ProductManager

TARGET = 3000

async def main():
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)

    sm = SettingsManager(session_factory)
    pm = ProductManager(session_factory, settings_manager=sm)

    before = await pm.get_product_count(PlatformCode.DM)
    removed = await pm.trim_to_target(PlatformCode.DM, TARGET)
    after = await pm.get_product_count(PlatformCode.DM)

    print(f"DM before={before}, removed={removed}, after={after}")

    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())