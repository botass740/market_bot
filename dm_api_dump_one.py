import asyncio
import aiohttp
from sqlalchemy import select

from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker
from bot.db.models import PlatformCode, Platform, Product


async def main():
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)

    async with session_factory() as session:
        platform = (await session.execute(
            select(Platform).where(Platform.code == PlatformCode.DM)
        )).scalar_one()

        pid = (await session.execute(
            select(Product.external_id).where(Product.platform_id == platform.id).limit(1)
        )).scalar_one()

    url = f"https://api.detmir.ru/v2/products/{pid}"
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            print("status:", r.status)
            print(await r.text())

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())