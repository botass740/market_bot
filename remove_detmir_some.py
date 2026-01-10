import asyncio
from sqlalchemy import delete, select

from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker
from bot.db.models import PlatformCode, Platform, Product


REMOVE = 50  # сколько удалить


async def main():
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)

    async with session_factory() as session:
        platform = (await session.execute(
            select(Platform).where(Platform.code == PlatformCode.DM)
        )).scalar_one_or_none()

        if not platform:
            print("DM platform not found")
            await engine.dispose()
            return

        ids = (await session.execute(
            select(Product.id)
            .where(Product.platform_id == platform.id)
            .order_by(Product.id.desc())
            .limit(REMOVE)
        )).scalars().all()

        if not ids:
            print("No DM products to delete")
            await engine.dispose()
            return

        res = await session.execute(delete(Product).where(Product.id.in_(ids)))
        await session.commit()

        print(f"Deleted DM products: {res.rowcount or 0}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())