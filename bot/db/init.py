# bot/db/init.py
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine

from bot.db.base import Base

async def init_db(engine: AsyncEngine) -> None:
    from bot.db import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
