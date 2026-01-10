#bot/db/models/platform.py
from __future__ import annotations

import enum

from sqlalchemy import Enum, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bot.db.base import Base


class PlatformCode(str, enum.Enum):
    WB = "WB"
    OZON = "OZON"
    DM = "DM"


class Platform(Base):
    __tablename__ = "platforms"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[PlatformCode] = mapped_column(
        Enum(PlatformCode, name="platform_code"),
        unique=True,
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    products = relationship("Product", back_populates="platform")
    categories = relationship("Category", back_populates="platform")
