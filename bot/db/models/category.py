#bot/db/models/category.py

from __future__ import annotations

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bot.db.base import Base


class Category(Base):
    __tablename__ = "categories"
    __table_args__ = (
        UniqueConstraint("platform_id", "external_id", name="uq_categories_platform_external"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    platform_id: Mapped[int] = mapped_column(ForeignKey("platforms.id"), nullable=False)

    external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)

    platform = relationship("Platform", back_populates="categories")
    products = relationship("Product", back_populates="category")
