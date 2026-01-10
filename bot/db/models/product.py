# bot/db/models/product.py

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Numeric, String, UniqueConstraint, Boolean, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from bot.db.base import Base


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("platform_id", "external_id", name="uq_products_platform_external"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    platform_id: Mapped[int] = mapped_column(ForeignKey("platforms.id"), nullable=False)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("categories.id"), nullable=True)

    external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)

    current_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    old_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    discount: Mapped[float | None] = mapped_column(nullable=True)

    stock: Mapped[int | None] = mapped_column(nullable=True)
    rating: Mapped[float | None] = mapped_column(nullable=True)

    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # === ПОЛЯ СТАБИЛЬНОСТИ ===
    # Счётчик успешных парсингов с полными данными
    stable_parse_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0")

    # Товар стабилен (данные надёжны для сравнения)
    is_stable: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, server_default="0")

    # Baseline — зафиксированные значения после стабилизации
    baseline_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    baseline_discount: Mapped[float | None] = mapped_column(nullable=True)
    baseline_set_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # === DEAD / OZON ===
    dead_check_fail_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0")
    last_dead_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)

     # Счётчик неудачных загрузок картинки (для "мягкой смерти")
    no_image_fail_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0")

    platform = relationship("Platform", back_populates="products")
    category = relationship("Category", back_populates="products")
    price_history = relationship(
        "PriceHistory",
        back_populates="product",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def has_complete_data(self) -> bool:
        """Проверяет, что данные полные."""
        return (
            self.current_price is not None
            and self.old_price is not None
            and self.current_price > 0
            and self.old_price > 0
        )