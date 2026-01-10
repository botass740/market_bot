# bot/filtering/filters.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any, TYPE_CHECKING

from bot.config import FilteringThresholds

if TYPE_CHECKING:
    from bot.services.settings_manager import SettingsManager


class FilterService:
    """
    Фильтрация товаров по цене, скидке, остаткам.
    Категории НЕ фильтруются — они используются только для refill.
    """
    
    def __init__(
        self,
        thresholds: FilteringThresholds,
        settings_manager: "SettingsManager | None" = None,
    ) -> None:
        self._static = thresholds
        self._settings_manager = settings_manager

    def set_settings_manager(self, manager: "SettingsManager") -> None:
        """Устанавливает менеджер для динамических настроек."""
        self._settings_manager = manager

    async def get_thresholds(self) -> dict[str, Any]:
        """
        Получает актуальные пороги из БД.
        Приоритет: БД -> статические настройки.
        """
        if self._settings_manager is None:
            return {
                "min_price": self._static.min_price,
                "max_price": self._static.max_price,
                "min_stock": self._static.min_stock,
                "min_discount_percent": self._static.min_discount_percent,
            }
        
        from bot.db.models.settings import BotSettings
        
        min_price = await self._settings_manager.get_float(BotSettings.KEY_MIN_PRICE)
        max_price = await self._settings_manager.get_float(BotSettings.KEY_MAX_PRICE)
        min_discount = await self._settings_manager.get_float(BotSettings.KEY_MIN_DISCOUNT)
        
        return {
            "min_price": min_price if min_price > 0 else self._static.min_price,
            "max_price": max_price if max_price > 0 else self._static.max_price,
            "min_stock": self._static.min_stock,
            "min_discount_percent": min_discount if min_discount > 0 else self._static.min_discount_percent,
        }

    def filter_products(self, products: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Синхронная фильтрация (использует статические настройки)."""
        return [p for p in products if self._passes_static(p)]

    async def filter_products_async(self, products: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Асинхронная фильтрация (использует динамические настройки из БД)."""
        thresholds = await self.get_thresholds()
        return [p for p in products if self._passes_with_thresholds(p, thresholds)]

    def passes(self, product: dict[str, Any]) -> bool:
        """Проверка с статическими настройками."""
        return self._passes_static(product)

    def _passes_static(self, product: dict[str, Any]) -> bool:
        """Проверка со статическими порогами."""
        thresholds = {
            "min_price": self._static.min_price,
            "max_price": self._static.max_price,
            "min_stock": self._static.min_stock,
            "min_discount_percent": self._static.min_discount_percent,
        }
        return self._passes_with_thresholds(product, thresholds)

    def _passes_with_thresholds(self, product: dict[str, Any], thresholds: dict[str, Any]) -> bool:
        """
        Проверка товара по порогам.
        Фильтруем только по: цене, скидке, остаткам.
        Категории НЕ проверяем — они для refill.
        """
        price = _as_float(product.get("price"))
        stock = _as_int(product.get("stock"))
        discount = _as_float(product.get("discount_percent"))

        min_price = thresholds.get("min_price", 0)
        max_price = thresholds.get("max_price", 0)
        min_stock = thresholds.get("min_stock", 0)
        min_discount_percent = thresholds.get("min_discount_percent", 0)

        # Фильтр по цене
        if min_price > 0 or max_price > 0:
            if price is None:
                return False
            if min_price > 0 and price < min_price:
                return False
            if max_price > 0 and price > max_price:
                return False

        # Фильтр по остаткам
        if min_stock > 0:
            if stock is None or stock < min_stock:
                return False

        # Фильтр по скидке
        if min_discount_percent > 0:
            if discount is None or discount < min_discount_percent:
                return False

        return True


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ".").strip())
    except ValueError:
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None