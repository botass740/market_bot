# bot/db/services/change_detection.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.db.models import Platform, PlatformCode, Product
from bot.db.models.price_history import PriceHistory


# Минимум парсингов для стабилизации
MIN_STABLE_PARSE_COUNT = 2


@dataclass
class FieldChange:
    field: str
    old: Any
    new: Any


@dataclass
class ChangeResult:
    product: Product
    is_new: bool
    is_stable: bool
    just_stabilized: bool
    changes: list[FieldChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0


def _to_decimal(val: Any) -> Decimal | None:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


def _has_complete_data(item: dict[str, Any]) -> bool:
    """Проверяет, что парсер вернул полные данные."""
    price = item.get("price")
    old_price = item.get("old_price")

    if price is None or old_price is None:
        return False

    try:
        return float(price) > 0 and float(old_price) > 0
    except (TypeError, ValueError):
        return False


async def detect_and_save_changes(
    session: AsyncSession,
    *,
    platform_code: PlatformCode,
    items: list[dict[str, Any]],
) -> list[ChangeResult]:
    """
    Обнаруживает изменения и сохраняет в БД.

    Логика стабилизации:
    1. Новый товар: is_stable=False, stable_parse_count=0
    2. Парсинг с полными данными: stable_parse_count += 1
    3. Когда stable_parse_count >= MIN_STABLE_PARSE_COUNT:
       - is_stable=True
       - baseline_price/baseline_discount фиксируются
    4. Изменения детектируются только для стабильных товаров
    5. Сравнение идёт с baseline (не с предыдущим значением)
    """

    if not items:
        return []

    # Получаем платформу
    stmt = select(Platform).where(Platform.code == platform_code)
    result = await session.execute(stmt)
    platform = result.scalar_one_or_none()

    if not platform:
        platform = Platform(code=platform_code, name=platform_code.value)
        session.add(platform)
        await session.flush()

    # Собираем external_id
    external_ids = [str(it.get("external_id")) for it in items if it.get("external_id")]

    # Загружаем существующие товары
    stmt = select(Product).where(
        Product.platform_id == platform.id,
        Product.external_id.in_(external_ids),
    )
    result = await session.execute(stmt)
    existing_products = {p.external_id: p for p in result.scalars().all()}

    now = datetime.now(timezone.utc)
    results: list[ChangeResult] = []

    for item in items:
        external_id = str(item.get("external_id", ""))
        if not external_id:
            continue

        product = existing_products.get(external_id)
        is_new = product is None
        just_stabilized = False

        # === Создание нового товара ===
        if is_new:
            product = Product(
                platform_id=platform.id,
                external_id=external_id,
                title=item.get("name") or item.get("title") or f"Товар {external_id}",
                url=item.get("product_url"),
                current_price=_to_decimal(item.get("price")),
                old_price=_to_decimal(item.get("old_price")),
                discount=_to_float(item.get("discount_percent")),
                stock=item.get("stock"),
                rating=_to_float(item.get("rating")),
                last_checked_at=now,
                stable_parse_count=1 if _has_complete_data(item) else 0,
                is_stable=False,
                baseline_price=None,
                baseline_discount=None,
                baseline_set_at=None,
                dead_check_fail_count=0,
                last_dead_reason=None,
            )
            session.add(product)

            results.append(ChangeResult(
                product=product,
                is_new=True,
                is_stable=False,
                just_stabilized=False,
                changes=[],
            ))
            continue

        # === Обновление существующего товара ===

        new_price = _to_decimal(item.get("price"))
        new_old_price = _to_decimal(item.get("old_price"))
        new_discount = _to_float(item.get("discount_percent"))
        new_stock = item.get("stock")
        new_rating = _to_float(item.get("rating"))

        # === DEAD-check для OZON/общий: учитываем ошибки парсинга ===
        err = item.get("error")
        err_str = str(err) if err is not None else None

        # Если товар "ожил" (есть цена) — сбрасываем счётчик
        if new_price is not None and new_price > 0:
            product.dead_check_fail_count = 0
            product.last_dead_reason = None
        else:
            # Увеличиваем счётчик только для "фатальных" причин (удалён)
            if err_str in ("404", "410"):
                product.dead_check_fail_count = (product.dead_check_fail_count or 0) + 1
                product.last_dead_reason = err_str

        # Обновляем счётчик стабильности
        if _has_complete_data(item):
            product.stable_parse_count = (product.stable_parse_count or 0) + 1

        # Проверяем, нужно ли стабилизировать
        was_stable = product.is_stable
        if not was_stable and (product.stable_parse_count or 0) >= MIN_STABLE_PARSE_COUNT:
            product.is_stable = True
            product.baseline_price = new_price
            product.baseline_discount = new_discount
            product.baseline_set_at = now
            just_stabilized = True

        # === Детекция изменений (только для стабильных товаров) ===
        changes: list[FieldChange] = []

        if product.is_stable and not just_stabilized:
            # Сравниваем с baseline
            baseline_price = product.baseline_price
            baseline_discount = product.baseline_discount

            # Изменение цены
            if baseline_price is not None and new_price is not None:
                if new_price != baseline_price:
                    changes.append(FieldChange(
                        field="price",
                        old=baseline_price,
                        new=new_price,
                    ))
                    # Обновляем baseline при изменении
                    product.baseline_price = new_price
                    product.baseline_set_at = now

            # Изменение скидки
            if baseline_discount is not None and new_discount is not None:
                if abs(new_discount - baseline_discount) >= 1.0:
                    changes.append(FieldChange(
                        field="discount",
                        old=baseline_discount,
                        new=new_discount,
                    ))
                    product.baseline_discount = new_discount

        # === Обновляем текущие значения в любом случае ===
        if new_price is not None:
            product.current_price = new_price
        if new_old_price is not None:
            product.old_price = new_old_price
        if new_discount is not None:
            product.discount = new_discount
        if new_stock is not None:
            product.stock = new_stock
        if new_rating is not None:
            product.rating = new_rating

        product.last_checked_at = now

        # Обновляем title/url если пришли
        if item.get("name") or item.get("title"):
            product.title = item.get("name") or item.get("title")
        if item.get("product_url"):
            product.url = item.get("product_url")

        # === Сохраняем историю цен ===
        if new_price is not None:
            history = PriceHistory(
                product_id=product.id,
                price=new_price,
                old_price=new_old_price,
                discount=new_discount,
                stock=new_stock,
                rating=new_rating,
                checked_at=now,
            )
            session.add(history)

        results.append(ChangeResult(
            product=product,
            is_new=False,
            is_stable=product.is_stable,
            just_stabilized=just_stabilized,
            changes=changes,
        ))

    return results