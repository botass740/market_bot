# bot/services/product_manager.py

from __future__ import annotations

import asyncio
import csv
import logging
import os
from pathlib import Path
from typing import Iterable, TYPE_CHECKING

import aiohttp
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from bot.db.models import Platform, PlatformCode, Product

if TYPE_CHECKING:
    from bot.services.settings_manager import SettingsManager

log = logging.getLogger(__name__)


class ProductManager:
    """Управление списком товаров для мониторинга."""
    
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings_manager: "SettingsManager | None" = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings_manager = settings_manager

    def set_settings_manager(self, manager: "SettingsManager") -> None:
        """Устанавливает менеджер настроек."""
        self._settings_manager = manager

    async def _get_categories_for_refill(self) -> list[str]:
        """
        Получает категории для refill.
        Приоритет: БД -> ENV(REFILL_CATEGORIES) -> ENV(WB_CATEGORIES, fallback) -> дефолт.
        """
        # 1. Пробуем из БД
        if self._settings_manager:
            try:
                from bot.db.models.settings import BotSettings
                categories = await self._settings_manager.get_list(BotSettings.KEY_CATEGORIES)
                if categories:
                    log.info(f"Using categories from DB: {len(categories)} items")
                    return categories
            except Exception as e:
                log.warning(f"Failed to get categories from DB: {e}")

        # 2. Пробуем из ENV (универсальный список для всех площадок)
        env_categories = os.getenv("REFILL_CATEGORIES", "").strip()
        if not env_categories:
            # обратная совместимость со старым именем
            env_categories = os.getenv("WB_CATEGORIES", "").strip()

        if env_categories:
            categories = [q.strip() for q in env_categories.split(",") if q.strip()]
            if categories:
                log.info(f"Using categories from ENV: {len(categories)} items")
                return categories

        # 3. Дефолтный список
        default = [
            "смартфон", "ноутбук", "наушники", "планшет", "телевизор",
            "платье", "кроссовки", "футболка", "джинсы", "куртка",
            "сумка", "часы", "парфюм", "косметика",
            "пылесос", "микроволновка", "чайник", "холодильник",
            "видеорегистратор", "автокресло", "автомагнитола",
        ]
        log.info(f"Using default categories: {len(default)} items")
        return default

    async def get_refill_categories(self) -> list[str]:
        """Публичный доступ к списку категорий/тем для добора базы."""
        return await self._get_categories_for_refill()

    async def add_products(
        self,
        platform: PlatformCode,
        external_ids: Iterable[str],
    ) -> tuple[int, int]:
        """Добавляет товары в мониторинг."""
        external_ids = [str(eid).strip() for eid in external_ids if str(eid).strip()]
        
        if not external_ids:
            return 0, 0
        
        async with self._session_factory() as session:
            platform_obj = await self._get_or_create_platform(session, platform)
            
            stmt = select(Product.external_id).where(
                Product.platform_id == platform_obj.id,
                Product.external_id.in_(external_ids),
            )
            result = await session.execute(stmt)
            existing = {row[0] for row in result.fetchall()}
            
            added = 0
            for eid in external_ids:
                if eid in existing:
                    continue
                
                product = Product(
                    platform_id=platform_obj.id,
                    external_id=eid,
                    title=f"Товар {eid}",
                )
                session.add(product)
                added += 1
            
            await session.commit()
            
            skipped = len(external_ids) - added
            log.info(f"Added {added} products, skipped {skipped} (already exist)")
            
            return added, skipped

    async def remove_products(
        self,
        platform: PlatformCode,
        external_ids: Iterable[str],
    ) -> int:
        """Удаляет товары из мониторинга."""
        external_ids = [str(eid).strip() for eid in external_ids if str(eid).strip()]
        
        if not external_ids:
            return 0
        
        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, platform)
            if not platform_obj:
                return 0
            
            stmt = delete(Product).where(
                Product.platform_id == platform_obj.id,
                Product.external_id.in_(external_ids),
            )
            result = await session.execute(stmt)
            await session.commit()
            
            deleted = result.rowcount
            log.info(f"Removed {deleted} products")
            
            return deleted

    async def get_product_ids(self, platform: PlatformCode) -> list[str]:
        """Возвращает список артикулов для мониторинга."""
        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, platform)
            if not platform_obj:
                return []
            
            stmt = select(Product.external_id).where(
                Product.platform_id == platform_obj.id,
            )
            result = await session.execute(stmt)
            
            return [row[0] for row in result.fetchall()]

    async def get_product_count(self, platform: PlatformCode) -> int:
        """Возвращает количество товаров."""
        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, platform)
            if not platform_obj:
                return 0
            
            stmt = select(func.count(Product.id)).where(
                Product.platform_id == platform_obj.id,
            )
            result = await session.execute(stmt)
            
            return result.scalar() or 0

    async def refill_products(
        self,
        platform: PlatformCode,
        target_count: int = 3000,
        queries: list[str] | None = None,
    ) -> tuple[int, int]:
        """Добирает товары до целевого количества."""
        current_count = await self.get_product_count(platform)
        
        if current_count >= target_count:
            log.info(f"No refill needed: {current_count} >= {target_count}")
            return 0, current_count
        
        need = target_count - current_count
        log.info(f"Need to add {need} products (current: {current_count}, target: {target_count})")
        
        if platform != PlatformCode.WB:
            log.warning(f"Refill not implemented for {platform}")
            return 0, current_count
        
        # Категории для парсинга
        if not queries:
            queries = await self._get_categories_for_refill()
        
        from bot.services.catalog_parser import CatalogParser
        
        parser = CatalogParser()
        
        existing_ids = set(await self.get_product_ids(platform))
        
        new_ids: list[str] = []
        products_per_query = (need // len(queries)) + 100
        
        for query in queries:
            if len(new_ids) >= need:
                break
                
            try:
                found_ids = await parser.search_products(query, max_products=products_per_query)
                
                for nm_id in found_ids:
                    str_id = str(nm_id)
                    if str_id not in existing_ids and str_id not in new_ids:
                        new_ids.append(str_id)
                        
                        if len(new_ids) >= need:
                            break
                
                log.info(f"Query '{query}': found {len(found_ids)}, new unique: {len(new_ids)}/{need}")
                
            except Exception as e:
                log.error(f"Error searching '{query}': {e}")
                continue
        
        if new_ids:
            added, _ = await self.add_products(platform, new_ids[:need])
            new_count = await self.get_product_count(platform)
            log.info(f"Refill complete: added {added}, total: {new_count}")
            return added, new_count
        
        return 0, current_count

    async def cleanup_dead_products(
        self,
        platform: PlatformCode,
        batch_size: int = 100,
    ) -> tuple[int, list[str]]:
        """Проверяет все товары и удаляет мёртвые."""
        if platform != PlatformCode.WB:
            log.warning(f"Cleanup not implemented for {platform}")
            return 0, []
        
        all_ids = await self.get_product_ids(platform)
        log.info(f"Checking {len(all_ids)} products for dead ones...")
        
        dead_ids: list[str] = []
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "image/webp,image/*,*/*;q=0.8",
            "Referer": "https://www.wildberries.ru/",
        }
        
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            for i in range(0, len(all_ids), batch_size):
                batch = all_ids[i:i + batch_size]
                
                for external_id in batch:
                    try:
                        nm_id = int(external_id)
                        vol = nm_id // 100_000
                        part = nm_id // 1_000
                        basket = self._get_basket_number(vol)
                        
                        url = f"https://basket-{basket:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/big/1.webp"
                        
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                dead_ids.append(external_id)
                                
                    except Exception as e:
                        log.warning(f"Error checking {external_id}: {e}")
                
                checked = min(i + batch_size, len(all_ids))
                if checked % 500 == 0 or checked == len(all_ids):
                    log.info(f"Checked {checked}/{len(all_ids)}, dead found: {len(dead_ids)}")
                
                await asyncio.sleep(0.1)
        
        if dead_ids:
            removed = await self.remove_products(platform, dead_ids)
            log.info(f"Cleanup complete: removed {removed} dead products")
            return removed, dead_ids
        
        log.info("Cleanup complete: no dead products found")
        return 0, []

    

    async def import_from_csv(
        self,
        platform: PlatformCode,
        file_path: str | Path,
        column: str = "article",
    ) -> tuple[int, int]:
        """Импортирует артикулы из CSV файла."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        external_ids = []
        
        with open(file_path, "r", encoding="utf-8-sig") as f:
            sample = f.read(1024)
            f.seek(0)
            
            delimiter = ";" if ";" in sample else ("\t" if "\t" in sample else ",")
            reader = csv.DictReader(f, delimiter=delimiter)
            
            fieldnames_lower = {fn.lower(): fn for fn in (reader.fieldnames or [])}
            
            actual_column = None
            for col_name in [column, "article", "артикул", "nm_id", "nmid", "sku", "id"]:
                if col_name.lower() in fieldnames_lower:
                    actual_column = fieldnames_lower[col_name.lower()]
                    break
            
            if not actual_column:
                actual_column = reader.fieldnames[0] if reader.fieldnames else None
            
            if not actual_column:
                raise ValueError("Cannot find article column in CSV")
            
            for row in reader:
                value = row.get(actual_column, "").strip()
                if value and value.isdigit():
                    external_ids.append(value)
        
        return await self.add_products(platform, external_ids)

    async def import_from_text(self, platform: PlatformCode, text: str) -> tuple[int, int]:
        """Импортирует артикулы из текста."""
        import re
        external_ids = re.findall(r'\d+', text)
        external_ids = [eid for eid in external_ids if len(eid) >= 6]
        return await self.add_products(platform, external_ids)

    async def clear_all(self, platform: PlatformCode) -> int:
        """Удаляет все товары платформы."""
        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, platform)
            if not platform_obj:
                return 0
            
            stmt = delete(Product).where(Product.platform_id == platform_obj.id)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def trim_to_target(self, platform: PlatformCode, target: int = 3000) -> int:
        """
        Оставляет только target товаров платформы (по smallest Product.id), лишнее удаляет.
        Возвращает количество удалённых.
        """
        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, platform)
            if not platform_obj:
                return 0

            stmt = (
                select(Product.id)
                .where(Product.platform_id == platform_obj.id)
                .order_by(Product.id.asc())
            )
            ids = (await session.execute(stmt)).scalars().all()

            if len(ids) <= target:
                return 0

            to_delete = ids[target:]
            res = await session.execute(delete(Product).where(Product.id.in_(to_delete)))
            await session.commit()
            return res.rowcount or 0

    def _get_basket_number(self, vol: int) -> int:
        """Определяет номер basket по vol."""
        ranges = [
            (143, 1), (287, 2), (431, 3), (719, 4), (1007, 5),
            (1061, 6), (1115, 7), (1169, 8), (1313, 9), (1601, 10),
            (1655, 11), (1919, 12), (2045, 13), (2189, 14), (2405, 15),
            (2621, 16), (2837, 17), (3053, 18), (3269, 19), (3485, 20),
            (3701, 21), (3917, 22), (4133, 23), (4349, 24), (4565, 25),
            (4899, 26), (5399, 27), (5599, 28), (5859, 29), (6259, 30),
            (6459, 31), (6659, 32), (6859, 33), (7059, 34), (7259, 35),
            (7459, 36), (7659, 37), (7859, 38), (8059, 39), (8259, 40),
        ]
        for max_vol, basket in ranges:
            if vol <= max_vol:
                return basket
        return 41

    async def _get_or_create_platform(self, session: AsyncSession, code: PlatformCode) -> Platform:
        """Получает или создаёт платформу."""
        stmt = select(Platform).where(Platform.code == code)
        result = await session.execute(stmt)
        platform = result.scalar_one_or_none()
        
        if platform:
            return platform
        
        name_map = {
            PlatformCode.WB: "Wildberries",
            PlatformCode.OZON: "Ozon",
            PlatformCode.DM: "Detmir",
        }
        
        platform = Platform(code=code, name=name_map.get(code, code.value))
        session.add(platform)
        await session.flush()
        return platform

    async def _get_platform(self, session: AsyncSession, code: PlatformCode) -> Platform | None:
        """Получает платформу."""
        stmt = select(Platform).where(Platform.code == code)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def cleanup_dead_products_ozon(
        self,
        *,
        dead_after: int = 3,
        batch_size: int = 500,
    ) -> tuple[int, list[str]]:
        """
        Удаляет OZON товары, которые dead_after циклов подряд получили 404/410.
        """
        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, PlatformCode.OZON)
            if not platform_obj:
                return 0, []

            stmt = (
                select(Product.external_id)
                .where(
                    Product.platform_id == platform_obj.id,
                    Product.dead_check_fail_count >= dead_after,
                    Product.last_dead_reason.in_(["404", "410"]),
                )
                .limit(batch_size)
            )

            dead_ids = (await session.execute(stmt)).scalars().all()
            if not dead_ids:
                return 0, []

            res = await session.execute(
                delete(Product).where(
                    Product.platform_id == platform_obj.id,
                    Product.external_id.in_(dead_ids),
                )
            )
            await session.commit()

            return res.rowcount or 0, list(dead_ids)

    async def remove_oldest_products(self, platform: PlatformCode, count: int) -> int:
        """Удаляет count самых старых товаров платформы (по Product.id asc)."""
        if count <= 0:
            return 0

        async with self._session_factory() as session:
            platform_obj = await self._get_platform(session, platform)
            if not platform_obj:
                return 0

            from sqlalchemy import select, delete

            ids = (await session.execute(
                select(Product.id)
                .where(Product.platform_id == platform_obj.id)
                .order_by(Product.id.asc())
                .limit(count)
            )).scalars().all()

            if not ids:
                return 0

            res = await session.execute(delete(Product).where(Product.id.in_(ids)))
            await session.commit()
            return res.rowcount or 0
