# bot/pipeline/runner.py

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from bot.config import FilteringThresholds
from bot.db.models import PlatformCode
from bot.db.models.settings import BotSettings
from bot.db.services.change_detection import ChangeResult, detect_and_save_changes
from bot.filtering.filters import FilterService
from bot.parsers.base import BaseParser
from bot.posting.poster import PostingService, ProductUnavailableError
from bot.services.settings_manager import SettingsManager
from datetime import datetime, timedelta
from pathlib import Path

YIELD_EVERY_N_ITEMS = int(os.getenv("YIELD_EVERY_N_ITEMS", "20"))

# Автоматическое удаление мёртвых товаров и добор
AUTO_CLEANUP_ENABLED = os.getenv("AUTO_CLEANUP_ENABLED", "true").lower() in ("true", "1", "yes")
TARGET_PRODUCT_COUNT = int(os.getenv("TARGET_PRODUCT_COUNT", "3000"))
DM_ROTATION_TIMESTAMP_FILE = Path(".last_rotation_detmir")
DM_ROTATION_ENABLED = os.getenv("DM_ROTATION_ENABLED", "true").lower() in ("true", "1", "yes")
DM_ROTATION_DAYS = int(os.getenv("DM_ROTATION_DAYS", "7"))
DM_ROTATION_FRACTION = float(os.getenv("DM_ROTATION_FRACTION", "0.2"))
DM_ROTATION_MAX_ATTEMPTS = int(os.getenv("DM_ROTATION_MAX_ATTEMPTS", "3"))
WB_ROTATION_TIMESTAMP_FILE = Path(".last_rotation_wb")
WB_ROTATION_ENABLED = os.getenv("WB_ROTATION_ENABLED", "true").lower() in ("true", "1", "yes")
WB_ROTATION_DAYS = int(os.getenv("WB_ROTATION_DAYS", "7"))
WB_ROTATION_FRACTION = float(os.getenv("WB_ROTATION_FRACTION", "0.2"))
OZON_ROTATION_TIMESTAMP_FILE = Path(".last_rotation_ozon")
OZON_ROTATION_ENABLED = os.getenv("OZON_ROTATION_ENABLED", "true").lower() in ("true", "1", "yes")
OZON_ROTATION_DAYS = int(os.getenv("OZON_ROTATION_DAYS", "7"))
OZON_ROTATION_FRACTION = float(os.getenv("OZON_ROTATION_FRACTION", "0.2"))

# Размер батча для парсинга
BATCH_SIZE = int(os.getenv("PARSE_BATCH_SIZE", "50"))

# Порог "мягкой смерти" — удаляем товар после N неудачных загрузок картинки подряд
NO_IMAGE_FAIL_THRESHOLD = int(os.getenv("NO_IMAGE_FAIL_THRESHOLD", "3"))

def _dm_rotation_needed() -> bool:
    if not DM_ROTATION_TIMESTAMP_FILE.exists():
        return True
    try:
        ts = float(DM_ROTATION_TIMESTAMP_FILE.read_text().strip())
        last = datetime.fromtimestamp(ts)
        return (datetime.now() - last) >= timedelta(days=DM_ROTATION_DAYS)
    except Exception:
        return True

def _wb_rotation_needed() -> bool:
    if not WB_ROTATION_TIMESTAMP_FILE.exists():
        return True
    try:
        ts = float(WB_ROTATION_TIMESTAMP_FILE.read_text().strip())
        last = datetime.fromtimestamp(ts)
        return (datetime.now() - last) >= timedelta(days=WB_ROTATION_DAYS)
    except Exception:
        return True

def _ozon_rotation_needed() -> bool:
    if not OZON_ROTATION_TIMESTAMP_FILE.exists():
        return True
    try:
        ts = float(OZON_ROTATION_TIMESTAMP_FILE.read_text().strip())
        last = datetime.fromtimestamp(ts)
        return (datetime.now() - last) >= timedelta(days=OZON_ROTATION_DAYS)
    except Exception:
        return True


def _ozon_mark_rotation_done() -> None:
    try:
        OZON_ROTATION_TIMESTAMP_FILE.write_text(str(datetime.now().timestamp()))
    except Exception:
        pass


def _wb_mark_rotation_done() -> None:
    try:
        WB_ROTATION_TIMESTAMP_FILE.write_text(str(datetime.now().timestamp()))
    except Exception:
        pass

def _dm_mark_rotation_done() -> None:
    try:
        DM_ROTATION_TIMESTAMP_FILE.write_text(str(datetime.now().timestamp()))
    except Exception:
        # не критично, просто потеряем метку
        pass

class PipelineRunner:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        filter_service: FilterService,
        posting_service: PostingService,
        thresholds: FilteringThresholds | None = None,
        product_manager=None,
        settings_manager: SettingsManager | None = None,
    ) -> None:
        self._log = logging.getLogger(self.__class__.__name__)
        self._session_factory = session_factory
        self._filter = filter_service
        self._poster = posting_service
        self._product_manager = product_manager
        self._settings_manager = settings_manager

        # Пороги для публикации (начальные значения из конфига)
        self._min_price_drop = thresholds.min_price_drop_percent if thresholds else 1.0
        self._min_discount_increase = thresholds.min_discount_increase if thresholds else 5.0

        self._log.info(
            "Publishing thresholds: price_drop>=%.1f%%, discount_increase>=%.1f%%",
            self._min_price_drop,
            self._min_discount_increase,
        )

    async def run_platform(self, *, platform: PlatformCode, parser: BaseParser) -> None:
        self._log.info("Pipeline started: %s", platform.value)

        # Загружаем актуальные пороги из БД
        if self._settings_manager:
            self._min_price_drop = await self._settings_manager.get_float(BotSettings.KEY_MIN_PRICE_DROP)
            self._min_discount_increase = await self._settings_manager.get_float(BotSettings.KEY_MIN_DISCOUNT_INCREASE)
            self._log.debug(
                "Loaded thresholds from DB: price_drop=%.1f%%, discount_increase=%.1f%%",
                self._min_price_drop,
                self._min_discount_increase,
            )

        try:
            raw_items = await parser.fetch_products()
        except NotImplementedError:
            self._log.warning("fetch_products is not implemented for %s", platform.value)
            return
        except Exception:
            self._log.exception("Failed to fetch products for %s", platform.value)
            return

        raw_list = list(raw_items)

        # Парсинг: batch или по одному
        parsed = await self._parse_products(parser, raw_list, platform)

        filtered = await self._filter.filter_products_async(parsed)
        self._log.info(
            "Pipeline %s: fetched=%s parsed=%s filtered=%s",
            platform.value,
            len(raw_list),
            len(parsed),
            len(filtered),
        )

        dead_products: list[str] = []

        async with self._session_factory() as session:
            try:
                changes = await detect_and_save_changes(session, platform_code=platform, items=filtered)

                # Логируем статистику стабильности
                stable_count = sum(1 for ch in changes if ch.is_stable)
                unstable_count = sum(1 for ch in changes if not ch.is_stable and not ch.is_new)
                just_stabilized_count = sum(1 for ch in changes if ch.just_stabilized)
                self._log.info(
                    "Stability stats: stable=%d, unstable=%d, just_stabilized=%d",
                    stable_count,
                    unstable_count,
                    just_stabilized_count,
                )

                to_publish = self._select_for_publish(changes, filtered)

                posted = 0
                skipped = 0

                for item in to_publish:
                    try:
                        ok = await self._poster.post_product(item)
                        
                        # Если успешно опубликовано и картинка загрузилась — сбрасываем счётчик
                        if ok and item.get("_image_ok"):
                            await self._reset_no_image_counter(session, item.get("external_id"), platform)
                            
                    except ProductUnavailableError as e:
                        self._log.warning("Skipped unavailable: %s", e)
                        skipped += 1
                        
                        # Увеличиваем счётчик неудачных загрузок картинки
                        should_delete = await self._increment_no_image_counter(
                            session, e.external_id, platform
                        )
                        
                        if should_delete:
                            self._log.warning(
                                "Product %s has no image %d times in a row — marking as dead",
                                e.external_id, NO_IMAGE_FAIL_THRESHOLD
                            )
                            dead_products.append(e.external_id)
                        
                        continue

                    if not ok:
                        self._log.info("Posting rate limit reached")
                        break

                    posted += 1

                await session.commit()

                self._log.info(
                    "Pipeline finished: %s new=%s changed=%s posted=%s skipped=%s dead=%s",
                    platform.value,
                    sum(1 for ch in changes if ch.is_new),
                    sum(1 for ch in changes if ch.has_changes),
                    posted,
                    skipped,
                    len(dead_products),
                )

            except Exception:
                await session.rollback()
                self._log.exception("Pipeline DB step failed: %s", platform.value)
                return

        # После основного pipeline — удаляем мёртвых и добираем новых
        # Для OZON refill делаем внутри _parse_products (auto-refill), поэтому тут не вызываем _cleanup_and_refill
        if dead_products and AUTO_CLEANUP_ENABLED and self._product_manager and platform != PlatformCode.OZON:
            await self._cleanup_and_refill(platform, dead_products)

        # OZON: удаляем мёртвые (no-image), refill будет сделан auto-refill на следующем цикле
        elif dead_products and AUTO_CLEANUP_ENABLED and self._product_manager and platform == PlatformCode.OZON:
            try:
                removed = await self._product_manager.remove_products(platform, dead_products)
                self._log.info("OZON: removed %d dead products (no-image): %s", removed, dead_products)

                # === OZON: добираем сразу до TARGET_PRODUCT_COUNT ===
                try:
                    current = await self._product_manager.get_product_count(PlatformCode.OZON)
                except Exception:
                    self._log.exception("OZON: failed to get count after delete")
                    current = TARGET_PRODUCT_COUNT

                need = max(0, TARGET_PRODUCT_COUNT - current)
                if need > 0:
                    self._log.warning("OZON: immediate refill needed: %d (current=%d target=%d)", need, current, TARGET_PRODUCT_COUNT)

                    # Собираем кандидатов через COLLECT и добавляем только недостающее
                    try:
                        # parser у нас уже есть, используем тот же
                        # Берём общий список категорий/тем (из БД/ENV)
                        queries: list[str] = []
                        if self._product_manager and hasattr(self._product_manager, "get_refill_categories"):
                            queries = await self._product_manager.get_refill_categories()

                        # Собираем кандидатов равномерно по запросам (быстро, без прокрутки до 3000)
                        if queries and hasattr(parser, "collect_skus_by_queries"):
                            target_for_collect = min(300, max(need * 10, need + 30))
                            collected_ids = await parser.collect_skus_by_queries(queries, target=target_for_collect)
                        else:
                            # fallback: старый COLLECT если queries пустые или метода ещё нет
                            collected = await getattr(parser, "parse_products_batch")([])  # COLLECT
                            collected_ids = [str(x.get("external_id")) for x in collected if isinstance(x, dict)]
                            collected_ids = [x for x in collected_ids if x and x.isdigit()]

                        existing_ids = set(await self._product_manager.get_product_ids(PlatformCode.OZON))
                        new_ids: list[str] = []
                        for eid in collected_ids:
                            if eid in existing_ids:
                                continue
                            if eid in new_ids:
                                continue
                            new_ids.append(eid)
                            if len(new_ids) >= need:
                                break

                        if new_ids:
                            added, skipped = await self._product_manager.add_products(PlatformCode.OZON, new_ids)
                            self._log.info("OZON immediate refill: added=%d skipped=%d", added, skipped)

                        removed_extra = await self._product_manager.trim_to_target(PlatformCode.OZON, TARGET_PRODUCT_COUNT)
                        if removed_extra:
                            self._log.info("OZON immediate refill: trimmed extra removed=%d", removed_extra)

                    except Exception:
                        self._log.exception("OZON immediate refill failed")

            except Exception:
                self._log.exception("OZON: failed to remove dead products")


    async def _parse_products(
        self,
        parser: BaseParser,
        raw_list: list[Any],
        platform: PlatformCode,
    ) -> list[dict[str, Any]]:
        """Парсит товары."""
        
        # === OZON ===
        if platform == PlatformCode.OZON and hasattr(parser, "parse_products_batch"):

            # 1) Если raw_list не пустой — обычный MONITOR
            if raw_list:
                self._log.info("OZON: MONITOR mode (%d products from DB)", len(raw_list))
                try:
                    # === OZON ROTATION (lazy, 20% weekly) ===
                    if self._product_manager and OZON_ROTATION_ENABLED and _ozon_rotation_needed():
                        try:
                            rotate_count = int(TARGET_PRODUCT_COUNT * OZON_ROTATION_FRACTION)
                            rotate_count = max(1, min(rotate_count, TARGET_PRODUCT_COUNT))

                            self._log.warning(
                                "OZON rotation needed: replacing %d/%d (%.0f%%)",
                                rotate_count, TARGET_PRODUCT_COUNT, OZON_ROTATION_FRACTION * 100
                            )

                            removed = await self._product_manager.remove_oldest_products(PlatformCode.OZON, rotate_count)
                            self._log.info("OZON rotation: removed %d products", removed)

                            # добор до 3000 через текущий OZON auto-refill механизм (COLLECT)
                            # 1) берём категории (queries) из БД
                            collect_queries: list[str] = []
                            try:
                                if hasattr(self._product_manager, "get_refill_categories"):
                                    collect_queries = await self._product_manager.get_refill_categories()
                                    self._log.info("OZON rotation: using %d categories from DB", len(collect_queries))
                            except Exception:
                                self._log.exception("OZON rotation: failed to load categories from DB")

                            # 2) сколько не хватает сейчас
                            current = await self._product_manager.get_product_count(PlatformCode.OZON)
                            need_now = max(0, TARGET_PRODUCT_COUNT - current)

                            if need_now > 0:
                                # собираем кандидатов с запасом
                                target_for_collect = min(3000, max(need_now * 10, need_now + 100))

                                collected_ids: list[str] = []
                                collected = await parser.parse_products_batch([], collect_queries=collect_queries)
                                collected_ids = [str(x.get("external_id")) for x in collected if isinstance(x, dict)]
                                collected_ids = [x for x in collected_ids if x and x.isdigit()]

                                existing_ids = set(await self._product_manager.get_product_ids(PlatformCode.OZON))
                                new_ids: list[str] = []
                                for eid in collected_ids:
                                    if eid in existing_ids or eid in new_ids:
                                        continue
                                    new_ids.append(eid)
                                    if len(new_ids) >= need_now:
                                        break

                                if new_ids:
                                    added, skipped = await self._product_manager.add_products(PlatformCode.OZON, new_ids)
                                    self._log.info("OZON rotation refill: added=%d skipped=%d", added, skipped)

                                removed_extra = await self._product_manager.trim_to_target(PlatformCode.OZON, TARGET_PRODUCT_COUNT)
                                if removed_extra:
                                    self._log.info("OZON rotation: trimmed extra removed=%d", removed_extra)

                            # обновляем raw_list после ротации
                            raw_list = await self._product_manager.get_product_ids(PlatformCode.OZON)
                            self._log.info("OZON: refreshed ids after rotation: %d", len(raw_list))

                            _ozon_mark_rotation_done()
                            self._log.info("OZON rotation: mark done")

                        except Exception:
                            self._log.exception("OZON rotation failed")

                    results = await parser.parse_products_batch(raw_list)
                    self._log.info("OZON monitor returned %d items", len(results) if results else 0)
                    # === OZON AUTO-REFILL до TARGET_PRODUCT_COUNT ===
                    # Если после удаления "мёртвых" стало меньше 3000 — добираем недостающее через COLLECT.
                    if self._product_manager:
                        try:
                            db_count = await self._product_manager.get_product_count(PlatformCode.OZON)
                        except Exception:
                            self._log.exception("OZON: failed to get count for auto-refill")
                            db_count = TARGET_PRODUCT_COUNT

                        if db_count < TARGET_PRODUCT_COUNT:
                            need = TARGET_PRODUCT_COUNT - db_count
                            self._log.warning("OZON: auto-refill needed: %d (current=%d target=%d)", need, db_count, TARGET_PRODUCT_COUNT)

                            try:
                                # Берём общий список категорий/тем (из БД/ENV)
                                queries: list[str] = []
                                if self._product_manager and hasattr(self._product_manager, "get_refill_categories"):
                                    queries = await self._product_manager.get_refill_categories()

                                # Собираем кандидатов равномерно по запросам
                                if queries and hasattr(parser, "collect_skus_by_queries"):
                                    # небольшой запас, но без лишней нагрузки при need=1..3
                                    target_for_collect = min(300, max(need * 10, need + 30))
                                    collected_ids = await parser.collect_skus_by_queries(queries, target=target_for_collect)
                                else:
                                    # fallback: старый COLLECT если queries пустые или метод ещё не добавлен
                                    collected = await parser.parse_products_batch([])  # COLLECT
                                    collected_ids = [str(x.get("external_id")) for x in collected if isinstance(x, dict)]
                                    collected_ids = [x for x in collected_ids if x and x.isdigit()]

                                existing_ids = set(await self._product_manager.get_product_ids(PlatformCode.OZON))
                                new_ids: list[str] = []
                                for eid in collected_ids:
                                    if eid in existing_ids:
                                        continue
                                    if eid in new_ids:
                                        continue
                                    new_ids.append(eid)
                                    if len(new_ids) >= need:
                                        break

                                if new_ids:
                                    added, skipped = await self._product_manager.add_products(PlatformCode.OZON, new_ids)
                                    self._log.info("OZON auto-refill: added=%d skipped=%d", added, skipped)

                                removed = await self._product_manager.trim_to_target(PlatformCode.OZON, TARGET_PRODUCT_COUNT)
                                if removed:
                                    self._log.info("OZON auto-refill: trimmed extra removed=%d", removed)

                            except Exception:
                                self._log.exception("OZON auto-refill failed")
                    return results if isinstance(results, list) else []
                except Exception:
                    self._log.exception("OZON monitor failed")
                    return []

            # 2) raw_list пустой — но это может быть из-за "пустого парсера".
            #    Проверяем БД и если там есть товары — форсим MONITOR.
            db_count = 0
            if self._product_manager:
                try:
                    db_count = await self._product_manager.get_product_count(PlatformCode.OZON)
                except Exception:
                    self._log.exception("OZON: failed to get product count from DB")
                    db_count = 0

            if db_count > 0 and self._product_manager:
                self._log.warning(
                    "OZON: raw_list empty, but DB has %d products -> forcing MONITOR from DB",
                    db_count,
                )
                try:
                    ids = await self._product_manager.get_product_ids(PlatformCode.OZON)
                    results = await parser.parse_products_batch(ids)
                    self._log.info("OZON monitor returned %d items", len(results) if results else 0)
                    return results if isinstance(results, list) else []
                except Exception:
                    self._log.exception("OZON forced MONITOR from DB failed")
                    return []

            # 3) БД реально пустая — делаем COLLECT
            self._log.info("OZON: COLLECT mode (DB empty)")

            try:
                # Получаем категории из БД для равномерного сбора
                collect_queries = []
                if self._product_manager and hasattr(self._product_manager, "get_refill_categories"):
                    collect_queries = await self._product_manager.get_refill_categories()
                    self._log.info("OZON COLLECT: using %d categories from DB", len(collect_queries))
                
                results = await parser.parse_products_batch([], collect_queries=collect_queries)
                ids: list[str] = []
                if results:
                    ids = [str(x.get("external_id")) for x in results if isinstance(x, dict)]
                    ids = [x for x in ids if x and x.isdigit()]
                    ids = ids[:TARGET_PRODUCT_COUNT]  # ровно 3000

                if self._product_manager and ids:
                    added, skipped = await self._product_manager.add_products(PlatformCode.OZON, ids)
                    self._log.info("OZON COLLECT: saved to DB added=%d skipped=%d", added, skipped)

                    # Приводим базу к ровно TARGET_PRODUCT_COUNT (твой Шаг 2 уже сделал метод trim_to_target)
                    removed = await self._product_manager.trim_to_target(PlatformCode.OZON, TARGET_PRODUCT_COUNT)
                    if removed:
                        self._log.info("OZON: trimmed extra products removed=%d", removed)

                self._log.info("OZON collect returned %d items", len(results) if results else 0)

                # Сразу запускаем MONITOR в этом же запуске (по ровно 3000 ids)
                if ids:
                    self._log.info("OZON: switching to MONITOR right after COLLECT (%d products)", len(ids))
                    monitor_results = await parser.parse_products_batch(ids)
                    self._log.info(
                        "OZON monitor after collect returned %d items",
                        len(monitor_results) if monitor_results else 0,
                    )
                    return monitor_results if isinstance(monitor_results, list) else []

                return []
            except Exception:
                self._log.exception("OZON collect failed")
                return []

        parsed: list[dict[str, Any]] = []

                # === DETMIR ===
        if platform == PlatformCode.DM and hasattr(parser, "parse_products_batch"):
            # 1) Если есть товары в БД — обычный MONITOR (как сейчас)
            if raw_list:
                self._log.info("DETMIR: MONITOR mode (%d products from DB)", len(raw_list))
                try:
                    # === DETMIR ROTATION (lazy, 20% weekly) ===
                    if DM_ROTATION_ENABLED and self._product_manager:
                        try:
                            if _dm_rotation_needed():
                                rotate_count = int(TARGET_PRODUCT_COUNT * DM_ROTATION_FRACTION)
                                rotate_count = max(1, min(rotate_count, TARGET_PRODUCT_COUNT))

                                self._log.warning(
                                    "DETMIR rotation needed: replacing %d/%d (%.0f%%)",
                                    rotate_count, TARGET_PRODUCT_COUNT, DM_ROTATION_FRACTION * 100
                                )

                                removed = await self._product_manager.remove_oldest_products(PlatformCode.DM, rotate_count)
                                self._log.info("DETMIR rotation: removed %d products", removed)

                                # добор до TARGET_PRODUCT_COUNT через COLLECT (учитывает only_in_stock внутри DetmirParser)
                                collect_slugs: list[str] | None = None
                                try:
                                    if self._settings_manager:
                                        from bot.db.models.settings import BotSettings
                                        collect_slugs = await self._settings_manager.get_list(BotSettings.KEY_DETMIR_SLUGS)
                                        if collect_slugs:
                                            self._log.info("DETMIR rotation: using %d slugs from DB", len(collect_slugs))
                                except Exception:
                                    self._log.exception("DETMIR rotation: failed to load slugs from DB")

                                for attempt in range(1, DM_ROTATION_MAX_ATTEMPTS + 1):
                                    current = await self._product_manager.get_product_count(PlatformCode.DM)
                                    need_now = max(0, TARGET_PRODUCT_COUNT - current)
                                    if need_now <= 0:
                                        break

                                    cap = max(500 * attempt, need_now)
                                    cap = min(cap, 6000)
                                    target_for_collect = min(cap, max(need_now * 20, need_now + 100))

                                    self._log.warning(
                                        "DETMIR rotation refill attempt %d/%d: need=%d collect_target=%d",
                                        attempt, DM_ROTATION_MAX_ATTEMPTS, need_now, target_for_collect
                                    )

                                    collected = await parser.parse_products_batch(
                                        [],
                                        collect_slugs=collect_slugs,
                                        collect_target=target_for_collect,
                                    )

                                    collected_ids = [str(x.get("external_id")) for x in collected if isinstance(x, dict)]
                                    collected_ids = [x for x in collected_ids if x and x.isdigit()]

                                    existing_ids = set(await self._product_manager.get_product_ids(PlatformCode.DM))
                                    new_ids: list[str] = []
                                    for eid in collected_ids:
                                        if eid in existing_ids or eid in new_ids:
                                            continue
                                        new_ids.append(eid)
                                        if len(new_ids) >= need_now:
                                            break

                                    if new_ids:
                                        added, skipped = await self._product_manager.add_products(PlatformCode.DM, new_ids)
                                        self._log.info("DETMIR rotation refill: added=%d skipped=%d", added, skipped)

                                    final_count = await self._product_manager.get_product_count(PlatformCode.DM)

                                    if final_count >= TARGET_PRODUCT_COUNT:
                                        self._log.info("DETMIR rotation done: current=%d target=%d", final_count, TARGET_PRODUCT_COUNT)
                                        _dm_mark_rotation_done()
                                    else:
                                        self._log.warning(
                                            "DETMIR rotation incomplete: current=%d target=%d. Will continue refill in next cycles.",
                                            final_count, TARGET_PRODUCT_COUNT
                                        )
                                        # метку НЕ ставим, чтобы ротация считалась незавершённой и повторилась/додобрала

                                    raw_list = await self._product_manager.get_product_ids(PlatformCode.DM)
                                    self._log.info("DETMIR: refreshed ids after rotation: %d", len(raw_list))

                        except Exception:
                            self._log.exception("DETMIR rotation failed")
                    results = await parser.parse_products_batch(raw_list)
                    self._log.info("DETMIR monitor returned %d items", len(results) if results else 0)

                    # === DETMIR AUTO-REFILL до TARGET_PRODUCT_COUNT ===
                    if self._product_manager:
                        try:
                            db_count = await self._product_manager.get_product_count(PlatformCode.DM)
                        except Exception:
                            self._log.exception("DETMIR: failed to get product count for auto-refill")
                            db_count = TARGET_PRODUCT_COUNT

                        if db_count < TARGET_PRODUCT_COUNT:
                            need = TARGET_PRODUCT_COUNT - db_count
                            self._log.warning(
                                "DETMIR: auto-refill needed: %d (current=%d target=%d)",
                                need, db_count, TARGET_PRODUCT_COUNT
                            )

                            # Берём slugs из БД настроек
                            collect_slugs: list[str] | None = None
                            try:
                                if self._settings_manager:
                                    from bot.db.models.settings import BotSettings
                                    collect_slugs = await self._settings_manager.get_list(BotSettings.KEY_DETMIR_SLUGS)
                                    if collect_slugs:
                                        self._log.info("DETMIR auto-refill: using %d slugs from DB", len(collect_slugs))
                            except Exception:
                                self._log.exception("DETMIR auto-refill: failed to load slugs from DB")

                            # Собираем кандидатов (берём запас, чтобы компенсировать дубли)
                            max_attempts = int(os.getenv("DETMIR_REFILL_MAX_ATTEMPTS", "3"))

                            for attempt in range(1, max_attempts + 1):
                                try:
                                    # пересчитываем, сколько реально не хватает (после каждой попытки)
                                    current = await self._product_manager.get_product_count(PlatformCode.DM)
                                    need_now = max(0, TARGET_PRODUCT_COUNT - current)

                                    if need_now <= 0:
                                        self._log.info("DETMIR auto-refill: target reached (current=%d)", current)
                                        break

                                    # увеличиваем объём кандидатов от попытки к попытке: 500 -> 1000 -> 1500
                                    cap = max(500 * attempt, need_now)
                                    cap = min(cap, 6000)
                                    target_for_collect = min(cap, max(need_now * 20, need_now + 100))

                                    self._log.warning(
                                        "DETMIR auto-refill attempt %d/%d: need=%d, collect_target=%d",
                                        attempt, max_attempts, need_now, target_for_collect
                                    )

                                    collected = await parser.parse_products_batch(
                                        [],
                                        collect_slugs=collect_slugs,
                                        collect_target=target_for_collect,
                                    )

                                    collected_ids = [str(x.get("external_id")) for x in collected if isinstance(x, dict)]
                                    collected_ids = [x for x in collected_ids if x and x.isdigit()]

                                    existing_ids = set(await self._product_manager.get_product_ids(PlatformCode.DM))
                                    new_ids: list[str] = []
                                    for eid in collected_ids:
                                        if eid in existing_ids:
                                            continue
                                        if eid in new_ids:
                                            continue
                                        new_ids.append(eid)
                                        if len(new_ids) >= need_now:
                                            break

                                    if not new_ids:
                                        self._log.warning("DETMIR auto-refill: attempt %d -> 0 new ids, retrying", attempt)
                                        continue

                                    added, skipped = await self._product_manager.add_products(PlatformCode.DM, new_ids)
                                    self._log.info("DETMIR auto-refill: attempt %d -> added=%d skipped=%d", attempt, added, skipped)

                                    # если добавили мало, следующая попытка доберёт остаток
                                except Exception:
                                    self._log.exception("DETMIR auto-refill failed on attempt %d", attempt)
                                    break

                    return results if isinstance(results, list) else []
                except Exception:
                    self._log.exception("DETMIR monitor failed")
                    return []

            # 2) Если БД пустая — COLLECT и сразу MONITOR
            self._log.info("DETMIR: COLLECT mode (DB empty)")
            try:
                # Берём DM slugs из БД (если settings_manager подключен)
                collect_slugs: list[str] | None = None
                try:
                    if self._settings_manager:
                        from bot.db.models.settings import BotSettings
                        collect_slugs = await self._settings_manager.get_list(BotSettings.KEY_DETMIR_SLUGS)
                        if collect_slugs:
                            self._log.info("DETMIR COLLECT: using %d slugs from DB", len(collect_slugs))
                except Exception:
                    self._log.exception("DETMIR: failed to load slugs from DB")

                collected = await parser.parse_products_batch([], collect_slugs=collect_slugs)  # COLLECT

                collected_ids = [str(x.get("external_id")) for x in collected if isinstance(x, dict)]
                collected_ids = [x for x in collected_ids if x and x.isdigit()]
                collected_ids = collected_ids[:TARGET_PRODUCT_COUNT]

                if self._product_manager and collected_ids:
                    added, skipped = await self._product_manager.add_products(PlatformCode.DM, collected_ids)
                    self._log.info("DETMIR COLLECT: saved to DB added=%d skipped=%d", added, skipped)

                if collected_ids:
                    self._log.info(
                        "DETMIR: switching to MONITOR right after COLLECT (%d products)",
                        len(collected_ids),
                    )
                    results = await parser.parse_products_batch(collected_ids)
                    return results if isinstance(results, list) else []

                return []
            except Exception:
                self._log.exception("DETMIR collect failed")
                return []

        # === WB ===
        if platform == PlatformCode.WB and self._product_manager:
            # Ленивое обновление базы раз в N дней
            if WB_ROTATION_ENABLED and _wb_rotation_needed():
                try:
                    rotate_count = int(TARGET_PRODUCT_COUNT * WB_ROTATION_FRACTION)
                    rotate_count = max(1, min(rotate_count, TARGET_PRODUCT_COUNT))

                    self._log.warning(
                        "WB rotation needed: replacing %d/%d (%.0f%%)",
                        rotate_count, TARGET_PRODUCT_COUNT, WB_ROTATION_FRACTION * 100
                    )

                    removed = await self._product_manager.remove_oldest_products(PlatformCode.WB, rotate_count)
                    self._log.info("WB rotation: removed %d products", removed)

                    # Добираем обратно до TARGET_PRODUCT_COUNT штатным механизмом WB refill
                    added, total = await self._product_manager.refill_products(
                        PlatformCode.WB,
                        target_count=TARGET_PRODUCT_COUNT,
                    )
                    self._log.info("WB rotation refill: added=%d total=%d", added, total)

                    # На всякий случай приводим базу WB к ровно TARGET_PRODUCT_COUNT
                    removed_extra = await self._product_manager.trim_to_target(PlatformCode.WB, TARGET_PRODUCT_COUNT)
                    if removed_extra:
                        self._log.info("WB rotation: trimmed extra removed=%d", removed_extra)

                    # ВАЖНО: обновляем raw_list, чтобы мониторить уже обновлённую базу
                    raw_list = await self._product_manager.get_product_ids(PlatformCode.WB)
                    self._log.info("WB: refreshed ids after rotation: %d", len(raw_list))

                    _wb_mark_rotation_done()

                except Exception:
                    self._log.exception("WB rotation failed")
                    
        # === WB и другие: batch парсинг ===
        if hasattr(parser, "parse_products_batch") and callable(getattr(parser, "parse_products_batch")):
            self._log.info(
                "Using BATCH parsing: %d products, batch_size=%d",
                len(raw_list),
                BATCH_SIZE,
            )

            total_batches = (len(raw_list) + BATCH_SIZE - 1) // BATCH_SIZE

            for batch_num, i in enumerate(range(0, len(raw_list), BATCH_SIZE), start=1):
                batch = raw_list[i:i + BATCH_SIZE]

                try:
                    batch_ids = [int(x) for x in batch]
                except (TypeError, ValueError):
                    batch_ids = [str(x) for x in batch]

                try:
                    batch_results = await parser.parse_products_batch(batch_ids)
                    if isinstance(batch_results, list):
                        parsed.extend(batch_results)

                    self._log.debug(
                        "Batch %d/%d: requested=%d, got=%d",
                        batch_num,
                        total_batches,
                        len(batch_ids),
                        len(batch_results) if isinstance(batch_results, list) else 0,
                    )
                except Exception:
                    self._log.exception("Batch %d/%d parsing failed", batch_num, total_batches)

                # Yield каждые 5 батчей — даём боту обработать команды
                if batch_num % 5 == 0:
                    await asyncio.sleep(0)

                if i + BATCH_SIZE < len(raw_list):
                    await asyncio.sleep(0.3)

            self._log.info("Batch parsing complete: %d/%d products parsed", len(parsed), len(raw_list))
            return parsed

        # === Fallback: по одному ===
        self._log.info("Using SINGLE parsing: %d products", len(raw_list))
        for idx, raw in enumerate(raw_list):
            try:
                item = await parser.parse_product(raw)
            except Exception:
                self._log.exception("Failed to parse product #%d", idx)
                continue
            if isinstance(item, dict):
                parsed.append(item)

        return parsed

    async def _cleanup_and_refill(
        self,
        platform: PlatformCode,
        dead_products: list[str],
    ) -> None:
        """Удаляет мёртвые товары и добирает новые."""
        try:
            removed = await self._product_manager.remove_products(platform, dead_products)
            self._log.info("Removed %d dead products: %s", removed, dead_products)

            added, total = await self._product_manager.refill_products(
                platform,
                target_count=TARGET_PRODUCT_COUNT,
            )

            if added > 0:
                self._log.info("Refilled %d new products, total now: %d", added, total)

        except Exception:
            self._log.exception("Cleanup/refill failed")

    def _select_for_publish(
        self,
        changes: list[ChangeResult],
        filtered: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Выбирает товары для публикации."""
        by_external: dict[str, dict[str, Any]] = {}
        for item in filtered:
            ext = item.get("external_id")
            if ext is None:
                continue
            by_external[str(ext)] = item

        selected: list[dict[str, Any]] = []

        for ch in changes:
            # Пропускаем новые товары
            if ch.is_new:
                self._log.debug("Skipping new product: %s", ch.product.external_id)
                continue

            # Пропускаем нестабильные товары
            if not ch.is_stable:
                self._log.debug(
                    "Skipping unstable product: %s (parse_count=%d)",
                    ch.product.external_id,
                    ch.product.stable_parse_count,
                )
                continue

            # Пропускаем только что стабилизировавшиеся
            if ch.just_stabilized:
                self._log.debug(
                    "Skipping just-stabilized product: %s (baseline set)",
                    ch.product.external_id,
                )
                continue

            # Нет изменений — пропускаем
            if not ch.has_changes:
                continue

            publish_reason = self._get_publish_reason(ch)
            if not publish_reason:
                continue

            ext = ch.product.external_id
            item = by_external.get(ext)
            if item is None:
                continue

            item = item.copy()
            item["publish_reason"] = publish_reason

            self._log.info(
                "Selected for publish %s: %s",
                ext,
                ", ".join(f"{c.field}: {c.old} → {c.new}" for c in ch.changes),
            )
            selected.append(item)

        return selected

    def _get_publish_reason(self, ch: ChangeResult) -> str | None:
        """Проверяет изменения и возвращает причину публикации."""
        reasons: list[str] = []

        for change in ch.changes:
            # Цена упала
            if change.field == "price":
                try:
                    old_price = float(change.old)
                    new_price = float(change.new) if change.new else 0
                except (TypeError, ValueError):
                    continue

                if new_price == 0 or old_price == 0:
                    continue

                if new_price < old_price:
                    drop_percent = (old_price - new_price) / old_price * 100
                    if drop_percent >= self._min_price_drop:
                        reasons.append(
                            f"📉 Цена снижена: {int(old_price)} → {int(new_price)} ₽ (-{drop_percent:.1f}%)"
                        )

            # Скидка увеличилась
            if change.field == "discount":
                try:
                    old_discount = float(change.old)
                    new_discount = float(change.new) if change.new else 0
                except (TypeError, ValueError):
                    continue

                if new_discount > old_discount:
                    increase = new_discount - old_discount
                    if increase >= self._min_discount_increase:
                        reasons.append(
                            f"🔥 Скидка выросла: {int(old_discount)}% → {int(new_discount)}% (+{increase:.0f}%)"
                        )

        if reasons:
            return "\n".join(reasons)
        return None

    def _has_favorable_changes(self, ch: ChangeResult) -> bool:
        return self._get_publish_reason(ch) is not None

    async def _increment_no_image_counter(
        self,
        session: AsyncSession,
        external_id: str | None,
        platform: PlatformCode,
    ) -> bool:
        """
        Увеличивает счётчик неудачных загрузок картинки.
        
        Returns:
            True — если достигнут порог и товар нужно удалить
            False — ещё не достигнут порог
        """
        if not external_id:
            return False
        
        try:
            from sqlalchemy import select, update
            from bot.db.models import Product, Platform
            
            # Получаем platform_id
            platform_result = await session.execute(
                select(Platform.id).where(Platform.code == platform)
            )
            platform_id = platform_result.scalar()
            if not platform_id:
                return False
            
            # Увеличиваем счётчик
            stmt = (
                update(Product)
                .where(
                    Product.platform_id == platform_id,
                    Product.external_id == str(external_id),
                )
                .values(no_image_fail_count=Product.no_image_fail_count + 1)
                .returning(Product.no_image_fail_count)
            )
            result = await session.execute(stmt)
            new_count = result.scalar()
            
            if new_count is None:
                return False
            
            self._log.debug(
                "Product %s no_image_fail_count: %d/%d",
                external_id, new_count, NO_IMAGE_FAIL_THRESHOLD
            )
            
            return new_count >= NO_IMAGE_FAIL_THRESHOLD
            
        except Exception as e:
            self._log.warning("Failed to increment no_image_counter for %s: %s", external_id, e)
            return False

    async def _reset_no_image_counter(
        self,
        session: AsyncSession,
        external_id: str | None,
        platform: PlatformCode,
    ) -> None:
        """Сбрасывает счётчик неудачных загрузок картинки."""
        if not external_id:
            return
        
        try:
            from sqlalchemy import select, update
            from bot.db.models import Product, Platform
            
            platform_result = await session.execute(
                select(Platform.id).where(Platform.code == platform)
            )
            platform_id = platform_result.scalar()
            if not platform_id:
                return
            
            stmt = (
                update(Product)
                .where(
                    Product.platform_id == platform_id,
                    Product.external_id == str(external_id),
                )
                .values(no_image_fail_count=0)
            )
            await session.execute(stmt)
            
        except Exception as e:
            self._log.warning("Failed to reset no_image_counter for %s: %s", external_id, e)


def _len_safe(it: Iterable[Any]) -> int | str:
    try:
        return len(it)
    except Exception:
        return "?"