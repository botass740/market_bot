# bot/main.py

import asyncio
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramNetworkError

from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker, init_db
from bot.db.models import PlatformCode
from bot.filtering.filters import FilterService
from bot.handlers.router import router as root_router
from bot.handlers import admin as admin_handlers
from bot.parsers.detmir import DetmirParser
from bot.parsers.ozon import OzonParser
from bot.parsers.wb import WildberriesParser
from bot.pipeline.runner import PipelineRunner
from bot.posting.poster import PostingService
from bot.scheduler.scheduler import SchedulerService
from bot.services.product_manager import ProductManager
from bot.services.settings_manager import SettingsManager
from bot.utils.logger import setup_logger


CLEANUP_TIMESTAMP_FILE = Path(".last_cleanup")
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", "24"))

# Retry настройки для polling
POLLING_RETRY_DELAY = 10  # секунд между попытками
POLLING_MAX_RETRIES = 0   # 0 = бесконечно


def _needs_cleanup() -> bool:
    if not CLEANUP_TIMESTAMP_FILE.exists():
        return True
    try:
        timestamp = float(CLEANUP_TIMESTAMP_FILE.read_text().strip())
        last_cleanup = datetime.fromtimestamp(timestamp)
        age = datetime.now() - last_cleanup
        return age > timedelta(hours=CLEANUP_INTERVAL_HOURS)
    except Exception:
        return True


def _mark_cleanup_done() -> None:
    CLEANUP_TIMESTAMP_FILE.write_text(str(datetime.now().timestamp()))


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


async def main() -> None:
    settings = load_settings()

    setup_logger(level=logging.INFO)
    log = logging.getLogger("bot")

    # Флаги включения платформ (для тестов/продакшена)
    enable_wb = _env_bool("ENABLE_WB", True)
    enable_ozon = _env_bool("ENABLE_OZON", True)
    enable_dm = _env_bool("ENABLE_DETMIR", True)
    log.info("Enabled platforms: WB=%s OZON=%s DM=%s", enable_wb, enable_ozon, enable_dm)

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()
    
    # Middleware для retry при сетевых ошибках (Windows fix)
    from bot.middlewares.retry import RetryMiddleware
    dp.message.middleware(RetryMiddleware())
    dp.callback_query.middleware(RetryMiddleware())
    
    dp.include_router(root_router)

    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)
    await init_db(engine)

    # Инициализируем менеджеры
    settings_manager = SettingsManager(session_factory)
    admin_handlers.set_settings_manager(settings_manager)

    product_manager = ProductManager(session_factory, settings_manager=settings_manager)

    filter_service = FilterService(settings.filtering, settings_manager=settings_manager)
    posting_service = PostingService(bot, settings.posting)

    pipeline = PipelineRunner(
        session_factory=session_factory,
        filter_service=filter_service,
        posting_service=posting_service,
        thresholds=settings.filtering,
        product_manager=product_manager,
        settings_manager=settings_manager,
    )

    # Очистка мёртвых товаров (только WB)
    if enable_wb:
        if _needs_cleanup():
            log.info("Cleaning up dead products (runs every %d hours)...", CLEANUP_INTERVAL_HOURS)
            try:
                removed, _dead_ids = await product_manager.cleanup_dead_products(PlatformCode.WB)
                if removed > 0:
                    log.info("Removed %d dead products", removed)
                _mark_cleanup_done()
            except Exception as e:
                log.error("Cleanup failed: %s", e)
        else:
            log.info("Skipping cleanup (last run < %d hours ago)", CLEANUP_INTERVAL_HOURS)
    else:
        log.info("WB disabled -> cleanup skipped")

        # Очистка мёртвых товаров (OZON: по 404/410 подряд)
    if enable_ozon:
        if _needs_cleanup():
            log.info("Cleaning up dead OZON products (404/410 x%d)...", 3)
            try:
                removed, dead_ids = await product_manager.cleanup_dead_products_ozon(dead_after=3)
                if removed > 0:
                    log.info("OZON dead removed %d products", removed)
                _mark_cleanup_done()
            except Exception as e:
                log.error("OZON cleanup failed: %s", e)
        else:
            log.info("Skipping OZON cleanup (last run < %d hours ago)", CLEANUP_INTERVAL_HOURS)
    else:
        log.info("OZON disabled -> cleanup skipped")
    

    # Проверяем количество товаров (WB)
    target_count = int(os.getenv("TARGET_PRODUCT_COUNT", "3000"))

    if enable_wb:
        current_count = await product_manager.get_product_count(PlatformCode.WB)
        if current_count < target_count:
            log.info("WB products count %d < %d, refilling...", current_count, target_count)
            added, total = await product_manager.refill_products(PlatformCode.WB, target_count=target_count)
            log.info("WB refilled %d products, total: %d", added, total)

    if enable_ozon:
        ozon_count = await product_manager.get_product_count(PlatformCode.OZON)
        if ozon_count < target_count:
            log.info(
                "OZON products count %d < %d. Will auto-refill via COLLECT.",
                ozon_count,
                target_count,
            )

    # Создаём парсеры с ID из БД
    wb_parser = None
    detmir_parser = None

    if enable_wb:
        log.info("WB enabled -> parser will be created per job (fresh IDs from DB)")

    if enable_ozon:
        ozon_product_ids = await product_manager.get_product_ids(PlatformCode.OZON)
        log.info("OZON products in DB: %d", len(ozon_product_ids))
        #ozon_parser = None  # парсер будем создавать на каждый запуск

    if enable_dm:
        detmir_parser = DetmirParser()

    # Initial sync (только включённые платформы)
    if enable_wb and wb_parser:
        log.info("WB: skipping initial sync, will run via scheduler")

    if enable_ozon:
        log.info("OZON: skipping initial sync, will run in background")

    if enable_dm and detmir_parser:
        log.info("DM: skipping initial sync, will run in background")

    async def ozon_job() -> None:
        """OZON парсинг."""
        ozon_ids = await product_manager.get_product_ids(PlatformCode.OZON)
        log.info("OZON products to monitor (fresh from DB): %d", len(ozon_ids))

        parser = OzonParser(product_ids=ozon_ids if ozon_ids else None)
        try:
            await pipeline.run_platform(platform=PlatformCode.OZON, parser=parser)
        finally:
            await parser.close()

    async def ozon_job_wrapper() -> None:
        """Обёртка для запуска OZON в фоновой задаче."""
        try:
            await ozon_job()
        except Exception as e:
            log.exception("OZON job failed: %s", e)
    async def detmir_job() -> None:
        """DETMIR парсинг."""
        dm_ids = await product_manager.get_product_ids(PlatformCode.DM)
        log.info("DETMIR products to monitor (fresh from DB): %d", len(dm_ids))

        parser = DetmirParser(product_ids=dm_ids if dm_ids else None)
        await pipeline.run_platform(platform=PlatformCode.DM, parser=parser)

    async def wb_job() -> None:
        """WB парсинг."""
        wb_ids = await product_manager.get_product_ids(PlatformCode.WB)
        log.info("WB products to monitor (fresh from DB): %d", len(wb_ids))

        parser = WildberriesParser(product_ids=[int(x) for x in wb_ids] if wb_ids else None)
        await pipeline.run_platform(platform=PlatformCode.WB, parser=parser)


    async def wb_job_wrapper() -> None:
        """Обёртка для запуска WB в фоновой задаче."""
        try:
            await wb_job()
        except Exception as e:
            log.exception("WB job failed: %s", e)


    async def detmir_job_wrapper() -> None:
        """Обёртка для запуска DETMIR в фоновой задаче."""
        try:
            await detmir_job()
        except Exception as e:
            log.exception("DETMIR job failed: %s", e)

    scheduler = SchedulerService(
        intervals=settings.parsing,
        wb_task=(lambda: wb_job_wrapper()) if enable_wb else None,
        ozon_task=(lambda: ozon_job_wrapper()) if enable_ozon else None,
        detmir_task=(lambda: detmir_job_wrapper()) if enable_dm else None,
    )

    scheduler.start()
    log.info("Scheduler started")

    # Запускаем первый DM цикл в фоне (не блокируя polling)
    #if enable_dm:
    #    log.info("DM: starting first sync in background")
    #    asyncio.create_task(detmir_job_wrapper())

    # === POLLING С RETRY ===
    retry_count = 0
    while True:
        try:
            log.info("Starting Telegram polling...")
            await dp.start_polling(bot)
            break  # Нормальный выход (Ctrl+C)
        except TelegramNetworkError as e:
            retry_count += 1
            log.error(
                "Telegram network error (attempt %d): %s. Retrying in %d sec...",
                retry_count,
                e,
                POLLING_RETRY_DELAY,
            )
            if POLLING_MAX_RETRIES > 0 and retry_count >= POLLING_MAX_RETRIES:
                log.critical("Max retries reached, exiting")
                break
            await asyncio.sleep(POLLING_RETRY_DELAY)
        except asyncio.CancelledError:
            log.info("Polling cancelled")
            break
        except Exception as e:
            retry_count += 1
            log.exception(
                "Unexpected polling error (attempt %d): %s. Retrying in %d sec...",
                retry_count,
                e,
                POLLING_RETRY_DELAY,
            )
            if POLLING_MAX_RETRIES > 0 and retry_count >= POLLING_MAX_RETRIES:
                log.critical("Max retries reached, exiting")
                break
            await asyncio.sleep(POLLING_RETRY_DELAY)

    log.info("Shutting down")
    scheduler.shutdown()
    await bot.session.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())