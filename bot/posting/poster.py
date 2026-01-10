# bot/posting/poster.py

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any

import aiohttp
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import (
    BufferedInputFile,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from playwright.async_api import async_playwright

from bot.config import PostingSettings

log = logging.getLogger(__name__)

# Настройки из ENV
FALLBACK_IMAGE_PATH = os.getenv("POSTING_FALLBACK_IMAGE", "test.jpg").strip()
POST_DELAY = float(os.getenv("POSTING_DELAY", "3.0"))
SKIP_PRODUCTS_WITHOUT_IMAGE = os.getenv("SKIP_PRODUCTS_WITHOUT_IMAGE", "true").lower() in ("true", "1", "yes")

# OZON browser fallback (через CDP)
OZON_CDP_URL = os.getenv("OZON_CDP_URL", "http://localhost:9222").strip()
OZON_IMAGE_BROWSER_TIMEOUT_MS = int(os.getenv("OZON_IMAGE_BROWSER_TIMEOUT_MS", "15000"))


class ProductUnavailableError(Exception):
    """Товар недоступен (удалён, нет картинки)."""

    def __init__(self, message: str, external_id: str | None = None):
        super().__init__(message)
        self.external_id = external_id


class PostingService:
    def __init__(self, bot: Bot, settings: PostingSettings) -> None:
        self._bot = bot

        env_channel = os.getenv("POSTING_CHANNEL", "").strip()
        self._channel = (settings.channel or env_channel).strip()

        self._max_per_hour = settings.max_posts_per_hour
        self._sent: deque[datetime] = deque()
        self._last_post_time: float = 0

        log.info(
            "PostingService channel=%r, delay=%.1fs, skip_no_image=%s",
            self._channel, POST_DELAY, SKIP_PRODUCTS_WITHOUT_IMAGE
        )

    async def post_product(self, product: dict[str, Any]) -> bool:
        """
        Публикует товар в канал.

        Returns:
            True — успешно опубликован
            False — не опубликован (rate limit)

        Raises:
            ProductUnavailableError — товар удалён/недоступен (после нескольких неудач)
        """
        if not self._channel:
            raise ValueError("POSTING_CHANNEL is not configured")

        if not self._allow_now():
            return False

        await self._wait_delay()

        url = _as_str(product.get("product_url"))
        caption = _build_caption(product)
        markup = _build_keyboard(url)

        # Пробуем получить картинку
        photo, is_fallback = await self._resolve_photo_with_status(product)

        external_id = product.get("external_id")

        # Если картинка не найдена
        if is_fallback:
            if SKIP_PRODUCTS_WITHOUT_IMAGE:
                # Увеличиваем счётчик неудач (будет обработано в pipeline)
                log.warning(
                    "No image for product %s — marking as no_image_fail",
                    external_id
                )
                raise ProductUnavailableError(
                    f"Product {external_id} has no image",
                    external_id=str(external_id) if external_id else None
                )

        success = await self._send_with_retry(photo, caption, markup)

        if success:
            self._mark_sent()
            # Сигнализируем что картинка успешно загружена (для сброса счётчика)
            product["_image_ok"] = True

        return success

    async def _resolve_photo_with_status(
        self, product: dict[str, Any]
    ) -> tuple[FSInputFile | BufferedInputFile, bool]:
        """
        Загрузка картинки с информацией о fallback.

        Returns:
            (photo, is_fallback) — картинка и флаг, что это заглушка
        """
        external_id = product.get("external_id")
        platform = str(product.get("platform", "")).upper()

        # 1) Обычная цепочка URL (как раньше)
        urls_to_try = _build_image_urls_chain(product)
        for url in urls_to_try:
            img_bytes = await _download_image(url)
            if img_bytes:
                ext = "webp" if url.endswith(".webp") else "jpg"
                log.debug("Downloaded image: %s", url)
                return BufferedInputFile(img_bytes, filename=f"photo.{ext}"), False

        log.warning("Could not download any image for product %s", external_id)

        # 2) OZON: browser fallback -> og:image / twitter:image
        if platform == "OZON":
            product_url = _as_str(product.get("product_url"))
            if product_url:
                og_url = await _resolve_ozon_image_url_via_browser(product_url)
                if og_url:
                    img_bytes = await _download_image(og_url)
                    if img_bytes:
                        log.info("OZON image resolved via browser for %s: %s", external_id, og_url)
                        return BufferedInputFile(img_bytes, filename="photo.jpg"), False

        # 3) Полный fallback
        return _fallback_photo(), True

    async def _send_with_retry(
        self,
        photo: FSInputFile | BufferedInputFile,
        caption: str,
        markup: InlineKeyboardMarkup | None,
        max_retries: int = 3
    ) -> bool:
        """Отправка с повторными попытками при flood control."""

        for attempt in range(max_retries):
            try:
                await self._bot.send_photo(
                    chat_id=self._channel,
                    photo=photo,
                    caption=caption,
                    reply_markup=markup,
                    parse_mode="HTML",
                )
                return True

            except TelegramRetryAfter as e:
                wait_time = e.retry_after + 1
                log.warning(
                    "Flood control, waiting %d seconds (attempt %d/%d)",
                    wait_time, attempt + 1, max_retries
                )
                await asyncio.sleep(wait_time)

            except Exception as e:
                log.warning("Failed to send photo: %s", e)

                # Если не FSInputFile (т.е. уже байты), пробуем отправить заглушку
                if not isinstance(photo, FSInputFile):
                    try:
                        await self._bot.send_photo(
                            chat_id=self._channel,
                            photo=_fallback_photo(),
                            caption=caption,
                            reply_markup=markup,
                            parse_mode="HTML",
                        )
                        return True
                    except TelegramRetryAfter as e2:
                        await asyncio.sleep(e2.retry_after + 1)
                    except Exception as e2:
                        log.error("Fallback also failed: %s", e2)

                return False

        log.error("Max retries exceeded for posting")
        return False

    async def _wait_delay(self) -> None:
        """Ждём минимальную задержку между постами."""
        now = time.time()
        elapsed = now - self._last_post_time

        if elapsed < POST_DELAY:
            wait = POST_DELAY - elapsed
            await asyncio.sleep(wait)

        self._last_post_time = time.time()

    async def post_products(self, products: Iterable[dict[str, Any]]) -> int:
        """Публикует несколько товаров."""
        posted = 0
        for p in products:
            try:
                ok = await self.post_product(p)
                if not ok:
                    break
                posted += 1
            except ProductUnavailableError:
                # Пропускаем удалённые товары, продолжаем с остальными
                continue
        return posted

    def _allow_now(self) -> bool:
        """Проверяет лимит постов в час."""
        if self._max_per_hour <= 0:
            return True

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=1)
        while self._sent and self._sent[0] < cutoff:
            self._sent.popleft()
        return len(self._sent) < self._max_per_hour

    def _mark_sent(self) -> None:
        """Отмечает отправку поста."""
        self._sent.append(datetime.now(timezone.utc))


# =============================================================================
# Вспомогательные функции
# =============================================================================

def _build_image_urls_chain(product: dict[str, Any]) -> list[str]:
    """Строит цепочку URL картинок для перебора."""
    urls: list[str] = []

    image_url = _as_str(product.get("image_url"))
    if image_url:
        urls.append(image_url)

        base_url = image_url.rsplit("/", 1)[0]
        pics = product.get("pics", 1)
        max_pics = min(pics, 5)

        for i in range(1, max_pics + 1):
            webp_url = f"{base_url}/{i}.webp"
            jpg_url = f"{base_url}/{i}.jpg"

            if webp_url not in urls:
                urls.append(webp_url)
            if jpg_url not in urls:
                urls.append(jpg_url)

        return urls

    # fallback для WB по nm_id (если image_url не пришёл)
    external_id = product.get("external_id")
    if not external_id:
        return urls

    try:
        nm_id = int(external_id)
    except (TypeError, ValueError):
        return urls

    pics = product.get("pics", 1)
    max_pics = min(pics, 5)

    vol = nm_id // 100_000
    part = nm_id // 1_000
    basket = _get_basket_number(vol)

    base = f"https://basket-{basket:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/big"

    for i in range(1, max_pics + 1):
        urls.append(f"{base}/{i}.webp")
        urls.append(f"{base}/{i}.jpg")

    return urls


def _get_basket_number(vol: int) -> int:
    """Определяет номер basket по vol (актуальная таблица WB)."""
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


def _fallback_photo() -> FSInputFile:
    """Возвращает заглушку-картинку."""
    return FSInputFile(FALLBACK_IMAGE_PATH)


async def _download_image(url: str, timeout: int = 20) -> bytes | None:
    """Скачивает картинку по URL (WB/OZON), с ретраями и корректными headers."""
    if not url:
        return None

    def _pick_referer(u: str) -> str:
        u = u.lower()
        if "ozon" in u or "ozone" in u:
            return "https://www.ozon.ru/"
        if "wildberries" in u or "wbbasket" in u:
            return "https://www.wildberries.ru/"
        return "https://www.google.com/"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Referer": _pick_referer(url),
        "Connection": "keep-alive",
    }

    t = aiohttp.ClientTimeout(total=timeout)

    for attempt in range(1, 4):
        try:
            async with aiohttp.ClientSession(timeout=t, headers=headers) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(0.6 * attempt)
                        continue

                    ct = (resp.headers.get("Content-Type") or "").lower()
                    if ("image" not in ct) and ("application/octet-stream" not in ct):
                        return None

                    data = await resp.read()
                    if not data:
                        await asyncio.sleep(0.6 * attempt)
                        continue

                    if len(data) > 8_000_000:
                        return None

                    return data
        except Exception:
            await asyncio.sleep(0.6 * attempt)

    return None


async def _resolve_ozon_image_url_via_browser(product_url: str) -> str | None:
    """
    Открывает страницу OZON в браузерном контексте (CDP) и достаёт og:image/twitter:image.
    """
    if not product_url:
        return None

    # гарантируем, что Chrome запущен на CDP порту
    try:
        from bot.utils.chrome_manager import ensure_chrome_running
        ok = await ensure_chrome_running()
        if not ok:
            return None
    except Exception:
        return None

    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.connect_over_cdp(OZON_CDP_URL)

        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = context.pages[0] if context.pages else await context.new_page()

        await page.goto(product_url, wait_until="domcontentloaded", timeout=OZON_IMAGE_BROWSER_TIMEOUT_MS)

        og = await page.eval_on_selector(
            "meta[property='og:image']",
            "el => el && el.content ? el.content : null"
        )
        if isinstance(og, str) and og.strip():
            return og.strip()

        tw = await page.eval_on_selector(
            "meta[name='twitter:image']",
            "el => el && el.content ? el.content : null"
        )
        if isinstance(tw, str) and tw.strip():
            return tw.strip()

        return None

    except Exception:
        return None

    finally:
        try:
            if pw:
                await pw.stop()
        except Exception:
            pass


def _build_keyboard(url: str | None) -> InlineKeyboardMarkup | None:
    """Создаёт inline-кнопку со ссылкой."""
    buttons = []
    if url:
        buttons.append([InlineKeyboardButton(text="🛒 Перейти к товару", url=url)])
    if not buttons:
        return None
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_caption(product: dict[str, Any]) -> str:
    """Формирует caption для поста."""
    lines: list[str] = []

    name = _as_str(product.get("name")) or _as_str(product.get("title")) or "Товар"
    platform = str(product.get("platform", "")).upper()
    platform_emoji = {"WB": "🟣", "OZON": "🔵", "DETMIR": "🟢"}.get(platform, "🛍")

    lines.append(f"{platform_emoji} <b>{escape(name)}</b>")
    lines.append("")

    publish_reason = product.get("publish_reason")
    if publish_reason:
        lines.append(f"<b>{publish_reason}</b>")
        lines.append("")

    price_min = product.get("price_min")
    price_max = product.get("price_max")
    price = product.get("price")

    if price_min is not None and price_max is not None:
        price_min_fmt = _format_price(price_min)
        price_max_fmt = _format_price(price_max)

        if price_min == price_max:
            lines.append(f"💰 Цена: <b>{price_min_fmt} ₽</b>")
        else:
            lines.append(f"💰 Цена: <b>от {price_min_fmt} ₽ до {price_max_fmt} ₽</b>")
    elif price is not None:
        lines.append(f"💰 Цена: <b>{_format_price(price)} ₽</b>")

    discount = product.get("discount_percent")
    old_price = product.get("old_price")

    if discount is not None and old_price is not None:
        old_price_fmt = _format_price(old_price)
        lines.append(f"🔥 Скидка: <b>{int(discount)}%</b> (было {old_price_fmt} ₽)")
    elif discount is not None:
        lines.append(f"🔥 Скидка: <b>{int(discount)}%</b>")
    elif old_price is not None:
        old_price_fmt = _format_price(old_price)
        lines.append(f"💸 Было: <s>{old_price_fmt} ₽</s>")

    # Нормализуем rating/feedbacks
    rating_raw = product.get("rating")
    try:
        rating = float(rating_raw) if rating_raw is not None else None
    except (TypeError, ValueError):
        rating = None

    feedbacks_raw = product.get("feedbacks")
    try:
        feedbacks = int(feedbacks_raw) if feedbacks_raw is not None else 0
    except (TypeError, ValueError):
        feedbacks = 0

    if rating is not None and rating > 0:
        if feedbacks > 0:
            lines.append(f"⭐ Рейтинг: <b>{rating:.1f}</b> ({feedbacks} отзывов)")
        else:
            lines.append(f"⭐ Рейтинг: <b>{rating:.1f}</b>")
    elif feedbacks > 0:
        lines.append(f"💬 Отзывов: {feedbacks}")

    article = product.get("external_id")
    if article:
        lines.append("")
        lines.append(f"📎 Артикул: <code>{escape(str(article))}</code>")

    return "\n".join(lines)


def _format_price(price: float | int) -> str:
    """Форматирует цену с пробелами."""
    if price is None:
        return "—"
    price_int = int(round(float(price)))
    return f"{price_int:,}".replace(",", " ")


def _as_str(value: Any) -> str | None:
    """Конвертирует значение в строку или None."""
    if value is None:
        return None
    s = str(value).strip()
    return s or None