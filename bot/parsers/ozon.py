# bot/parsers/ozon.py
"""
Парсер OZON с двумя режимами:
- COLLECT: сбор товаров через infinite scroll (для наполнения БД)
- MONITOR: проверка цен через API (для мониторинга)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
from collections import Counter
from typing import Any, Iterable

log = logging.getLogger(__name__)

# CDP подключение к Chrome
CDP_URL = os.getenv("OZON_CDP_URL", "http://localhost:9222").strip()

# === Настройки COLLECT режима ===
COLLECT_TARGET_COUNT = int(os.getenv("OZON_COLLECT_TARGET", "3000"))
SCROLL_DELAY_SEC = float(os.getenv("OZON_SCROLL_DELAY_SEC", "1.2"))
MAX_SCROLL_STEPS = int(os.getenv("OZON_MAX_SCROLL_STEPS", "500"))
QUIET_STEPS_STOP = int(os.getenv("OZON_QUIET_STEPS_STOP", "30"))
LOG_EVERY_STEPS = int(os.getenv("OZON_LOG_EVERY_STEPS", "25"))

# === Настройки MONITOR режима ===
MONITOR_BATCH_SIZE = int(os.getenv("OZON_MONITOR_BATCH_SIZE", "100"))
MONITOR_REQUEST_DELAY = float(os.getenv("OZON_MONITOR_REQUEST_DELAY", "0.3"))
MONITOR_ERROR_DELAY = float(os.getenv("OZON_MONITOR_ERROR_DELAY", "2.0"))
MONITOR_MAX_ERRORS = int(os.getenv("OZON_MONITOR_MAX_ERRORS", "10"))
# === Антибан / recovery ===
OZON_403_COOLDOWN_SEC = float(os.getenv("OZON_403_COOLDOWN_SEC", "120"))  # пауза при волне 403
OZON_MAX_RECOVERIES = int(os.getenv("OZON_MAX_RECOVERIES", "3"))         # сколько раз пытаться восстановиться за цикл

# Категории для сбора
DEFAULT_SEED_URLS = [
    "https://www.ozon.ru/category/smartfony-15502/",
    "https://www.ozon.ru/category/noutbuki-15692/",
    "https://www.ozon.ru/category/naushniki-i-bluetooth-garnitury-15548/",
    "https://www.ozon.ru/category/planshety-15525/",
    "https://www.ozon.ru/category/televizory-15528/",
]

# Пропускать товары с ценой только по карте
SKIP_CARD_ONLY_ITEMS = os.getenv("OZON_SKIP_CARD_ONLY", "false").lower() in ("1", "true", "yes")


def _extract_price(text: str) -> int | None:
    """Извлекает число из строки с ценой."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else None


def _parse_discount(discount_str: str | None) -> int | None:
    """Парсит процент скидки."""
    if not discount_str:
        return None
    match = re.search(r"(\d+)", str(discount_str))
    return int(match.group(1)) if match else None


class OzonParser:
    """
    Парсер OZON.

    Режимы:
    - COLLECT: parse_products_batch([]) — сбор через scroll (SKU)
    - MONITOR: parse_products_batch(["sku1", "sku2", ...]) — проверка через API
    """

    def __init__(self, product_ids: Iterable[int | str] | None = None) -> None:
        self._product_ids = [str(x) for x in product_ids] if product_ids else []

        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._connected = False

    # =========================================================================
    # Подключение к Chrome
    # =========================================================================

    async def _connect(self) -> None:
        """Подключается к Chrome через CDP."""
        if self._connected and self._page:
            return

        from bot.utils.chrome_manager import ensure_chrome_running

        ok = await ensure_chrome_running()
        if not ok:
            raise RuntimeError("Не удалось запустить Chrome для OZON")

        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.connect_over_cdp(CDP_URL)

        self._context = self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()

        # Инициализируем сессию
        await self._page.goto("https://www.ozon.ru/", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

        self._connected = True
        log.info("OZON: connected to Chrome")

    async def _ensure_connected(self) -> None:
        if not self._connected:
            await self._connect()

    async def close(self) -> None:
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass

        self._connected = False
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    # =========================================================================
    # Основные методы
    # =========================================================================

    async def fetch_products(self) -> Iterable[Any]:
        """Возвращает список SKU для мониторинга."""
        return list(self._product_ids)

    async def parse_product(self, raw: Any) -> dict[str, Any]:
        await self._ensure_connected()
        sku = str(raw)
        products = await self._monitor_products([sku])
        return products[0] if products else self._empty_product(sku)

    async def parse_products_batch(
        self, 
        product_ids: list[int | str],
        collect_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_connected()

        if not product_ids:
            log.info("OZON: COLLECT mode (scroll)")
            if collect_queries:
                # Используем категории из БД
                return await self._collect_from_scroll(search_queries=collect_queries)
            else:
                # Fallback на дефолтные
                return await self._collect_from_scroll()

        log.info("OZON: MONITOR mode (%d products)", len(product_ids))
        return await self._monitor_products(product_ids)

    # =========================================================================
    # COLLECT режим: сбор через scroll
    # =========================================================================

    def _extract_sku_from_href(self, href: str) -> str | None:
        if not href:
            return None
        m = re.search(r"/product/(\d+)", href)
        return m.group(1) if m else None

    def _build_search_url(self, query: str) -> str:
        from urllib.parse import quote_plus
        q = quote_plus(query.strip())
        return f"https://www.ozon.ru/search/?text={q}"

    async def _collect_from_scroll(
        self, 
        seed_urls: list[str] | None = None,
        search_queries: list[str] | None = None, 
        target: int | None = None,
    ) -> list[dict[str, Any]]:
        """Собирает товары через infinite scroll.

        Args:
            seed_urls: Прямые URL категорий OZON
            search_queries: Текстовые запросы для поиска (приоритет над seed_urls)
            target: Целевое количество товаров
            
        Сбор SKU делаем:
        - best-effort из network (tileGrid/widgetStates), если формат совпадает
        - надёжный fallback из DOM: ссылки /product/<sku>
        """

        collected: dict[str, dict[str, Any]] = {}
        target = int(target or COLLECT_TARGET_COUNT)
        
        # Формируем список URL для обхода
        if search_queries:
            urls_to_visit = [self._build_search_url(q) for q in search_queries]
            log.info("OZON COLLECT: using %d search queries", len(search_queries))
        elif seed_urls:
            urls_to_visit = seed_urls
        else:
            urls_to_visit = DEFAULT_SEED_URLS
            log.info("OZON COLLECT: using DEFAULT_SEED_URLS")

        num_categories = len(urls_to_visit)
        
        # Равномерная квота на каждую категорию
        base_quota = target // num_categories
        extra = target % num_categories  # остаток распределим на первые категории
        
        log.info("OZON COLLECT: target=%d, categories=%d, base_quota=%d, extra=%d", 
                target, num_categories, base_quota, extra)

        # Словарь: сколько собрали с каждой категории
        collected_per_category: dict[int, int] = {}

        # --- network collector ---
        current_category_idx = 0
        quiet_steps = 0
        
        async def on_response(response):
            nonlocal quiet_steps
            try:
                url = response.url
                ct = (response.headers.get("content-type") or "").lower()

                if "json" not in ct:
                    return

                data = await response.json()
                if not isinstance(data, dict):
                    return

                items = self._parse_tile_grid(data)
                if not items:
                    return

                before = len(collected)
                for item in items:
                    eid = item.get("external_id")
                    if eid and eid not in collected:
                        collected[eid] = item

                if len(collected) > before:
                    quiet_steps = 0
                    log.info("OZON COLLECT(network): +%d (total=%d/%d)", 
                            len(collected) - before, len(collected), target)

            except Exception:
                return

        log.info("OZON: attach response listener")
        self._page.on("response", on_response)

        try:
            # === ПЕРВЫЙ ПРОХОД: собираем по квоте с каждой категории ===
            for category_idx, seed_url in enumerate(urls_to_visit):
                current_category_idx = category_idx
                
                # Квота для этой категории (первые extra категорий получают +1)
                category_quota = base_quota + (1 if category_idx < extra else 0)
                
                # Сколько уже собрано до этой категории
                collected_before = len(collected)
                
                quiet_steps = 0
                log.info("OZON: [Pass 1] category %d/%d: %s (quota=%d, total=%d/%d)", 
                        category_idx + 1, num_categories, seed_url[:60], 
                        category_quota, len(collected), target)

                try:
                    await self._page.goto(seed_url, wait_until="domcontentloaded", timeout=45000)
                except Exception as e:
                    log.warning("OZON: failed to open %s: %s", seed_url, e)
                    collected_per_category[category_idx] = 0
                    continue

                await asyncio.sleep(3)

                # Диагностика страницы
                try:
                    title = await self._page.title()
                    log.info("OZON page title: %s", title)
                    html = (await self._page.content()).lower()
                    if "captcha" in html or "капча" in html:
                        log.warning("OZON: looks like CAPTCHA page")
                except Exception:
                    pass

                # Скроллим пока не соберём квоту для этой категории
                for step in range(1, MAX_SCROLL_STEPS + 1):
                    collected_from_this = len(collected) - collected_before
                    
                    if collected_from_this >= category_quota:
                        log.info("OZON: category %d/%d quota reached (%d items)", 
                                category_idx + 1, num_categories, collected_from_this)
                        break

                    await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(SCROLL_DELAY_SEC)

                    # DOM fallback каждые 5 шагов
                    if step % 5 == 0:
                        try:
                            hrefs: list[str] = await self._page.eval_on_selector_all(
                                "a[href*='/product/']",
                                "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
                            )

                            before_dom = len(collected)
                            for href in hrefs:
                                sku = self._extract_sku_from_href(href)
                                if not sku or sku in collected:
                                    continue
                                collected[sku] = {
                                    "external_id": sku,
                                    "platform": "ozon",
                                    "name": None,
                                    "title": None,
                                    "price": None,
                                    "card_price": None,
                                    "old_price": None,
                                    "discount_percent": None,
                                    "rating": None,
                                    "feedbacks": None,
                                    "in_stock": None,
                                    "product_url": f"https://www.ozon.ru/product/{sku}/",
                                    "image_url": None,
                                }

                            if len(collected) > before_dom:
                                quiet_steps = 0
                                log.info("OZON COLLECT(dom): +%d (total=%d/%d)",
                                        len(collected) - before_dom, len(collected), target)
                            else:
                                quiet_steps += 1

                        except Exception:
                            quiet_steps += 1
                    else:
                        quiet_steps += 1

                    if step % LOG_EVERY_STEPS == 0:
                        log.info("OZON scroll step=%d, category %d/%d, collected=%d/%d", 
                                step, category_idx + 1, num_categories, len(collected), target)

                    if quiet_steps >= QUIET_STEPS_STOP:
                        log.info("OZON: no new items for %d steps in category %d", 
                                quiet_steps, category_idx + 1)
                        break

                # Запоминаем сколько собрали с этой категории
                collected_per_category[category_idx] = len(collected) - collected_before

            # === ВТОРОЙ ПРОХОД: добираем если не хватает ===
            if len(collected) < target:
                shortage = target - len(collected)
                log.info("OZON: [Pass 2] need to collect %d more items", shortage)
                
                # Сортируем категории по тому, сколько ещё можно собрать
                # (те, с которых собрали меньше квоты — приоритетнее)
                categories_to_retry = []
                for idx in range(num_categories):
                    quota = base_quota + (1 if idx < extra else 0)
                    collected_from = collected_per_category.get(idx, 0)
                    potential = quota * 2 - collected_from  # можем собрать ещё столько же
                    if potential > 0:
                        categories_to_retry.append((idx, potential))
                
                # Сортируем по потенциалу (где больше можно собрать)
                categories_to_retry.sort(key=lambda x: x[1], reverse=True)
                
                for category_idx, _ in categories_to_retry:
                    if len(collected) >= target:
                        break
                        
                    seed_url = urls_to_visit[category_idx]
                    quiet_steps = 0
                    
                    log.info("OZON: [Pass 2] retry category %d/%d: %s (need %d more)", 
                            category_idx + 1, num_categories, seed_url[:60], 
                            target - len(collected))

                    try:
                        await self._page.goto(seed_url, wait_until="domcontentloaded", timeout=45000)
                    except Exception as e:
                        log.warning("OZON: failed to open %s: %s", seed_url, e)
                        continue

                    await asyncio.sleep(3)

                    for step in range(1, MAX_SCROLL_STEPS + 1):
                        if len(collected) >= target:
                            break

                        await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(SCROLL_DELAY_SEC)

                        if step % 5 == 0:
                            try:
                                hrefs = await self._page.eval_on_selector_all(
                                    "a[href*='/product/']",
                                    "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
                                )
                                before_dom = len(collected)
                                for href in hrefs:
                                    sku = self._extract_sku_from_href(href)
                                    if not sku or sku in collected:
                                        continue
                                    collected[sku] = {
                                        "external_id": sku,
                                        "platform": "ozon",
                                        "name": None,
                                        "title": None,
                                        "price": None,
                                        "card_price": None,
                                        "old_price": None,
                                        "discount_percent": None,
                                        "rating": None,
                                        "feedbacks": None,
                                        "in_stock": None,
                                        "product_url": f"https://www.ozon.ru/product/{sku}/",
                                        "image_url": None,
                                    }
                                if len(collected) > before_dom:
                                    quiet_steps = 0
                                else:
                                    quiet_steps += 1
                            except Exception:
                                quiet_steps += 1
                        else:
                            quiet_steps += 1

                        if quiet_steps >= QUIET_STEPS_STOP:
                            break

        finally:
            log.info("OZON: detach response listener")
            try:
                self._page.remove_listener("response", on_response)
            except Exception:
                pass

        # Статистика по категориям
        log.info("OZON COLLECT stats per category:")
        for idx, count in sorted(collected_per_category.items()):
            quota = base_quota + (1 if idx < extra else 0)
            log.info("  category %d: collected %d (quota was %d)", idx + 1, count, quota)

        result = list(collected.values())
        log.info("OZON COLLECT done: %d items from %d categories", len(result), num_categories)
        return result
        
    async def collect_skus_by_queries(self, queries: list[str], target: int) -> list[str]:
        """
        Равномерно собирает SKU по списку запросов (категорий/тем).

        target — сколько всего SKU нужно собрать (например 10 или 3000).
        Возвращает список sku строк (digits).
        """
        queries = [q.strip() for q in (queries or []) if str(q).strip()]
        if target <= 0 or not queries:
            return []

        # квоты на запросы
        n = len(queries)
        base = target // n
        extra = target % n

        collected_skus: list[str] = []
        seen: set[str] = set()

        for i, q in enumerate(queries):
            quota = base + (1 if i < extra else 0)
            if quota <= 0:
                continue

            url = self._build_search_url(q)
            log.info("OZON REFILL: query='%s' quota=%d url=%s", q, quota, url)

            # собираем немного больше (quota*2), чтобы компенсировать дубли
            items = await self._collect_from_scroll(seed_urls=[url], target=quota * 2)

            # из items берём external_id
            for it in items:
                sku = str(it.get("external_id") or "").strip()
                if not sku.isdigit():
                    continue
                if sku in seen:
                    continue
                seen.add(sku)
                collected_skus.append(sku)
                if len(collected_skus) >= target:
                    return collected_skus

        return collected_skus

    def _parse_tile_grid(self, page_json: dict) -> list[dict[str, Any]]:
        widget_states = page_json.get("widgetStates") or {}
        items: list[dict[str, Any]] = []

        for key, value in widget_states.items():
            if "tilegrid" not in key.lower():
                continue
            if not isinstance(value, str):
                continue
            try:
                parsed = json.loads(value)
            except Exception:
                continue

            for item in parsed.get("items") or []:
                product = self._parse_tile_item(item)
                if product:
                    items.append(product)

        return items

    def _parse_tile_item(self, item: dict) -> dict[str, Any] | None:
        sku = item.get("sku")
        if not sku:
            return None

        name = None
        price = None
        old_price = None
        discount_percent = None
        rating = None
        feedbacks = None
        image_url = None
        product_url = None

        action = item.get("action") or {}
        link = action.get("link")
        if link:
            product_url = link if link.startswith("http") else "https://www.ozon.ru" + link.split("?")[0]

        tile_image = (item.get("tileImage") or {}).get("items") or []
        if tile_image:
            image_url = (tile_image[0].get("image") or {}).get("link")

        is_card_price = False

        for state in item.get("mainState") or []:
            state_type = state.get("type")

            if state_type == "textAtom":
                ta = state.get("textAtom") or {}
                if not name:
                    name = ta.get("text")

            if state_type == "priceV2":
                pv = state.get("priceV2") or {}

                style_type = ((pv.get("priceStyle") or {}).get("styleType") or "").upper()
                if style_type == "CARD_PRICE":
                    is_card_price = True

                discount_percent = _parse_discount(pv.get("discount"))

                for p in pv.get("price") or []:
                    style = p.get("textStyle")
                    val = _extract_price(p.get("text"))
                    if not val:
                        continue
                    if style == "PRICE":
                        price = val
                    elif style == "ORIGINAL_PRICE":
                        old_price = val

            if state_type == "labelList":
                for label in (state.get("labelList") or {}).get("items") or []:
                    icon = (label.get("icon") or {}).get("image") or ""
                    title = label.get("title") or ""

                    if "star" in icon and rating is None:
                        m = re.search(r"(\d+[.,]\d+|\d+)", title.replace(",", "."))
                        if m:
                            try:
                                rating = float(m.group(1))
                            except Exception:
                                pass

                    if "dialog" in icon and feedbacks is None:
                        digits = re.sub(r"[^\d]", "", title)
                        if digits:
                            try:
                                feedbacks = int(digits)
                            except Exception:
                                pass

        if SKIP_CARD_ONLY_ITEMS and is_card_price and not old_price:
            return None

        if price is None:
            return None

        return {
            "external_id": str(sku),
            "platform": "ozon",
            "name": name,
            "title": name,
            "price": price,
            "old_price": old_price,
            "discount_percent": discount_percent,
            "rating": rating,
            "feedbacks": feedbacks,
            "product_url": product_url,
            "image_url": image_url,
        }

    # =========================================================================
    # MONITOR режим: проверка через API
    # =========================================================================

    async def _monitor_products(self, product_ids: list[int | str]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        error_counts = Counter()
        no_price_count = 0
        errors_count = 0
        recoveries = 0
        total = len(product_ids)

        for idx, sku in enumerate(product_ids, 1):
            sku = str(sku)

            try:
                product = await self._fetch_product_api(sku)

                if product.get("price"):
                    results.append(product)
                    errors_count = 0
                else:
                    if product.get("error"):
                        err = str(product.get("error"))
                        error_counts[err] += 1
                        errors_count += 1
                        log.warning(
                            "OZON: api error for %s: %s (errors подряд=%d)",
                            sku, err, errors_count
                        )
                        await asyncio.sleep(MONITOR_ERROR_DELAY)
                    else:
                        no_price_count += 1
                        errors_count += 1
                        log.debug("OZON: no price for %s", sku)

            except Exception as e:
                log.warning("OZON: exception fetching %s: %s", sku, e)
                errors_count += 1
                await asyncio.sleep(MONITOR_ERROR_DELAY)

            # Если много ошибок подряд — пробуем восстановиться
            if errors_count >= MONITOR_MAX_ERRORS:
                recoveries += 1
                log.error("OZON: too many errors подряд (%d). Recovery #%d", errors_count, recoveries)

                # Если за цикл были 403 — считаем это волной бана/лимита, делаем паузу
                if error_counts.get("403", 0) > 0:
                    log.warning("OZON: 403 wave detected -> cooldown %.0f sec", OZON_403_COOLDOWN_SEC)
                    await asyncio.sleep(OZON_403_COOLDOWN_SEC)

                # Переподключаемся к Chrome/Playwright
                try:
                    await self.close()
                    await asyncio.sleep(2)
                    await self._ensure_connected()
                    errors_count = 0
                    log.info("OZON: recovery reconnect done, continuing monitor")
                except Exception as e:
                    log.error("OZON: recovery reconnect failed: %s", e)

                # Если восстановление не помогло несколько раз — завершаем цикл и ждём следующий запуск scheduler
                if recoveries >= OZON_MAX_RECOVERIES:
                    log.error(
                        "OZON: reached max recoveries (%d). Finishing monitor early.",
                        OZON_MAX_RECOVERIES
                    )
                    break

            if idx % 100 == 0:
                log.info("OZON monitor: %d/%d, success=%d", idx, total, len(results))

            await asyncio.sleep(MONITOR_REQUEST_DELAY)
        
            # Yield каждые 20 товаров — даём боту обработать команды
            if idx % 20 == 0:
                await asyncio.sleep(0)

        log.info("OZON MONITOR done: %d/%d products", len(results), total)
        if error_counts or no_price_count:
            top = ", ".join(f"{k}={v}" for k, v in error_counts.most_common(10))
            log.info(
                "OZON MONITOR stats: success=%d/%d, no_price=%d, errors=[%s]",
                len(results), total, no_price_count, top
            )
        return results

    async def _fetch_product_api(self, sku: str) -> dict[str, Any]:
        slug = f"/product/{sku}/"
        api_url = f"https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url={slug}"

        response = await self._page.evaluate(f"""
            async () => {{
                try {{
                    const resp = await fetch("{api_url}");
                    if (!resp.ok) return {{error: resp.status}};
                    return await resp.json();
                }} catch (e) {{
                    return {{error: e.message}};
                }}
            }}
        """)

        if isinstance(response, dict) and "error" in response:
            # логируем статус/ошибку
            log.debug("OZON API error for %s: %s", sku, response["error"])
            return self._empty_product(sku, error=str(response["error"]))

        #if isinstance(response, dict) and "error" in response:
        #    return self._empty_product(sku, error=str(response["error"]))

        return self._parse_product_api(response, sku)

    def _parse_product_api(self, data: dict, sku: str) -> dict[str, Any]:
        result = self._empty_product(sku)
        widget_states = data.get("widgetStates") or {}

        for key, value in widget_states.items():
            if not isinstance(value, str):
                continue

            try:
                parsed = json.loads(value)
            except Exception:
                continue

            if "webPrice" in key and "Decreased" not in key:
                result["price"] = _extract_price(parsed.get("price"))
                result["card_price"] = _extract_price(parsed.get("cardPrice"))
                result["old_price"] = _extract_price(parsed.get("originalPrice"))
                result["in_stock"] = parsed.get("isAvailable", True)

                if result["price"] and result["old_price"] and result["old_price"] > result["price"]:
                    result["discount_percent"] = round((1 - result["price"] / result["old_price"]) * 100)

            if "webProductHeading" in key:
                result["name"] = parsed.get("title")
                result["title"] = parsed.get("title")

            if "webGallery" in key:
                covers = parsed.get("covers") or []
                if covers:
                    result["image_url"] = covers[0].get("link")

            if "webReviewProductScore" in key:
                result["rating"] = parsed.get("score")
                result["feedbacks"] = parsed.get("count")

        result["product_url"] = f"https://www.ozon.ru/product/{sku}/"
        return result

    def _empty_product(self, sku: str, error: str | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {
            "external_id": str(sku),
            "platform": "ozon",
            "name": None,
            "title": None,
            "price": None,
            "card_price": None,
            "old_price": None,
            "discount_percent": None,
            "rating": None,
            "feedbacks": None,
            "in_stock": None,
            "product_url": f"https://www.ozon.ru/product/{sku}/",
            "image_url": None,
        }
        if error:
            result["error"] = error
        return result