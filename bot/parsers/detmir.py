# bot/parsers/detmir.py
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Iterable

import aiohttp
from bs4 import BeautifulSoup

from bot.parsers.base import BaseParser

log = logging.getLogger(__name__)

# ===== Настройки =====
DM_COLLECT_TARGET = int(os.getenv("DM_COLLECT_TARGET", "3000"))
DM_COLLECT_PAGES_PER_CATEGORY = int(os.getenv("DM_COLLECT_PAGES_PER_CATEGORY", "50"))
DM_REQUEST_TIMEOUT = int(os.getenv("DM_REQUEST_TIMEOUT", "30"))
DM_REQUEST_DELAY = float(os.getenv("DM_REQUEST_DELAY", "0.15"))

DM_COLLECT_MAX_EMPTY_PAGES = int(os.getenv("DM_COLLECT_MAX_EMPTY_PAGES", "2"))
DM_COLLECT_LOG_EVERY = int(os.getenv("DM_COLLECT_LOG_EVERY", "1"))
DM_COLLECT_CONCURRENCY = int(os.getenv("DM_COLLECT_CONCURRENCY", "6"))

DM_COLLECT_YIELD_EVERY_BATCH = int(os.getenv("DM_COLLECT_YIELD_EVERY_BATCH", "2"))
DM_COLLECT_MAX_NO_NEW_PAGES = int(os.getenv("DM_COLLECT_MAX_NO_NEW_PAGES", "6"))

# Новый режим: держать в БД только товары "в наличии"
DM_COLLECT_ONLY_IN_STOCK = os.getenv("DM_COLLECT_ONLY_IN_STOCK", "false").lower() in ("1", "true", "yes", "y", "on")
DM_COLLECT_STOCK_CHECK_CONCURRENCY = int(os.getenv("DM_COLLECT_STOCK_CHECK_CONCURRENCY", "50"))
DM_COLLECT_STOCK_CHECK_TIMEOUT = int(os.getenv("DM_COLLECT_STOCK_CHECK_TIMEOUT", "20"))
DM_COLLECT_STOCK_CHECK_MAX_PASSES = int(os.getenv("DM_COLLECT_STOCK_CHECK_MAX_PASSES", "3"))

DEFAULT_DM_CATEGORY_SLUGS = [
    "igry_i_igrushki",
    "children_clothes",
    "hygiene_care",
    "diapers_pants",
    "bottles_cups",
    "hobbies_creativity",
    "knigy",
    "dom",
    "childrens_room",
    "kolyaski",
    "avtokresla",
    "konstruktory",
    "myagkie_igrushki",
    "nastolnye_igry",
    "all_mom",
    "bady_pravilnoe_i_sportivnoe_pitanie",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": "https://www.detmir.ru/",
}

DM_API_BASE = "https://api.detmir.ru/v2/products"

DM_API_HEADERS = {
    **HEADERS,
    "Accept": "application/json,text/plain,*/*",
    "Origin": "https://www.detmir.ru",
    "Referer": "https://www.detmir.ru/",
}


def _build_product_url(product_id: str | int) -> str:
    return f"https://www.detmir.ru/product/index/id/{product_id}/"


def _build_category_url(slug: str, page: int) -> str:
    # предсказуемая пагинация
    return f"https://www.detmir.ru/catalog/index/name/{slug}/?order=date-desc&page={page}"


def _extract_product_ids_from_html(html: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("main") or soup
    ids: set[str] = set()

    for a in root.find_all("a", href=True):
        href = a.get("href") or ""
        m = re.search(r"/product/index/id/(\d+)/", href)
        if m:
            ids.add(m.group(1))

    return ids


def _extract_json_ld_product(html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (s.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict) and obj.get("@type") == "Product":
                    return obj
            continue

        if isinstance(data, dict) and data.get("@type") == "Product":
            return data

    return None


def _safe_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _compute_in_stock_and_stock(available: Any) -> tuple[bool | None, int | None]:
    """
    DM API не даёт точный остаток, но даёт структуру доступности.
    Возвращаем:
      in_stock: True/False/None
      stock: 1/0/None (условный, чтобы min_stock работал)
    """
    if available is None:
        return None, None

    if not isinstance(available, dict):
        in_stock = bool(available)
        return in_stock, (1 if in_stock else 0)

    online = available.get("online")
    offline = available.get("offline")

    online_ok = False
    if isinstance(online, dict):
        wh = online.get("warehouse_codes")
        online_ok = isinstance(wh, list) and len(wh) > 0

    offline_ok = False
    if isinstance(offline, dict):
        regions = offline.get("region_iso_codes")
        stores = offline.get("stores")
        offline_ok = (isinstance(regions, list) and len(regions) > 0) or (isinstance(stores, list) and len(stores) > 0)

    in_stock = online_ok or offline_ok
    stock = 1 if in_stock else 0
    return in_stock, stock


class DetmirParser(BaseParser):
    def __init__(self, product_ids: Iterable[int | str] | None = None) -> None:
        self._product_ids = [str(x) for x in product_ids] if product_ids else []

    async def fetch_products(self) -> Iterable[Any]:
        return list(self._product_ids)

    async def parse_product(self, raw: Any) -> dict[str, Any]:
        product_id = str(raw).strip()
        product_url = _build_product_url(product_id)

        api_url = f"{DM_API_BASE}/{product_id}"
        data = await self._fetch_json(api_url)
        if isinstance(data, dict):
            parsed = self._parse_api_item(product_id, data)
            if parsed:
                return parsed

        html = await self._fetch_html(product_url)
        if not html:
            return self._empty_product(product_id, url=product_url, error="fetch_failed")

        p = _extract_json_ld_product(html)
        if not p:
            return self._empty_product(product_id, url=product_url, error="no_json_ld_product")

        name = p.get("name")
        image_url = p.get("image")
        offers = p.get("offers") or {}
        if isinstance(offers, list) and offers:
            offers = offers[0]

        price = _safe_int(offers.get("price"))
        availability = str(offers.get("availability") or "")
        in_stock = "instock" in availability.lower()

        agg = p.get("aggregateRating") or {}
        rating = _safe_float(agg.get("ratingValue"))
        feedbacks = _safe_int(agg.get("reviewCount"))

        return {
            "external_id": product_id,
            "platform": "detmir",
            "name": name,
            "title": name,
            "price": price,
            "old_price": None,
            "discount_percent": None,
            "stock": 1 if in_stock else 0,
            "rating": rating,
            "feedbacks": feedbacks,
            "in_stock": in_stock,
            "product_url": product_url,
            "image_url": image_url,
            "error": "api_failed_used_jsonld",
        }

    async def parse_products_batch(
        self,
        product_ids: list[int | str],
        collect_slugs: list[str] | None = None,
        collect_target: int | None = None,
    ) -> list[dict[str, Any]]:
        if not product_ids:
            log.info("DETMIR: COLLECT mode")

            slugs = collect_slugs or DEFAULT_DM_CATEGORY_SLUGS
            slugs = [s.strip() for s in slugs if isinstance(s, str) and s.strip()]

            target = int(collect_target or DM_COLLECT_TARGET)

            # 1) собираем кандидатов (может включать out-of-stock)
            candidates = await self.collect_ids_from_categories(
                slugs=slugs,
                target=target,
                pages_per_category=DM_COLLECT_PAGES_PER_CATEGORY,
            )

            # 2) фильтруем "только в наличии" (если включено)
            if DM_COLLECT_ONLY_IN_STOCK:
                log.warning(
                    "DETMIR COLLECT: only_in_stock enabled -> validating via API (target=%d)",
                    target
                )
                candidates = await self._filter_ids_only_in_stock(candidates, target=target)

            return [
                {
                    "external_id": pid,
                    "platform": "detmir",
                    "product_url": _build_product_url(pid),
                    "name": None,
                    "title": None,
                    "price": None,
                    "old_price": None,
                    "discount_percent": None,
                    "stock": None,
                    "rating": None,
                    "feedbacks": None,
                    "in_stock": None,
                    "image_url": None,
                }
                for pid in candidates[:target]
            ]

        # ===== MONITOR =====
        log.info("DETMIR: MONITOR mode (%d products)", len(product_ids))

        sem = asyncio.Semaphore(int(os.getenv("DM_MONITOR_CONCURRENCY", "25")))
        results: list[dict[str, Any]] = []

        timeout = aiohttp.ClientTimeout(total=DM_REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=int(os.getenv("DM_MONITOR_CONCURRENCY", "25")), ttl_dns_cache=300)

        async with aiohttp.ClientSession(timeout=timeout, headers=DM_API_HEADERS, connector=connector) as session:

            async def one(pid: str) -> None:
                async with sem:
                    try:
                        api_url = f"{DM_API_BASE}/{pid}"
                        data = await self._fetch_json_with_session(session, api_url)
                        if isinstance(data, dict):
                            parsed = self._parse_api_item(pid, data)
                            if parsed:
                                results.append(parsed)
                                return

                        item = await self.parse_product(pid)
                        results.append(item)
                    except Exception as e:
                        log.warning("DETMIR: failed to parse %s: %s", pid, e)

            tasks = [asyncio.create_task(one(str(x))) for x in product_ids]
            await asyncio.gather(*tasks)

        return results

    async def _filter_ids_only_in_stock(self, ids: list[str], *, target: int) -> list[str]:
        """
        Оставляет только те ids, которые API считает in_stock=True.
        Делает несколько проходов, если не хватает из-за out-of-stock/ошибок.
        """
        ids = [str(x).strip() for x in ids if str(x).strip().isdigit()]
        if not ids:
            return []

        # Чтобы компенсировать out-of-stock и дубли, даём несколько проходов
        passes = max(1, DM_COLLECT_STOCK_CHECK_MAX_PASSES)

        kept: list[str] = []
        seen: set[str] = set()

        timeout = aiohttp.ClientTimeout(total=DM_COLLECT_STOCK_CHECK_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=DM_COLLECT_STOCK_CHECK_CONCURRENCY, ttl_dns_cache=300)

        async with aiohttp.ClientSession(timeout=timeout, headers=DM_API_HEADERS, connector=connector) as session:
            sem = asyncio.Semaphore(DM_COLLECT_STOCK_CHECK_CONCURRENCY)

            async def check_one(pid: str) -> bool:
                async with sem:
                    data = await self._fetch_json_with_session(session, f"{DM_API_BASE}/{pid}")
                    if not isinstance(data, dict):
                        return False
                    item = data.get("item")
                    if not isinstance(item, dict):
                        return False
                    in_stock, _stock = _compute_in_stock_and_stock(item.get("available"))
                    return in_stock is True

            # проходы: пытаемся добрать target, пока есть кандидаты
            remaining = ids[:]
            for pnum in range(1, passes + 1):
                if len(kept) >= target:
                    break
                if not remaining:
                    break

                # берём порцию кандидатов с запасом
                need = target - len(kept)
                batch_size = min(len(remaining), max(need * 10, 200))
                portion = remaining[:batch_size]
                remaining = remaining[batch_size:]

                checks = [asyncio.create_task(check_one(pid)) for pid in portion]

                done = 0
                for fut, pid in zip(checks, portion):
                    ok = await fut
                    done += 1
                    if ok and pid not in seen:
                        seen.add(pid)
                        kept.append(pid)
                        if len(kept) >= target:
                            break

                    if done % 200 == 0:
                        await asyncio.sleep(0)

                log.info(
                    "DETMIR COLLECT in_stock filter pass %d/%d: kept=%d/%d (checked=%d)",
                    pnum, passes, len(kept), target, done
                )

        if len(kept) < target:
            log.warning("DETMIR COLLECT only_in_stock: not enough in_stock items: %d/%d", len(kept), target)

        return kept[:target]

    async def collect_ids_from_categories(
        self,
        *,
        slugs: list[str],
        target: int,
        pages_per_category: int,
    ) -> list[str]:
        slugs = [s.strip() for s in (slugs or []) if s.strip()]
        if not slugs or target <= 0:
            return []

        n = len(slugs)
        base = target // n
        extra = target % n

        seen: set[str] = set()
        out: list[str] = []

        timeout = aiohttp.ClientTimeout(total=DM_REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=DM_COLLECT_CONCURRENCY, ttl_dns_cache=300)

        async with aiohttp.ClientSession(timeout=timeout, headers=HEADERS, connector=connector) as session:
            for i, slug in enumerate(slugs):
                quota = base + (1 if i < extra else 0)
                if quota <= 0:
                    continue

                start_len = len(out)
                log.info("DETMIR COLLECT: slug=%s quota=%d (have=%d/%d)", slug, quota, len(out), target)

                test_url = _build_category_url(slug, 1)
                status, final_url, test_html = await self._fetch_html_status_with_session(session, test_url)

                if status == 404:
                    log.warning("DETMIR COLLECT: slug=%s -> 404 (url=%s). Skipping slug.", slug, test_url)
                    continue

                if not test_html:
                    log.warning(
                        "DETMIR COLLECT: slug=%s unavailable (status=%s final_url=%s). Skipping slug.",
                        slug, status, final_url
                    )
                    continue

                empty_pages = 0
                no_new_pages = 0
                page = 1

                while page <= pages_per_category and (len(out) - start_len) < quota and len(out) < target:
                    batch_pages = list(range(page, min(page + DM_COLLECT_CONCURRENCY, pages_per_category + 1)))
                    urls = [_build_category_url(slug, p) for p in batch_pages]

                    html_list = await asyncio.gather(
                        *[self._fetch_html_with_session(session, u) for u in urls],
                        return_exceptions=True
                    )

                    for p, u, html in zip(batch_pages, urls, html_list):
                        if isinstance(html, Exception) or not html:
                            empty_pages += 1
                            if p % DM_COLLECT_LOG_EVERY == 0:
                                log.info("DETMIR %s page=%d -> empty (empty_pages=%d)", slug, p, empty_pages)
                            if empty_pages >= DM_COLLECT_MAX_EMPTY_PAGES:
                                log.info("DETMIR %s: stop, %d empty pages подряд", slug, empty_pages)
                                break
                            continue

                        ids = _extract_product_ids_from_html(html)
                        if not ids:
                            empty_pages += 1
                            if p % DM_COLLECT_LOG_EVERY == 0:
                                log.info("DETMIR %s page=%d -> 0 ids (empty_pages=%d)", slug, p, empty_pages)
                            if empty_pages >= DM_COLLECT_MAX_EMPTY_PAGES:
                                log.info("DETMIR %s: stop, %d empty pages подряд", slug, empty_pages)
                                break
                            continue

                        empty_pages = 0
                        new_here = 0
                        for pid in ids:
                            if pid in seen:
                                continue
                            seen.add(pid)
                            out.append(pid)
                            new_here += 1
                            if (len(out) - start_len) >= quota or len(out) >= target:
                                break

                        if new_here == 0:
                            no_new_pages += 1
                        else:
                            no_new_pages = 0

                        if no_new_pages >= DM_COLLECT_MAX_NO_NEW_PAGES:
                            log.info(
                                "DETMIR %s: stop, %d pages подряд без новых товаров (last_page=%d)",
                                slug, no_new_pages, p
                            )
                            empty_pages = DM_COLLECT_MAX_EMPTY_PAGES
                            break

                        if p % DM_COLLECT_LOG_EVERY == 0:
                            log.info(
                                "DETMIR %s page=%d ids=%d new=%d progress=%d/%d (slug_quota=%d)",
                                slug, p, len(ids), new_here, len(out), target, quota
                            )

                        await asyncio.sleep(DM_REQUEST_DELAY)

                        if (len(out) - start_len) >= quota or len(out) >= target:
                            break

                    if empty_pages >= DM_COLLECT_MAX_EMPTY_PAGES:
                        break

                    if (page // DM_COLLECT_CONCURRENCY) % DM_COLLECT_YIELD_EVERY_BATCH == 0:
                        await asyncio.sleep(0)

                    page += DM_COLLECT_CONCURRENCY

                got = len(out) - start_len
                log.info("DETMIR COLLECT DONE slug=%s got=%d/%d (total=%d/%d)", slug, got, quota, len(out), target)

                if len(out) >= target:
                    break

            return out[:target]

    async def _fetch_html_with_session(self, session: aiohttp.ClientSession, url: str) -> str | None:
        try:
            async with session.get(url, allow_redirects=True) as r:
                if r.status != 200:
                    log.warning("DETMIR HTTP %s for %s -> final_url=%s", r.status, url, str(r.url))
                    return None
                text = await r.text()
                if not text:
                    log.warning("DETMIR empty body for %s -> final_url=%s", url, str(r.url))
                    return None
                return text
        except Exception as e:
            log.warning("DETMIR fetch failed: %s (%s)", url, e)
            return None

    async def _fetch_html_status_with_session(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> tuple[int | None, str | None, str | None]:
        try:
            async with session.get(url, allow_redirects=True) as r:
                status = r.status
                final_url = str(r.url)
                if status != 200:
                    return status, final_url, None
                text = await r.text()
                if not text:
                    return status, final_url, None
                return status, final_url, text
        except Exception:
            return None, None, None

    async def _fetch_json_with_session(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> dict[str, Any] | None:
        try:
            async with session.get(url, allow_redirects=True) as r:
                if r.status != 200:
                    return None
                return json.loads(await r.text())
        except Exception:
            return None

    async def _fetch_json(self, url: str) -> dict[str, Any] | None:
        timeout = aiohttp.ClientTimeout(total=DM_REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout, headers=DM_API_HEADERS) as s:
            return await self._fetch_json_with_session(s, url)

    async def _fetch_html(self, url: str) -> str | None:
        timeout = aiohttp.ClientTimeout(total=DM_REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout, headers=HEADERS) as s:
            return await self._fetch_html_with_session(s, url)

    def _parse_api_item(self, product_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        item = data.get("item")
        if not isinstance(item, dict):
            return None

        title = item.get("title")

        price = None
        price_obj = item.get("price")
        if isinstance(price_obj, dict):
            price = _safe_int(price_obj.get("price"))

        old_price = None
        prices_obj = item.get("prices")
        if isinstance(prices_obj, dict):
            old_price = _safe_int(prices_obj.get("old"))
            sale_price = _safe_int(prices_obj.get("sale"))
            if sale_price is not None:
                price = sale_price

        discount_percent = None
        if price and old_price and old_price > price:
            discount_percent = round((1 - price / old_price) * 100)

        rating = _safe_float(item.get("rating"))
        feedbacks = _safe_int(item.get("review_count"))

        in_stock, stock = _compute_in_stock_and_stock(item.get("available"))

        image_url = None
        pics = item.get("pictures") or []
        if isinstance(pics, list) and pics:
            p0 = pics[0]
            if isinstance(p0, dict):
                image_url = p0.get("original") or p0.get("web") or p0.get("thumbnail")
            elif isinstance(p0, str):
                image_url = p0

        product_url = _build_product_url(product_id)
        link = item.get("link")
        if isinstance(link, str) and link.startswith("http"):
            product_url = link

        return {
            "external_id": product_id,
            "platform": "detmir",
            "name": title,
            "title": title,
            "price": price,
            "old_price": old_price,
            "discount_percent": discount_percent,
            "stock": stock,
            "rating": rating,
            "feedbacks": feedbacks,
            "in_stock": in_stock,
            "product_url": product_url,
            "image_url": image_url,
        }

    def _empty_product(self, product_id: str, *, url: str, error: str) -> dict[str, Any]:
        return {
            "external_id": product_id,
            "platform": "detmir",
            "name": None,
            "title": None,
            "price": None,
            "old_price": None,
            "discount_percent": None,
            "stock": None,
            "rating": None,
            "feedbacks": None,
            "in_stock": None,
            "product_url": url,
            "image_url": None,
            "error": error,
        }