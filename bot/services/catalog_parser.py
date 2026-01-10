#bot/services/catalog_parser.py
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any

import requests

log = logging.getLogger(__name__)

# Глобальный кэш cookies
_cookies_cache: dict[str, str] = {}
_cookies_updated: datetime | None = None
COOKIES_REFRESH_HOURS = 2


def _get_fresh_cookies() -> dict[str, str]:
    """Получает свежие cookies через Selenium."""
    global _cookies_cache, _cookies_updated
    
    if _cookies_updated and _cookies_cache:
        age = datetime.now() - _cookies_updated
        if age < timedelta(hours=COOKIES_REFRESH_HOURS):
            return _cookies_cache
    
    log.info("Refreshing WB cookies for catalog...")
    
    try:
        import undetected_chromedriver as uc
        
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        driver = uc.Chrome(options=options)
        
        try:
            driver.get("https://www.wildberries.ru/")
            time.sleep(5)
            
            cookies = {}
            for cookie in driver.get_cookies():
                cookies[cookie['name']] = cookie['value']
            
            _cookies_cache = cookies
            _cookies_updated = datetime.now()
            
            log.info(f"Cookies refreshed: {len(cookies)}")
            return cookies
            
        finally:
            try:
                driver.quit()
            except:
                pass
                
    except Exception as e:
        log.error(f"Failed to get cookies: {e}")
        return _cookies_cache or {}


class CatalogParser:
    """
    Парсер каталога WB.
    
    Собирает артикулы товаров по поисковым запросам или категориям.
    """
    
    def __init__(self):
        self._headers = {
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.wildberries.ru/",
            "x-requested-with": "XMLHttpRequest",
        }
        self._base_url = "https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search"
    
    async def search_products(
        self,
        query: str,
        max_products: int = 1000,
        dest: str = "-3827418",
    ) -> list[int]:
        """
        Ищет товары по запросу и возвращает список артикулов.
        
        Args:
            query: Поисковый запрос (например "смартфон", "платье")
            max_products: Максимальное количество товаров
            dest: Регион доставки
            
        Returns:
            Список артикулов (nm_id)
        """
        cookies = await asyncio.to_thread(_get_fresh_cookies)
        
        if not cookies:
            log.error("No cookies available")
            return []
        
        all_ids: list[int] = []
        page = 1
        max_pages = (max_products // 100) + 1
        
        log.info(f"Searching: '{query}', max {max_products} products")
        
        while len(all_ids) < max_products and page <= max_pages:
            params = {
                "ab_testing": "false",
                "appType": "1",
                "curr": "rub",
                "dest": dest,
                "lang": "ru",
                "page": str(page),
                "query": query,
                "resultset": "catalog",
                "sort": "popular",
                "spp": "30",
            }
            
            try:
                response = await asyncio.to_thread(
                    lambda: requests.get(
                        self._base_url,
                        params=params,
                        headers=self._headers,
                        cookies=cookies,
                        timeout=15,
                    )
                )
                
                if response.status_code == 200:
                    data = response.json()
                    products = data.get("products", [])
                    
                    if not products:
                        log.info(f"Page {page}: empty, stopping")
                        break
                    
                    page_ids = [p.get("id") for p in products if p.get("id")]
                    all_ids.extend(page_ids)
                    
                    log.debug(f"Page {page}: {len(page_ids)} products (total: {len(all_ids)})")
                    
                elif response.status_code == 498:
                    log.warning("498 - cookies expired, refreshing...")
                    global _cookies_cache, _cookies_updated
                    _cookies_cache = {}
                    _cookies_updated = None
                    cookies = await asyncio.to_thread(_get_fresh_cookies)
                    continue
                    
                else:
                    log.warning(f"Page {page}: HTTP {response.status_code}")
                    break
                    
            except Exception as e:
                log.error(f"Page {page}: {e}")
                break
            
            page += 1
            await asyncio.sleep(0.3)  # Rate limiting
        
        # Обрезаем до max_products
        result = all_ids[:max_products]
        log.info(f"Search '{query}': collected {len(result)} products")
        
        return result
    
    async def collect_from_queries(
        self,
        queries: list[str],
        products_per_query: int = 500,
    ) -> list[int]:
        """
        Собирает артикулы по нескольким запросам.
        
        Args:
            queries: Список поисковых запросов
            products_per_query: Товаров с каждого запроса
            
        Returns:
            Уникальный список артикулов
        """
        all_ids: set[int] = set()
        
        for query in queries:
            ids = await self.search_products(query, max_products=products_per_query)
            all_ids.update(ids)
            log.info(f"After '{query}': total unique {len(all_ids)}")
            await asyncio.sleep(1)  # Пауза между запросами
        
        return list(all_ids)


async def collect_products_for_monitoring(
    queries: list[str],
    target_count: int = 3000,
) -> list[int]:
    """
    Удобная функция для сбора товаров.
    
    Args:
        queries: Список поисковых запросов/категорий
        target_count: Целевое количество товаров
        
    Returns:
        Список артикулов
    """
    parser = CatalogParser()
    
    # Распределяем товары по запросам
    per_query = target_count // len(queries) + 100
    
    all_ids = await parser.collect_from_queries(queries, products_per_query=per_query)
    
    # Обрезаем до целевого количества
    return all_ids[:target_count]