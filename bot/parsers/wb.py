# bot/parsers/wb.py

from __future__ import annotations

import asyncio
import logging
import atexit
import os
import time
import random
from typing import Any, Iterable
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bot.parsers.base import BaseParser

log = logging.getLogger(__name__)

# ============================================================================
# Прокси (опционально)
# ============================================================================

WB_PROXY_URL = os.getenv("WB_PROXY_URL", "").strip()
PROXIES: dict[str, str] | None = None
if WB_PROXY_URL:
    PROXIES = {"http": WB_PROXY_URL, "https": WB_PROXY_URL}

# ============================================================================
# Константы
# ============================================================================

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
BATCH_SIZE = 50  # Товаров за один запрос (WB позволяет до 100)
COOKIES_REFRESH_HOURS = 2  # Обновлять cookies каждые N часов

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Глобальное хранилище cookies
_cookies_cache: dict[str, Any] = {}
_cookies_updated: datetime | None = None


# ============================================================================
# Получение cookies через Selenium
# ============================================================================

def _get_fresh_cookies() -> dict[str, str]:
    """Получает свежие cookies через undetected-chromedriver."""
    global _cookies_cache, _cookies_updated
    
    # Проверяем, нужно ли обновлять
    if _cookies_updated and _cookies_cache:
        age = datetime.now() - _cookies_updated
        if age < timedelta(hours=COOKIES_REFRESH_HOURS):
            log.debug("Using cached cookies (age: %s)", age)
            return _cookies_cache
    
    log.info("Refreshing WB cookies via Selenium...")
    
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
            
            log.info("WB cookies refreshed, count: %d", len(cookies))
            return cookies
            
        finally:
            try:
                driver.quit()
            except:
                pass
                
    except Exception as e:
        log.error("Failed to get WB cookies: %s", e)
        return _cookies_cache or {}


# ============================================================================
# API запросы
# ============================================================================

def _get_headers() -> dict[str, str]:
    return {
        "Accept": "*/*",
        "Accept-Language": "ru,en;q=0.9",
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.wildberries.ru/",
        "x-requested-with": "XMLHttpRequest",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }


def _create_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _create_session()
    return _session


def _fetch_products_batch(nm_ids: list[int]) -> list[dict[str, Any]]:
    """
    Получает данные по batch товаров через внутренний API WB.
    
    Возвращает список продуктов с ценами, рейтингом, остатками.
    """
    if not nm_ids:
        return []
    
    cookies = _get_fresh_cookies()
    if not cookies:
        log.warning("No cookies available, cannot fetch products")
        return []
    
    nm_string = ";".join(str(x) for x in nm_ids)
    url = f"https://www.wildberries.ru/__internal/u-card/cards/v4/detail?appType=1&curr=rub&dest=12354108&spp=30&lang=ru&nm={nm_string}"
    
    session = _get_session()
    
    try:
        resp = session.get(
            url,
            headers=_get_headers(),
            cookies=cookies,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        
        if resp.status_code == 200:
            data = resp.json()
            products = data.get("data", {}).get("products", [])
            if not products:
                products = data.get("products", [])
            return products
            
        elif resp.status_code == 498:
            # Cookies протухли, сбрасываем кэш
            global _cookies_cache, _cookies_updated
            _cookies_cache = {}
            _cookies_updated = None
            log.warning("WB returned 498, cookies expired")
            
        else:
            log.warning("WB API error: %d", resp.status_code)
            
    except Exception as e:
        log.error("WB API request failed: %s", e)
    
    return []


# ============================================================================
# Вспомогательные функции (basket API для картинок)
# ============================================================================

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


def _build_image_url(nm_id: int) -> str:
    vol = nm_id // 100000
    part = nm_id // 1000
    basket = _get_basket_number(vol)
    return f"https://basket-{basket:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/big/1.webp"


# ============================================================================
# Основной класс
# ============================================================================

class WildberriesParser(BaseParser):
    """
    Парсер Wildberries с гибридным подходом:
    - Selenium для получения cookies (раз в 2 часа)
    - Внутренний API WB для данных (быстро, batch запросы)
    """

    def __init__(self, product_ids: Iterable[int] | None = None) -> None:
        if product_ids:
            self._product_ids = [int(i) for i in product_ids]
        else:
            self._product_ids = []

    async def fetch_products(self) -> Iterable[Any]:
        """Возвращает список nm_id для парсинга."""
        if not self._product_ids:
            log.warning("WildberriesParser: product_ids list is empty")
        return list(self._product_ids)

    async def parse_product(self, raw: Any) -> dict[str, Any]:
        """Парсит один товар."""
        nm_id = int(raw) if not isinstance(raw, int) else raw
        
        products = await asyncio.to_thread(_fetch_products_batch, [nm_id])
        
        if products:
            return self._convert_product(products[0])
        
        return {
            "external_id": str(nm_id),
            "platform": "wb",
            "name": f"Товар {nm_id}",
            "price": None,
            "product_url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
            "image_url": _build_image_url(nm_id),
        }

    async def parse_products_batch(self, nm_ids: list[int]) -> list[dict[str, Any]]:
        """Парсит batch товаров за один запрос."""
        results = []
        
        for i in range(0, len(nm_ids), BATCH_SIZE):
            batch = nm_ids[i:i + BATCH_SIZE]
            
            products = await asyncio.to_thread(_fetch_products_batch, batch)
            
            for p in products:
                try:
                    result = self._convert_product(p)
                    results.append(result)
                except Exception as e:
                    log.warning("Failed to convert product: %s", e)
            
            if i + BATCH_SIZE < len(nm_ids):
                await asyncio.sleep(0.5)
        
        return results

    def _convert_product(self, p: dict[str, Any]) -> dict[str, Any]:
        """Конвертирует сырые данные API в наш формат."""
        nm_id = p.get("id")
        
        # Цены
        sizes = p.get("sizes", [])
        price_info = sizes[0].get("price", {}) if sizes else {}
        
        basic_price = price_info.get("basic", 0) / 100
        product_price = price_info.get("product", 0) / 100
        
        # Скидка
        discount = None
        if basic_price and product_price and basic_price > product_price:
            discount = round((1 - product_price / basic_price) * 100, 0)
        
        # Остатки
        total_qty = p.get("totalQuantity", 0)
        
        # Имя
        brand = p.get("brand", "")
        name = p.get("name", "")
        if brand and name:
            full_name = f"{brand} / {name}"
        elif brand:
            full_name = brand
        elif name:
            full_name = name
        else:
            full_name = f"Товар {nm_id}"
        
        return {
            "external_id": str(nm_id),
            "platform": "wb",
            "name": full_name,
            "title": name,
            "brand": brand,
            "supplier": p.get("supplier"),
            "category": p.get("entity"),
            "price": product_price,
            "price_min": product_price,
            "price_max": product_price,
            "old_price": basic_price,
            "discount_percent": discount,
            "stock": total_qty,
            "rating": p.get("reviewRating"),
            "feedbacks": p.get("feedbacks", 0),
            "product_url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
            "image_url": _build_image_url(nm_id),
            "pics": p.get("pics", 1),
        }