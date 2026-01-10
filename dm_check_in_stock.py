import asyncio
import json
from typing import Any

import aiohttp
from sqlalchemy import select

from bot.config import load_settings
from bot.db import create_engine, create_sessionmaker
from bot.db.models import PlatformCode, Platform, Product


SAMPLE = 200          # сколько товаров проверить из БД
CONCURRENCY = 30      # параллельность запросов к api.detmir.ru
SHOW_EXAMPLES = 10    # сколько примеров показать для in_stock True/False


def compute_in_stock_and_stock(item: dict[str, Any]) -> tuple[bool | None, int | None]:
    """
    Та же логика, что мы добавляем в DetmirParser:
    in_stock True, если есть хотя бы один онлайн-склад или оффлайн регионы/магазины.
    stock = 1/0 условный.
    """
    available = item.get("available")

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


async def fetch_one(session: aiohttp.ClientSession, pid: str) -> dict[str, Any] | None:
    url = f"https://api.detmir.ru/v2/products/{pid}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                return {"id": pid, "status": r.status, "error": "http"}
            text = await r.text()
            data = json.loads(text)
            return {"id": pid, "status": 200, "data": data}
    except Exception as e:
        return {"id": pid, "status": None, "error": str(e)}


async def main() -> None:
    settings = load_settings()
    engine = create_engine(settings.postgres_dsn)
    session_factory = create_sessionmaker(engine)

    # Берём SAMPLE external_id из БД для DM
    async with session_factory() as db:
        platform = (await db.execute(
            select(Platform).where(Platform.code == PlatformCode.DM)
        )).scalar_one_or_none()

        if not platform:
            print("DM platform not found in DB")
            await engine.dispose()
            return

        ids = (await db.execute(
            select(Product.external_id)
            .where(Product.platform_id == platform.id)
            .order_by(Product.id.desc())
            .limit(SAMPLE)
        )).scalars().all()

    if not ids:
        print("No DM products in DB")
        await engine.dispose()
        return

    print(f"Checking DM products: {len(ids)} items")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://www.detmir.ru",
        "Referer": "https://www.detmir.ru/",
    }

    sem = asyncio.Semaphore(CONCURRENCY)
    results: list[dict[str, Any]] = []

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        async def one(pid: str) -> None:
            async with sem:
                results.append(await fetch_one(session, pid))

        tasks = [asyncio.create_task(one(str(pid))) for pid in ids]
        for i, fut in enumerate(asyncio.as_completed(tasks), 1):
            await fut
            if i % 50 == 0 or i == len(tasks):
                print(f"Progress: {i}/{len(tasks)}")

    ok = 0
    http_err = 0
    parse_err = 0
    instock_true: list[tuple[str, str]] = []
    instock_false: list[tuple[str, str]] = []

    for r in results:
        if not r:
            parse_err += 1
            continue
        if r.get("status") != 200:
            http_err += 1
            continue

        data = r.get("data") or {}
        item = data.get("item")
        if not isinstance(item, dict):
            parse_err += 1
            continue

        in_stock, stock = compute_in_stock_and_stock(item)
        ok += 1

        title = str(item.get("title") or "")[:80]
        if in_stock is True:
            if len(instock_true) < SHOW_EXAMPLES:
                instock_true.append((r["id"], title))
        elif in_stock is False:
            if len(instock_false) < SHOW_EXAMPLES:
                instock_false.append((r["id"], title))

    print("\n=== Summary ===")
    print(f"ok(parsed): {ok}")
    print(f"http errors: {http_err}")
    print(f"parse errors: {parse_err}")
    print(f"in_stock True examples: {len(instock_true)}")
    print(f"in_stock False examples: {len(instock_false)}")

    print("\n=== Examples: in_stock=False (likely 'нет в наличии') ===")
    for pid, title in instock_false:
        print(f"- id={pid} | {title} | https://www.detmir.ru/product/index/id/{pid}/")

    print("\n=== Examples: in_stock=True ===")
    for pid, title in instock_true:
        print(f"- id={pid} | {title} | https://www.detmir.ru/product/index/id/{pid}/")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())