# quick_test.py
import asyncio
from bot.parsers.wb import WildberriesParser

async def test():
    parser = WildberriesParser()
    
    # Любые реальные артикулы WB (возьмите из вашей БД или с сайта)
    ids = [88851570, 265851592, 259897681]
    
    print("Первый запрос:")
    data1 = await parser.parse_products_batch(ids)
    for item in data1:
        print(f"  {item['external_id']}: {item['price']}₽, скидка {item['discount_percent']}%")
    
    print("\nЖдём 60 сек...")
    await asyncio.sleep(60)
    
    print("Второй запрос:")
    data2 = await parser.parse_products_batch(ids)
    for item in data2:
        print(f"  {item['external_id']}: {item['price']}₽, скидка {item['discount_percent']}%")

asyncio.run(test())