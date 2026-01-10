# export_chrome_cookies.py
"""
Экспортирует cookies из Chrome для Ozon.
Требует: pip install browser-cookie3
"""

import json

try:
    import browser_cookie3
    
    print("Извлекаю cookies из Chrome...")
    
    # Получаем cookies для ozon.ru
    cookies = browser_cookie3.chrome(domain_name='.ozon.ru')
    
    cookie_list = []
    for cookie in cookies:
        cookie_list.append({
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain,
            "path": cookie.path,
            "secure": cookie.secure,
        })
    
    # Сохраняем в файл
    with open("ozon_cookies.json", "w", encoding="utf-8") as f:
        json.dump(cookie_list, f, indent=2, ensure_ascii=False)
    
    print(f"Сохранено {len(cookie_list)} cookies в ozon_cookies.json")
    
except ImportError:
    print("Установите: pip install browser-cookie3")
except Exception as e:
    print(f"Ошибка: {e}")
    print("\nПопробуйте закрыть Chrome и запустить снова")