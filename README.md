# Telegram‑бот мониторинга цен WB / OZON / Детский Мир (aiogram 3)

Бот собирает базу товаров (по ~3000 на платформу), регулярно мониторит цены/скидки и публикует изменения в Telegram‑канал по заданным порогам. Настройки меняются командами администратора. Работает 24/7 через планировщик.

---

## Возможности

- Платформы: **Wildberries / OZON / Детский Мир**
- База: **~3000 товаров на платформу**
- Мониторинг 24/7 по расписанию (scheduler)
- Публикации в канал по порогам:
  - падение цены на X%+
  - рост скидки на Y п.п.+
- Админ‑управление настройками из Telegram
- Автодобор базы до целевого размера
- Ротация базы (лениво, раз в N дней) + ручной запуск через команды

---

## Требования

- Python 3.11+ (у тебя сейчас 3.13 — допустимо, если зависимости ставятся)
- Chrome/Chromium (нужно для OZON; для WB может использоваться для получения cookies)
- База данных:
  - SQLite — удобно для тестов
  - PostgreSQL — рекомендуется для продакшена

---

## Установка

pip install -r requirements.txt

## Конфигурация (.env)
# Создай файл .env в корне проекта.

Минимально:
BOT_TOKEN=...
DATABASE_DSN=sqlite+aiosqlite:///./parser.db
POSTING_CHANNEL=@your_channel
ADMIN_IDS=123456789

## Включение платформ
ENABLE_WB=true
ENABLE_OZON=true
ENABLE_DETMIR=true

## Интервалы мониторинга (секунды)
# Рекомендуемые стартовые для продакшена:
PARSING_WB_SECONDS=900
PARSING_OZON_SECONDS=3600
PARSING_DETMIR_SECONDS=1800

## Размер базы
TARGET_PRODUCT_COUNT=3000

## Пороги публикации
# Это основной рычаг количества постов:
MIN_PRICE_DROP_PERCENT=10.0
MIN_DISCOUNT_INCREASE=20.0

## Фильтры (отсечение товаров до сравнения)
FILTER_MIN_PRICE=0
FILTER_MAX_PRICE=0
FILTER_MIN_DISCOUNT_PERCENT=0
FILTER_MIN_STOCK=0

## Лимиты постинга
POSTING_MAX_POSTS_PER_HOUR=20
POSTING_DELAY=3.0
SKIP_PRODUCTS_WITHOUT_IMAGE=true

## Детский Мир (DM): собирать только товары “в наличии”
DM_COLLECT_ONLY_IN_STOCK=true
DM_COLLECT_STOCK_CHECK_CONCURRENCY=50
DM_COLLECT_STOCK_CHECK_TIMEOUT=20
DM_COLLECT_STOCK_CHECK_MAX_PASSES=3

## Ротации базы (ленивые, раз в N дней)
# Детский мир
DM_ROTATION_ENABLED=true
DM_ROTATION_DAYS=7
DM_ROTATION_FRACTION=0.2
DM_ROTATION_MAX_ATTEMPTS=6

# WB
WB_ROTATION_ENABLED=true
WB_ROTATION_DAYS=7
WB_ROTATION_FRACTION=0.2
WB_ROTATION_REMOVE_OLDEST=true

# OZON
OZON_ROTATION_ENABLED=true
OZON_ROTATION_DAYS=7
OZON_ROTATION_FRACTION=0.2

## OZON: Chrome CDP
OZON_CDP_URL=http://localhost:9222

## Запуск
python -m bot

# После запуска:
- polling стартует сразу
- планировщик запускает WB/OZON/DM задачи по расписанию
- при недоборе база автоматически добирается до TARGET_PRODUCT_COUNT

## Как работает (кратко)
# Общий цикл
1. Берём список товаров платформы из БД
2. MONITOR: получаем цены/скидки
3. Фильтруем (цена/скидка/наличие и т.д.)
4. Детектим изменения (через baseline + стабилизацию)
5. Публикуем в канал по порогам
6. Ротация/добор базы по правилам

# Детский Мир (DM)
- MONITOR через API https://api.detmir.ru/v2/products/<id>
- COLLECT по detmir_slugs (slug’и категорий)
- При DM_COLLECT_ONLY_IN_STOCK=true в базу попадают только товары “в наличии” (по available)

# OZON
- MONITOR через API entrypoint-api.bx/...
- COLLECT через Playwright/CDP (scroll)
- Recovery при волнах 403
- Ротация заменяет 20% и добирает обратно до 3000

# WB
- MONITOR через внутренний API + cookies
- Ротация заменяет 20% и добирает обратно до 3000 через WB refill

## Админ‑команды (Telegram)
# Справка/статус
/help_admin — список команд
/settings — текущие настройки
/stats — статистика по базе/изменениям
/myid — показать свой Telegram ID

# Пороги публикации
/set_price_drop 15
/set_discount_increase 25

# Фильтры
/set_min_price 1000
/set_max_price 30000
/set_min_discount 30

# Категории для добора (общие, WB/OZON)
/categories
/add_category телевизор
/remove_category телевизор
/clear_categories

# DM slugs (категории Детского Мира)
/dm_slugs
/dm_add_slug detskaya_obuv
/dm_remove_slug detskaya_obuv

# Ручная ротация 20%
/dm_refresh
/wb_refresh
/ozon_refresh

## Сервисные скрипты
- reset_db.py — очистка БД по платформам или полностью:
python reset_db.py