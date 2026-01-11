# OZON модуль — техническая документация

Документ описывает OZON‑часть проекта: сбор SKU (COLLECT), мониторинг (MONITOR), интеграцию с пайплайном и БД, конфигурацию через ENV и диагностику на VPS.

> Важно: в документе не используются реальные токены/пароли/DSN/прокси. Везде используются плейсхолдеры.

---

## 1) Назначение OZON‑модуля

OZON‑модуль выполняет две задачи:

1) **COLLECT** — сбор SKU (external_id) для наполнения базы товаров до целевого размера (обычно `TARGET_PRODUCT_COUNT`, например 3000).
2) **MONITOR** — мониторинг цен/скидок/картинок по списку SKU из базы и фиксация изменений в БД (через `change_detection`), затем отбор событий для публикации в Telegram‑канал.

Модуль рассчитан на 24/7 работу в составе бота и регулярно запускается планировщиком.

---

## 2) Поток выполнения (Flow)

### 2.1 Высокоуровневый flow

1. `bot/main.py` поднимает:
   - конфиг/ENV, БД, менеджеры (settings/product),
   - Telegram polling,
   - scheduler (APScheduler).

2. Scheduler вызывает функцию‑задачу OZON:
   - получает свежие `product_ids` из БД,
   - создаёт `OzonParser(product_ids=...)`,
   - запускает `pipeline.run_platform(platform=OZON, parser=OzonParser)`.

3. `PipelineRunner`:
   - решает MONITOR или COLLECT (в зависимости от наличия IDs в БД),
   - запускает парсер,
   - фильтрует результаты (общие фильтры),
   - пишет изменения в БД (`detect_and_save_changes`),
   - выбирает события для публикации,
   - постит через `PostingService`.

4. (Опционально) авто‑удаление/добор/ротация:
   - может удалять часть товаров и добирать новых через COLLECT.

### 2.2 Текстовая диаграмма
APScheduler job
└─ ozon_job() in bot/main.py
├─ read OZON IDs from DB
├─ OzonParser(product_ids)
└─ PipelineRunner.run_platform(OZON, parser)
├─ parser.fetch_products()
├─ parse (MONITOR or COLLECT)
├─ FilterService.filter_products_async()
├─ detect_and_save_changes()
├─ select_for_publish()
└─ PostingService.post_product()

text


---

## 3) Файлы OZON‑части и назначение каждого

### 3.1 `bot/parsers/ozon.py`
Основной парсер OZON.

Функционал:
- Подключение к браузеру (варианты: через CDP к Chrome или запуск Playwright Chromium).
- Режим **COLLECT**:
  - обход категорий/запросов,
  - infinite scroll,
  - сбор SKU из network (`widgetStates/tileGrid`) + fallback из DOM (`a[href*='/product/']`),
  - равномерное распределение по категориям (квоты).
- Режим **MONITOR**:
  - запрос JSON через browser `fetch` к endpoint:
    `https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=/product/<sku>/`
  - нормализация результата (price/old_price/discount/rating/feedbacks/image_url),
  - обработка ошибок (`403` волны, cooldown, reconnect),
  - rate limiting.

Публичные методы:
- `fetch_products()`
- `parse_product(raw)`
- `parse_products_batch(product_ids, collect_queries=None)`
- `close()`

### 3.2 `bot/utils/chrome_manager.py`
Управление “внешним” Chrome для CDP‑режима (если используется).

Функционал:
- Поиск Chrome/Chromium на системе.
- Запуск Chrome с `--remote-debugging-port`.
- Проверка доступности CDP (`/json/version`).
- Используется OZON‑парсером в CDP режиме.

### 3.3 `bot/pipeline/runner.py` (ветка OZON)
Пайплайн обработки результатов.

OZON‑функционал:
- Выбор режима:
  - если IDs есть → MONITOR,
  - если БД пустая → COLLECT → сохранить IDs → MONITOR.
- Rotation/refill:
  - может удалить часть базы (например 20% раз в N дней) и добрать через COLLECT.
- Auto‑refill:
  - при уменьшении количества товаров ниже target может добирать через COLLECT.
- Публикация:
  - отбор событий по порогам падения цены/роста скидки,
  - отправка в канал.
- “Мягкая смерть” по картинке:
  - если картинка не скачалась N раз подряд, товар может считаться “dead” и удаляться.

### 3.4 `bot/services/product_manager.py` (OZON‑методы)
Функционал, используемый OZON:
- `get_product_ids(PlatformCode.OZON)`
- `get_product_count(PlatformCode.OZON)`
- `add_products(PlatformCode.OZON, ids)`
- `remove_products(PlatformCode.OZON, ids)`
- `trim_to_target(PlatformCode.OZON, target)`
- `remove_oldest_products(PlatformCode.OZON, count)`
- `cleanup_dead_products_ozon(dead_after=3)` — удаление товаров с повторяющимися 404/410 (если включено).

Также используется общий список категорий/тем для добора:
- `get_refill_categories()` — единый список категорий из БД/ENV/дефолта.

### 3.5 `bot/db/services/change_detection.py`
Логика фиксации изменений в БД.

Для OZON:
- Создаёт/обновляет `Product` по `external_id` и `platform`.
- Ведёт стабилизацию:
  - `stable_parse_count`, `is_stable`, baseline.
- Пишет `PriceHistory` на каждую проверку (если есть цена).
- Учитывает ошибки (например `403`, `404`, `410`) в `dead_check_fail_count`/`last_dead_reason` (для удаления “мертвых” SKU).

### 3.6 `bot/posting/poster.py` (OZON‑особенности)
Отвечает за публикации.

OZON‑аспекты:
- попытки скачать картинку по `image_url` из данных парсера,
- fallback: для OZON может пытаться получить `og:image` через браузер,
- если картинка не доступна — в зависимости от настроек:
  - либо пропускать товар (и увеличивать `no_image_fail_count`),
  - либо отправлять заглушку.

### 3.7 `bot/main.py` (OZON‑запуск)
Создаёт и запускает OZON‑задачу по расписанию:
- берёт свежие IDs из БД,
- создаёт `OzonParser(product_ids=...)`,
- запускает pipeline,
- гарантирует закрытие парсера.

Также может выполнять периодическую очистку “мертвых” OZON товаров (404/410), если включено.

---

## 4) Парсер OZON (`bot/parsers/ozon.py`) — детали

### 4.1 Режим MONITOR (основной)
Вход:
- список SKU из БД (обычно `Product.external_id` для OZON).

Действие:
- для каждого SKU выполняется запрос к JSON API через browser `fetch`:

`/api/entrypoint-api.bx/page/json/v2?url=/product/<sku>/`

Разбор:
- из `widgetStates` извлекаются:
  - цена, старая цена, card price, доступность,
  - заголовок/название,
  - обложка/картинка,
  - рейтинг и количество отзывов.

Ошибки:
- при `403` накапливаются “ошибки подряд”.
- при достижении `MONITOR_MAX_ERRORS`:
  - фиксируется “403 wave”,
  - делается пауза `OZON_403_COOLDOWN_SEC`,
  - переподключение к браузеру,
  - продолжение мониторинга.
- ограничение по числу recoveries: `OZON_MAX_RECOVERIES`.

### 4.2 Режим COLLECT
Используется для:
- первичного наполнения БД,
- добора после удаления/ротации,
- поддержания target_count.

Источники SKU:
1) Network: разбор JSON ответов (widgetStates/tilegrid).
2) DOM fallback: поиск ссылок `a[href*='/product/']` и извлечение `/product/<sku>`.

Алгоритм:
- список категорий/тем задаётся через общий список (из БД настроек / ENV / дефолт),
- строится равномерная квота: `base_quota + extra`,
- проход по категориям (Pass1) + повторный добор (Pass2) при нехватке,
- дедупликация по `external_id`.

Ограничения:
- scroll delay,
- max scroll steps,
- остановка при отсутствии новых товаров N шагов.

---

## 5) Конфигурация OZON через ENV

### 5.1 Основные
| Переменная | Назначение | Пример |
|---|---|---|
| `ENABLE_OZON` | включить/выключить задачу OZON | `true/false` |
| `TARGET_PRODUCT_COUNT` | целевой размер базы OZON (и других) | `3000` |

### 5.2 CDP / Chrome (если используется CDP режим)
| Переменная | Назначение | Пример |
|---|---|---|
| `OZON_CDP_URL` | URL CDP | `http://127.0.0.1:9222` |
| `OZON_CDP_PORT` | порт CDP (для chrome_manager) | `9222` |
| `OZON_CHROME_PROFILE_DIR` | профиль Chrome | `/home/user/.ozon_parser_chrome` |
| `CHROME_HEADLESS` | headless режим запуска Chrome | `true/false` |

### 5.3 COLLECT
| Переменная | Назначение | Пример |
|---|---|---|
| `OZON_COLLECT_TARGET` | целевой объём COLLECT (если запускать COLLECT) | `3000` |
| `OZON_SCROLL_DELAY_SEC` | задержка между scroll | `1.2` |
| `OZON_MAX_SCROLL_STEPS` | лимит скроллов | `500` |
| `OZON_QUIET_STEPS_STOP` | остановка при отсутствии новых товаров | `30` |
| `OZON_LOG_EVERY_STEPS` | логирование прогресса | `25` |
| `OZON_SKIP_CARD_ONLY` | пропуск товаров “только по карте” | `true/false` |

### 5.4 MONITOR
| Переменная | Назначение | Пример |
|---|---|---|
| `OZON_MONITOR_BATCH_SIZE` | батч (если используется) | `100` |
| `OZON_MONITOR_REQUEST_DELAY` | задержка между SKU | `0.3` |
| `OZON_MONITOR_ERROR_DELAY` | задержка после ошибки | `2.0` |
| `OZON_MONITOR_MAX_ERRORS` | порог ошибок подряд | `10` |
| `OZON_403_COOLDOWN_SEC` | пауза при волне 403 | `120` |
| `OZON_MAX_RECOVERIES` | максимум переподключений за цикл | `3` |

### 5.5 Rotation
| Переменная | Назначение | Пример |
|---|---|---|
| `OZON_ROTATION_ENABLED` | включить ротацию | `true/false` |
| `OZON_ROTATION_DAYS` | период ротации | `7` |
| `OZON_ROTATION_FRACTION` | доля замены | `0.2` |

### 5.6 Proxy (если используется режим с запуском Playwright Chromium + proxy)
| Переменная | Назначение | Пример |
|---|---|---|
| `OZON_USE_CDP` | использовать CDP или launch | `true/false` |
| `OZON_HEADLESS` | headless для Playwright launch | `true/false` |
| `OZON_PROXY_SERVER` | адрес прокси | `http://proxy.host:port` |
| `OZON_PROXY_USERNAME` | логин | `<username>` |
| `OZON_PROXY_PASSWORD` | пароль | `<password>` |

---

## 6) База данных (OZON)

### 6.1 Platform
Используется `PlatformCode.OZON`.

### 6.2 Product (ключевые поля)
Таблица `products`:
- `platform_id`, `external_id` — уникальная пара.
- `title`, `url`
- `current_price`, `old_price`, `discount`
- `stock`, `rating`
- `stable_parse_count`, `is_stable`, `baseline_price`, `baseline_discount`
- `dead_check_fail_count`, `last_dead_reason` — для удаления по 404/410
- `no_image_fail_count` — “мягкая смерть” по картинкам

### 6.3 PriceHistory
Таблица `price_history`:
- пишется на каждую проверку (если есть цена),
- хранит `price`, `old_price`, `discount`, `rating`, `stock`, `checked_at`.

---

## 7) Логика pipeline/runner для OZON

### 7.1 MONITOR vs COLLECT
- Если в БД есть IDs → MONITOR.
- Если БД пустая → COLLECT → сохранить → MONITOR.

### 7.2 Auto-refill
Если количество товаров < target:
- выполняется COLLECT,
- добавляются новые IDs,
- база приводится к target (`trim_to_target`).

### 7.3 Rotation
Раз в N дней может удалять N% самых старых товаров и добирать через COLLECT.

> Важно: rotation/refill могут принудительно запускать COLLECT. Если COLLECT недоступен (например, “Доступ ограничен”), база может уменьшиться.

---

## 8) Диагностика и типовые проблемы

### 8.1 Симптомы

**A) 403 wave в MONITOR**
- Логи: `api error ... 403`, затем `cooldown` и `reconnect`.
- Причины: ограничения на стороне OZON / репутация IP / прокси.

**B) 307 redirect loop `__rr=1..N`**
- Воспроизводится даже через `curl -L -D -`.
- Это инфраструктурный/edge‑уровень, не “тайминги парсера”.
- Если `curl` не может получить 200, Playwright обычно тоже не сможет стабильно мониторить.

**C) COLLECT: "Доступ ограничен"**
- Title страницы: “Доступ ограничен”.
- Сбор SKU = 0.

### 8.2 Полезные команды

**Проверка CDP (если используется):**
```bash
curl -s http://127.0.0.1:9222/json/version | head
Проверка статуса OZON через curl:

Bash

curl -s -L -o /dev/null -w "%{http_code}\n" https://www.ozon.ru/
Проверка прокси healthcheck (минимальный трафик):

Bash

curl -s --max-time 15 -x http://PROXY_HOST:PORT -U 'USER:PASS' -H "Range: bytes=0-0" -D - -o /dev/null https://api.ipify.org?format=json | head
Проверка редиректов OZON:

Bash

curl -s -L -D - -o /dev/null https://www.ozon.ru/ | sed -n '1,120p'
9) Запуск OZON части
9.1 Локально
настроить .env
установить зависимости
python -m bot
9.2 На VPS (общо)
venv активировать:
Bash

cd /opt/parser-bot
source .venv/bin/activate
запуск:
Bash

python -m bot
Если используется CDP‑браузер:

отдельный сервис chrome-cdp (systemd) должен быть running.