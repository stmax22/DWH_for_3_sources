# ETL Pipeline for 3 sources
Проект реализует ETL-пайплайн для построения хранилища данных (DWH). Система собирает данные из трёх источников (PostgreSQL, MongoDB, API), трансформирует их в модель «Снежинка» и формирует аналитические витрины.

## Описание проекта
Необходимо построить хранилище данных, которое:
- **Собирает данные as is** из трёх подсистем:
  - **PostgreSQL** — подсистема бонусных баллов (пользователи, ранги, транзакции)
  - **MongoDB** — подсистема обработки заказов (заказы, рестораны, пользователи)
  - **REST API** — подсистема курьерской доставки (курьеры, доставки)
- **Хранит историю изменений** (SCD1, SCD2)
- **Обеспечивает стабильность** при недоступности источников или изменении их формата
- **Формирует витрины** для бизнес-аналитики

## Архитектура решения
Проект построен по методологии Ральфа Кимбалла с тремя слоями данных:
- CDM
- DDS
- STG

## Структура проекта
```
.
├── dags/
│   ├── DAG_PG.py                           # DAG: загрузка сырых данных из PostgreSQL (бонусная система)
│   ├── DAG_MongoDB.py                      # DAG: загрузка сырых данных из MongoDB (система заказов)
│   ├── DAG_API.py                          # DAG: загрузка сырых данных из API (курьерская служба)
│   ├── DAG_DDS.py                          # DAG: заполнение DDS-слоя
│   ├── DAG_CDM_1.py                        # DAG: заполнение витрины dm_settlement_report данными
│   ├── DAG_CDM_2.py                        # DAG: заполнение витрины dm_courier_ledger данными
├── scripts/
│   ├── Script_PG.py                        # Python-скрипт для работы с PostgreSQL-источником
│   ├── Script_MongoDB.py                   # Python-скрипт для работы с MongoDB-источником
│   ├── Script_API.py                       # Python-скрипт для работы с API-источником
├── sql/
│   ├── DDL_Project.sql                     # DDL: создание всех схем и таблиц (CDM, DDS, STG)
│   ├── DML_srv_wf_settings_ts.sql          # Обновление курсора инкрементальной загрузки
│   ├── DML_dm_users.sql                    # Загрузка измерения «Пользователи»
│   ├── DML_dm_restaurants.sql              # Загрузка измерения «Рестораны»
│   ├── DML_dm_timestamps.sql               # Загрузка измерения «Временные метки»
│   ├── DML_dm_products.sql                 # Загрузка измерения «Продукты»
│   ├── DML_dm_orders.sql                   # Загрузка измерения «Заказы»
│   ├── DML_couriers.sql                    # Загрузка справочника курьеров
│   ├── DML_deliveries.sql                  # Загрузка фактов доставок
│   ├── DML_fct_product_sales.sql           # Загрузка фактов продаж
│   ├── DML_dm_settlement_report.sql        # Наполнение витрины взаиморасчётов с ресторанами
│   └── DML_dm_courier_ledger.sql           # Наполнение витрины расчётов с курьерами
└── Diagram_DDS.png                         # ER-диаграмма DDS-слоя
```

## Источники данных

### PostgreSQL — Подсистема бонусных баллов
| Таблица | Описание |
|---------|----------|
| `users` | ID пользователя |
| `ranks` | Ранговая система (процент бонусов, минимальный порог) |
| `outbox` | События: изменение ранга, баланса, транзакции оплаты/начисления |
| `bonus_balance` | Текущий баланс пользователя |
| `bonus_transactions` | Детализация оплат и начислений бонусных баллов |
| `user_ranks` | Текущий ранг пользователя |

**Особенности:**
- Инкрементальная загрузка по `id` (для `outbox`)
- Таблица `srv_wf_settings_posgresql` хранит позицию курсора

### MongoDB — Подсистема обработки заказов
| Коллекция | Описание |
|-----------|----------|
| `orders` | Заказы с вложенными данными о пользователе, ресторане, продуктах |
| `restaurants` | Рестораны с меню (вложенный массив продуктов) |
| `users` | Пользователи системы заказов |

**Особенности:**
- Инкрементальная загрузка по `update_ts` (для `orders`)
- Таблица `srv_wf_settings_mongodb` хранит метку времени последней загрузки

### API — Подсистема курьерской доставки
| Endpoint | Описание |
|----------|----------|
| `GET /couriers` | Список курьеров |
| `GET /deliveries` | Список доставок |

**Поля доставки:**
- `order_id` — ID заказа
- `order_ts` — дата/время создания заказа
- `courier_id` — ID курьера
- `rate` — рейтинг (1–5)
- `tip_sum` — сумма чаевых

**Особенности:**
- Инкрементальная загрузка за период от последней загрузки до текущей даты (для `deliveries`)

## Схемы данных

### STG-слой
Слой для хранения сырых данных из источников.

#### PostgreSQL (бонусная система)
| Таблица | Поля | Описание |
|---------|------|----------|
| `srv_wf_settings_posgresql` | `id`, `workflow_key`, `workflow_settings` | Курсор инкрементальной загрузки |
| `bonussystem_users` | `id`, `order_user_id` | Связь пользователей |
| `bonussystem_ranks` | `id`, `name`, `bonus_percent`, `min_payment_threshold` | Ранги |
| `bonussystem_events` | `id`, `event_ts`, `event_type`, `event_value` | События |

#### MongoDB (система заказов)
| Таблица | Поля | Описание |
|---------|------|----------|
| `srv_wf_settings_mongodb` | `id`, `last_update_ts` | Курсор по времени |
| `ordersystem_users` | `id`, `object_id`, `object_value`, `update_ts` | Пользователи |
| `ordersystem_orders` | `id`, `object_id`, `object_value`, `update_ts` | Заказы |
| `ordersystem_restaurants` | `id`, `object_id`, `object_value`, `update_ts` | Рестораны |

#### API (курьерская служба)
| Таблица | Поля | Описание |
|---------|------|----------|
| `couriers` | `courier_info` | Данные курьеров |
| `deliveries` | `deliveri_info` | Данные доставок |

### DDS-слой
Модель данных «Снежинка» с SCD2 для изменяющихся измерений.
![](https://github.com/stmax22/DWH_for_3_sources/blob/4ba47a09287f9ed83654b67483bbdffbdf70cb23/Diagram_DDS.png)

#### Таблицы измерений
| Таблица | Поля | Описание |
|---------|---------|------|----------|
| `dm_users` | `id`, `user_id`, `user_name`, `user_login` | Пользователи |
| `dm_restaurants` | `id`, `restaurant_id`, `restaurant_name`, `active_from`, `active_to` | Рестораны |
| `dm_products` | `id`, `restaurant_id`, `product_id`, `product_name`, `product_price`, `active_from`, `active_to` | Продукты |
| `dm_timestamps` | `id`, `ts`, `year`, `month`, `day`, `time`, `date` | Временные метки |
| `couriers` | `id`, `courier_id`, `courier_name` | Курьеры |

#### Таблицы фактов
| Таблица | Поля | Описание |
|---------|------|----------|
| `dm_orders` | `id`, `user_id`, `restaurant_id`, `courier_id`, `timestamp_id`, `order_key`, `order_status` | Заказы |
| `deliveries` | `id`, `order_id`, `order_ts`, `courier_id`, `rate`, `tip_sum` | Доставки |
| `fct_product_sales` | `id`, `product_id`, `order_id`, `count`, `price`, `total_sum`, `bonus_payment`, `bonus_grant` | Продажи по позициям |

#### Сервисная таблица
| Таблица | Поля | Описание |
|---------|------|----------|
| `srv_wf_settings_ts` | `id`, `last_update_date` | Курсор для инкрементальной загрузки DDS |

### CDM-слой
Широкие витрины с бизнес-агрегатами.

#### Витрина dm_settlement_report
Взаиморасчёты с ресторанами-партнёрами.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | INTEGER | Идентификатор записи (PK) |
| `restaurant_id` | INTEGER | ID ресторана (FK) |
| `restaurant_name` | VARCHAR(100) | Название ресторана |
| `settlement_date` | DATE | Дата отчёта |
| `orders_count` | INTEGER | Количество заказов |
| `orders_total_sum` | NUMERIC(14,2) | Общая сумма заказов |
| `orders_bonus_payment_sum` | NUMERIC(14,2) | Сумма оплат бонусами |
| `orders_bonus_granted_sum` | NUMERIC(14,2) | Сумма начисленных бонусов |
| `order_processing_fee` | NUMERIC(14,2) | Комиссия компании (25% от суммы) |
| `restaurant_reward_sum` | NUMERIC(14,2) | Сумма к перечислению ресторану |

**Бизнес-логика:**
- Учитываются только заказы со статусом `CLOSED`
- `order_processing_fee` = `orders_total_sum` × 0.25
- `restaurant_reward_sum` = `orders_total_sum` − `order_processing_fee` − `orders_bonus_payment_sum`
- Уникальный ключ: `(restaurant_id, settlement_date)`

#### Витрина dm_courier_ledger
Расчёты с курьерами.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | INTEGER | Идентификатор записи (PK) |
| `courier_id` | INTEGER | ID курьера (FK) |
| `courier_name` | VARCHAR(100) | Ф.И.О. курьера |
| `settlement_year` | SMALLINT | Год отчёта |
| `settlement_month` | SMALLINT | Месяц отчёта |
| `orders_count` | INTEGER | Количество заказов |
| `orders_total_sum` | NUMERIC(14,2) | Общая сумма заказов |
| `rate_avg` | SMALLINT | Средний рейтинг курьера |
| `order_processing_fee` | NUMERIC(14,2) | Комиссия компании (25%) |
| `courier_order_sum` | NUMERIC(14,2) | Выплата за заказы |
| `courier_tips_sum` | NUMERIC(14,2) | Сумма чаевых |
| `courier_reward_sum` | NUMERIC(14,2) | Итоговая сумма к выплате |

**Бизнес-логика расчёта выплат курьеру:**
| Условие рейтинга | Процент | Минимум |
|------------------|---------|---------|
| `rate_avg < 4` | 5% | 100 ₽ |
| `4 ≤ rate_avg < 4.5` | 7% | 150 ₽ |
| `4.5 ≤ rate_avg < 4.9` | 8% | 175 ₽ |
| `rate_avg ≥ 4.9` | 10% | 200 ₽ |

- `order_processing_fee` = `orders_total_sum` × 0.25
- `courier_reward_sum` = `courier_order_sum` + `courier_tips_sum` × 0.95 (5% — комиссия за обработку платежа)
- Уникальный ключ: `(courier_id, settlement_year, settlement_month)`
- Отчёт собирается по **дате заказа**, а не доставки
