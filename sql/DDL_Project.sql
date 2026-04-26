/* Создаем CDM слой. */
DROP SCHEMA IF EXISTS cdm;

CREATE SCHEMA IF NOT EXISTS cdm;

/* Создаем витрину dm_settlement_report. */
DROP TABLE IF EXISTS cdm.dm_settlement_report;

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	restaurant_id INTEGER NOT NULL,
	restaurant_name VARCHAR(100) NOT NULL,
	settlement_date DATE NOT NULL,
	orders_count INTEGER NOT NULL DEFAULT 0,
	orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
	restaurant_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_settlement_report_pk PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01'),
	CONSTRAINT orders_count_checking_for_0 CHECK (orders_count >= 0),
	CONSTRAINT orders_total_sum_checking_for_0 CHECK (orders_total_sum >= 0),
	CONSTRAINT orders_bonus_payment_sum_checking_for_0 CHECK (orders_bonus_payment_sum >= 0),
	CONSTRAINT orders_bonus_granted_sum_checking_for_0 CHECK (orders_bonus_granted_sum >= 0),
	CONSTRAINT order_processing_fee_checking_for_0 CHECK (order_processing_fee >= 0),
	CONSTRAINT restaurant_reward_sum_checking_for_0 CHECK (restaurant_reward_sum >= 0),
	CONSTRAINT there_cannot_be_more_than_1_record UNIQUE (restaurant_id, settlement_date)
);

/* Создаем витрину dm_settlement_report. */
DROP TABLE IF EXISTS cdm.dm_courier_ledger;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	courier_id INTEGER NOT NULL,
	courier_name VARCHAR(100) NOT NULL,
	settlement_year SMALLINT NOT NULL,
	settlement_month SMALLINT NOT NULL,
	orders_count INTEGER NOT NULL DEFAULT 0,
	orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	rate_avg SMALLINT NOT NULL,
	order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
	courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	CONSTRAINT courier_payment_report_pk PRIMARY KEY (id),
	CONSTRAINT courier_payment_report_settlement_year CHECK (settlement_year >= 2022 AND settlement_year < 2500),
	CONSTRAINT settlement_month_checking CHECK (settlement_month >= 1 AND settlement_month <= 12),
	CONSTRAINT orders_count_checking_for_0 CHECK (orders_count >= 0),
	CONSTRAINT orders_total_sum_checking_for_0 CHECK (orders_total_sum >= 0),
	CONSTRAINT rate_avg_checking CHECK (rate_avg >= 1 AND rate_avg <= 5),
	CONSTRAINT order_processing_fee_checking_for_0 CHECK (order_processing_fee >= 0),
	CONSTRAINT courier_order_sum_checking_for_0 CHECK (courier_order_sum >= 0),
	CONSTRAINT courier_tips_sum_checking_for_0 CHECK (courier_tips_sum >= 0),
	CONSTRAINT courier_reward_sum_checking_for_0 CHECK (courier_reward_sum >= 0),
	CONSTRAINT dm_courier_ledger_unique_combination UNIQUE (courier_id, settlement_year, settlement_month)
);


/* Создаем DDS слой. */
DROP SCHEMA IF EXISTS dds;

CREATE SCHEMA IF NOT EXISTS dds;

/* Создаем таблицу srv_wf_settings_ts. */
DROP TABLE IF EXISTS dds.srv_wf_settings_ts;

CREATE TABLE dds.srv_wf_settings_ts (
	id SMALLINT NOT NULL,
	last_update_date TIMESTAMP NOT NULL,
	CONSTRAINT srv_wf_settings_ts_uindex UNIQUE (id)
);

/* Создаем таблицу users. */
DROP TABLE IF EXISTS dds.dm_users;

CREATE TABLE IF NOT EXISTS dds.dm_users(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	user_id VARCHAR NOT NULL,
	user_name VARCHAR NOT NULL,
	user_login VARCHAR NOT NULL,
	CONSTRAINT user_id_pk PRIMARY KEY (id),
	CONSTRAINT dm_users_user_id_uindex UNIQUE (user_id)
);

/* Создаем таблицу restaurants. */
DROP TABLE IF EXISTS dds.dm_restaurants;

CREATE TABLE IF NOT EXISTS dds.dm_restaurants(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	restaurant_id VARCHAR NOT NULL,
	restaurant_name VARCHAR NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL,
	CONSTRAINT restaurant_id_pk PRIMARY KEY (id),
	CONSTRAINT dm_restaurants_restaurant_id_uindex UNIQUE (restaurant_id)
);

/* Создаем таблицу products. */
DROP TABLE IF EXISTS dds.dm_products;

CREATE TABLE IF NOT EXISTS dds.dm_products(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	restaurant_id INTEGER NOT NULL,
	product_id VARCHAR NOT NULL,
	product_name VARCHAR NOT NULL,
	product_price NUMERIC(14,2) DEFAULT 0 NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL,
	CONSTRAINT product_id_pk PRIMARY KEY (id),
	CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	CONSTRAINT dm_products_restaurant_id_and_product_id_uindex UNIQUE (restaurant_id, product_id),
	CONSTRAINT dm_products_count_check CHECK (product_price >= 0)
);

/* Создаем таблицу timestamps. */
DROP TABLE IF EXISTS dds.dm_timestamps;

CREATE TABLE IF NOT EXISTS dds.dm_timestamps(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	ts TIMESTAMP NOT NULL,
	year SMALLINT NOT NULL CHECK (year >= 2022 AND year < 2500),
	month SMALLINT NOT NULL CHECK (month >= 1 AND month <= 12),
	day SMALLINT NOT NULL CHECK (day >= 1 AND day <= 31),
	time TIME NOT NULL,
	date DATE NOT NULL,
	CONSTRAINT timestamp_id_pk PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_ts_uindex UNIQUE (ts)
);

/* Создаем таблицу couriers. */
DROP TABLE IF EXISTS dds.couriers;

CREATE TABLE IF NOT EXISTS dds.couriers (
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	courier_id VARCHAR(40) NOT NULL,
	courier_name VARCHAR(100) NOT NULL,
	CONSTRAINT couriers_pk PRIMARY KEY (id),
	CONSTRAINT couriers_courier_id_uindex UNIQUE (courier_id)
);

/* Создаем таблицу deliveries. */
DROP TABLE IF EXISTS dds.deliveries;

CREATE TABLE IF NOT EXISTS dds.deliveries (
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	order_id VARCHAR(40) NOT NULL,
	order_ts TIMESTAMP NOT NULL,
	courier_id INTEGER NOT NULL,
	rate SMALLINT NOT NULL,
	tip_sum NUMERIC(14,2) NOT NULL,
	CONSTRAINT deliveries_pk PRIMARY KEY (id),
	CONSTRAINT deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.couriers(id),
	CONSTRAINT deliveries_uindex UNIQUE (order_id, order_ts)
);

/* Создаем таблицу orders. */
DROP TABLE IF EXISTS dds.dm_orders;

CREATE TABLE IF NOT EXISTS dds.dm_orders(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	user_id INTEGER NOT NULL,
	restaurant_id INTEGER NOT NULL,
	courier_id INTEGER NOT NULL,
	timestamp_id INTEGER NOT NULL,
	order_key VARCHAR NOT NULL,
	order_status VARCHAR NOT NULL,
	CONSTRAINT order_id_pk PRIMARY KEY (id),
	CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
	CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.couriers(id),
	CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);

/* Создаем таблицу фактов fct_product_sales. */
DROP TABLE IF EXISTS dds.fct_product_sales;

CREATE TABLE IF NOT EXISTS dds.fct_product_sales(
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	product_id INTEGER NOT NULL,
	order_id INTEGER NOT NULL,
	count INTEGER NOT NULL DEFAULT 0,
	price NUMERIC(14,2) NOT NULL DEFAULT 0,
	total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	bonus_payment NUMERIC(14,2) NOT NULL DEFAULT 0,
	bonus_grant NUMERIC(14,2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_id_pk PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
	CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
	CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0),
	CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products(id),
	CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);


/* Создаем STG слой для данных из источников. */
DROP SCHEMA IF EXISTS stg;

CREATE SCHEMA IF NOT EXISTS stg;

/* STG из PostgreSQL. */

/* Создаем таблицу srv_wf_settings_posgresql. */
DROP TABLE IF EXISTS stg.srv_wf_settings_posgresql;

CREATE TABLE stg.srv_wf_settings_posgresql (
	id INTEGER NOT NULL,
	workflow_key VARCHAR NOT NULL,
	workflow_settings JSON NOT NULL,
	CONSTRAINT srv_wf_settings_posgresql_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_posgresql_workflow_key_key UNIQUE (workflow_key)
);

/* Создаем таблицу bonussystem_users. */
DROP TABLE IF EXISTS stg.bonussystem_users;

CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id INTEGER NOT NULL,
	order_user_id TEXT NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id),
	CONSTRAINT bonussystem_users_order_user_id_uindex UNIQUE (order_user_id)
);

/* Создаем таблицу bonussystem_ranks. */
DROP TABLE IF EXISTS stg.bonussystem_ranks;

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
	id INTEGER NOT NULL,
	name VARCHAR(2048) NOT NULL,
	bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL,
	min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL,
	CONSTRAINT ranks_pkey PRIMARY KEY (id),
	CONSTRAINT ranks_bonus_percent_check CHECK (bonus_percent >= 0),
	CONSTRAINT bonussystem_ranks_name_uindex UNIQUE (name)
);

/* Создаем таблицу bonussystem_events. */
DROP TABLE IF EXISTS stg.bonussystem_events;

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id INTEGER NOT NULL,
	event_ts TIMESTAMP NOT NULL,
	event_type VARCHAR NOT NULL,
	event_value TEXT NOT NULL,
	CONSTRAINT outbox_pkey PRIMARY KEY (id),
	CONSTRAINT bonussystem_event_ts_uindex UNIQUE (event_ts)
);

/* STG из MongoDB. */

/* Создаем таблицу srv_wf_settings_mongodb. */
DROP TABLE IF EXISTS stg.srv_wf_settings_mongodb;

CREATE TABLE stg.srv_wf_settings_mongodb (
	id SMALLINT NOT NULL,
	last_update_ts TIMESTAMP NOT NULL,
	CONSTRAINT srv_wf_settings_mongodb_pkey PRIMARY KEY (id)
);

/* Создаем таблицу ordersystem_users. */
DROP TABLE IF EXISTS stg.ordersystem_users;

CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
	id SERIAL NOT NULL,
	object_id VARCHAR(100) NOT NULL,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL,
	CONSTRAINT users_id_primary_key PRIMARY KEY (id),
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id)
);

/* Создаем таблицу ordersystem_orders. */
DROP TABLE IF EXISTS stg.ordersystem_orders;

CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id SERIAL NOT NULL,
	object_id VARCHAR(100) NOT NULL,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL,
	CONSTRAINT orders_id_primary_key PRIMARY KEY (id),
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id)
);

/* Создаем таблицу ordersystem_restaurants. */
DROP TABLE IF EXISTS stg.ordersystem_restaurants;

CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
	id SERIAL NOT NULL,
	object_id VARCHAR(100) NOT NULL,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL,
	CONSTRAINT restaurants_id_primary_key PRIMARY KEY (id),
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id)
);

/* STG из API. */

/* Создаем таблицу couriers. */
DROP TABLE IF EXISTS stg.couriers;

CREATE TABLE IF NOT EXISTS stg.couriers (
	courier_info JSONB NOT NULL,
	CONSTRAINT couriers_uindex UNIQUE (courier_info)
);

/* Создаем таблицу deliveries. */
DROP TABLE IF EXISTS stg.deliveries;

CREATE TABLE IF NOT EXISTS stg.deliveries (
	deliveri_info JSONB NOT NULL,
	CONSTRAINT deliveries_uindex UNIQUE (deliveri_info)
);
