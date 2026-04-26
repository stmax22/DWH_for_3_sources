/* Заполняем таблицу fct_product_sales. */
WITH 
alter_data_1 AS (
	SELECT
		event_value::JSON ->> 'order_id' AS order_id,
		TRIM(event_value::json ->> 'product_payments', '[]') AS info
	FROM stg.bonussystem_events
	WHERE event_ts >= (
	    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
	)
),
alter_data_2 AS (
	SELECT
		order_id,
		REPLACE(info, '}, {', '}@{') AS info
	FROM alter_data_1
),
alter_data_3 AS (
	SELECT
		order_id,
		UNNEST(STRING_TO_ARRAY(info, '@'))::JSON AS info
	FROM alter_data_2
),
all_info AS (
	SELECT 
		info ->> 'product_id' AS product_id,
		order_id,
		(info ->> 'quantity')::INTEGER AS count,
		(info ->> 'price')::NUMERIC(19, 5) AS price,
		(info ->> 'product_cost')::NUMERIC(19, 5) AS total_sum,
		(info ->> 'bonus_payment')::NUMERIC(19, 5) AS bonus_payment,
		(info ->> 'bonus_grant')::NUMERIC(19, 5) AS bonus_grant
	FROM alter_data_3
)

INSERT INTO dds.fct_product_sales (
	product_id,
	order_id,
	count,
	price,
	total_sum,
	bonus_payment,
	bonus_grant
)
SELECT 
	dp.id AS product_id,
	ddr.id AS order_id,
	SUM(ai.count) AS count,
	SUM(ai.price) AS price,
	SUM(ai.total_sum) AS total_sum,
	SUM(ai.bonus_payment) AS bonus_payment,
	SUM(ai.bonus_grant) AS bonus_grant
FROM all_info AS ai
INNER JOIN dds.dm_products AS dp ON dp.product_id = ai.product_id
INNER JOIN dds.dm_orders AS ddr ON ddr.order_key = ai.order_id
GROUP BY dp.id, ddr.id;