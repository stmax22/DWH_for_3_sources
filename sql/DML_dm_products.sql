/* Заполняем таблицу dm_products. */
WITH
alter_data_1 AS (
	SELECT
		dr.id AS restaurant_id,
		TRIM(sor.object_value::JSON ->> 'menu', '[]') AS info,
		dr.active_from,
		dr.active_to
	FROM stg.ordersystem_restaurants AS sor
	INNER JOIN dds.dm_restaurants AS dr ON dr.restaurant_id = sor.object_id
	WHERE sor.update_ts >= (
	    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
	  )
),
alter_data_2 AS (
	SELECT
		restaurant_id,
		REPLACE(info, '}, {', '}@{') AS info,
		active_from,
		active_to
	FROM alter_data_1
),
restaurant_info AS (
	SELECT
		restaurant_id,
		UNNEST(STRING_TO_ARRAY(info, '@'))::JSON AS info,
		active_from,
		active_to
	FROM alter_data_2
)

INSERT INTO dds.dm_products (
	restaurant_id,
	product_id,
	product_name,
	product_price,
	active_from,
	active_to
)
SELECT
	restaurant_id,
	info ->> '_id' AS product_id,
	info ->> 'name' AS product_name,
	(info ->> 'price')::NUMERIC(14,2) AS product_price,
	active_from,
	active_to
FROM restaurant_info
ON CONFLICT (restaurant_id, product_id) DO UPDATE SET
	product_name=EXCLUDED.product_name,
	product_price=EXCLUDED.product_price,
	active_from=EXCLUDED.active_from,
	active_to=EXCLUDED.active_to;