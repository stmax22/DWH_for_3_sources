/* Заполняем таблицу dm_orders. */
INSERT INTO dds.dm_orders (
	user_id,
	restaurant_id,
	courier_id,
	timestamp_id,
	order_key,
	order_status
)
SELECT 
	du.id AS user_id,
	dr.id AS restaurant_id,
	d.courier_id AS courier_id,
	dt.id AS timestamp_id,
	oo.object_id AS order_key,
	oo.object_value::JSON ->> 'final_status' AS order_status
FROM stg.ordersystem_orders AS oo
INNER JOIN dds.dm_users AS du ON (oo.object_value::JSON -> 'user' ->> 'id') = du.user_id
INNER JOIN dds.dm_restaurants AS dr ON (oo.object_value::JSON -> 'restaurant' ->> 'id') = dr.restaurant_id 
INNER JOIN dds.dm_timestamps AS dt ON (oo.object_value::JSON ->> 'date')::TIMESTAMP = dt.ts
INNER JOIN dds.deliveries AS d ON oo.object_id = d.order_id
WHERE oo.update_ts >= (
    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
);