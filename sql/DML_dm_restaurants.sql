/* Заполняем таблицу restaurants. */
INSERT INTO dds.dm_restaurants (
	restaurant_id,
	restaurant_name,
	active_from,
	active_to
)
SELECT 
	object_id AS restaurant_id,
	object_value::JSON ->> 'name' AS restaurant_name,
	update_ts AS active_from,
	'2099-12-31'::TIMESTAMP AS active_to
FROM stg.ordersystem_restaurants
WHERE update_ts >= (
    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
)
ON CONFLICT (restaurant_id) DO UPDATE SET
	restaurant_name = EXCLUDED.restaurant_name,
	active_from = EXCLUDED.active_from;