/* Заполняем таблицу srv_wf_settings_ts. */
INSERT INTO dds.srv_wf_settings_ts (
	id,
	last_update_date
)
SELECT
	1,
	MAX(update_ts)
FROM stg.ordersystem_orders
ON CONFLICT (id) DO UPDATE SET
	last_update_date=EXCLUDED.last_update_date;