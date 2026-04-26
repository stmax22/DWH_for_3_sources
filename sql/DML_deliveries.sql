/* Заполняем таблицу deliveries. */
INSERT INTO dds.deliveries (
	order_id,
	order_ts,
	courier_id,
	rate,
	tip_sum
)
SELECT 
	deliveri_info ->> 'order_id' AS order_id,
	(deliveri_info ->> 'order_ts')::TIMESTAMP AS order_ts,
	dc.id AS courier_id,
	(deliveri_info ->> 'rate')::SMALLINT AS rate,
	(deliveri_info ->> 'tip_sum')::NUMERIC(14,2) AS tip_sum
FROM stg.deliveries AS sd
INNER JOIN dds.couriers AS dc ON sd.deliveri_info ->> 'courier_id' = dc.courier_id
WHERE (deliveri_info ->> 'order_ts')::TIMESTAMP >= (
    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
)
ON CONFLICT (order_id, order_ts) DO UPDATE SET
	courier_id = EXCLUDED.courier_id,
	rate = EXCLUDED.rate,
	tip_sum = EXCLUDED.tip_sum;