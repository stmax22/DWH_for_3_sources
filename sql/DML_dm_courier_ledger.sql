/* Заполняем витрину dm_courier_ledger. */
INSERT INTO cdm.dm_courier_ledger (
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
)
SELECT
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	orders_total_sum * 0.25 AS order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM (
	SELECT
		dc.id AS courier_id,
		dc.courier_name,
		ddt."year" AS settlement_year,
		ddt."month" AS settlement_month,
		COUNT(DISTINCT ddo.id) AS orders_count,
		SUM(fct.total_sum) AS orders_total_sum,
		AVG(dd.rate) AS rate_avg,
		SUM(CASE
				WHEN dd.rate < 4 THEN GREATEST(fct.total_sum * 0.05, 100)
				WHEN 4 <= dd.rate AND dd.rate < 4.5 THEN GREATEST(fct.total_sum * 0.07, 150)
				WHEN 4.5 <= dd.rate AND dd.rate < 4.9 THEN GREATEST(fct.total_sum * 0.08, 175)
				WHEN 4.9 <= dd.rate THEN GREATEST(fct.total_sum * 0.1, 200)
			END) AS courier_order_sum,
		SUM(dd.tip_sum) AS courier_tips_sum
	FROM dds.fct_product_sales AS fct
	INNER JOIN dds.dm_orders AS ddo ON fct.order_id = ddo.id
	INNER JOIN dds.dm_timestamps AS ddt ON ddo.timestamp_id = ddt.id
	INNER JOIN dds.deliveries AS dd ON ddo.order_key = dd.order_id
	INNER JOIN dds.couriers AS dc ON dd.courier_id = dc.id
	WHERE ddo.order_status = 'CLOSED'
	GROUP BY
		dc.id,
		dc.courier_name,
		ddt."year",
		ddt."month"
) AS data_sum
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
	courier_name = EXCLUDED.courier_name,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum;