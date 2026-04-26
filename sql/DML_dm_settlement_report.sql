/* Заполняем витрину dm_settlement_report. */
INSERT INTO cdm.dm_settlement_report (
	restaurant_id,
	restaurant_name,
	settlement_date,
	orders_count,
	orders_total_sum,
	orders_bonus_payment_sum,
	orders_bonus_granted_sum,
	order_processing_fee,
	restaurant_reward_sum
)
SELECT
	restaurant_id,
	restaurant_name,
	settlement_date,
	orders_count,
	orders_total_sum,
	orders_bonus_payment_sum,
	orders_bonus_granted_sum,
	orders_total_sum * 0.25,
	orders_total_sum - orders_total_sum * 0.25 - orders_bonus_payment_sum
FROM (
	SELECT
		dr.id AS restaurant_id,
		dr.restaurant_name,
		t.date AS settlement_date,
		COUNT(DISTINCT dor.id) AS orders_count,
		SUM(fct.total_sum) AS orders_total_sum,
		SUM(fct.bonus_payment) AS orders_bonus_payment_sum,
		SUM(fct.bonus_grant) AS orders_bonus_granted_sum
	FROM dds.fct_product_sales AS fct
	INNER JOIN dds.dm_orders AS dor ON fct.order_id = dor.id
	INNER JOIN dds.dm_restaurants AS dr ON dor.restaurant_id = dr.id 
	INNER JOIN dds.dm_timestamps AS t ON dor.timestamp_id = t.id
	WHERE dor.order_status = 'CLOSED'
	GROUP BY dr.id, dr.restaurant_name, t.date
) AS data_sum
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
	restaurant_name = EXCLUDED.restaurant_name,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
	orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
	order_processing_fee = EXCLUDED.order_processing_fee,
	restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;