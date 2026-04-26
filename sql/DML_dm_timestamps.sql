/* Заполняем таблицу dm_timestamps. */
INSERT INTO dds.dm_timestamps (
	ts,
	"year",
	"month",
	"day",
	"time",
	"date"
)
SELECT
	(object_value::JSON ->> 'date')::TIMESTAMP AS ts,
	EXTRACT(YEAR FROM (object_value::JSON ->> 'date')::TIMESTAMP) AS year,
	EXTRACT(MONTH FROM (object_value::JSON ->> 'date')::TIMESTAMP) AS month,
	EXTRACT(DAY FROM (object_value::JSON ->> 'date')::TIMESTAMP) AS day,
	(object_value::JSON ->> 'date')::TIME AS time,
	(object_value::JSON ->> 'date')::DATE AS date
FROM stg.ordersystem_orders
WHERE update_ts >= (
    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
  )
ON CONFLICT (ts) DO UPDATE SET
	year = EXCLUDED.year,
	month = EXCLUDED.month,
	day = EXCLUDED.day,
	time = EXCLUDED.time,
	date = EXCLUDED.date;