/* Заполняем таблицу couriers. */
INSERT INTO dds.couriers (
	courier_id,
	courier_name
)
SELECT
	courier_info ->> '_id' AS courier_id,
	courier_info ->> 'name' AS courier_name
FROM stg.couriers
ON CONFLICT (courier_id) DO UPDATE SET
	courier_name = EXCLUDED.courier_name;