/* Заполняем таблицу dm_users. */
INSERT INTO dds.dm_users (
	user_id,
	user_name,
	user_login
)
SELECT
	object_id AS user_id, 
	object_value::JSON ->> 'name' AS user_name, 
	object_value::JSON  ->> 'login' AS user_login
FROM stg.ordersystem_users
WHERE update_ts >= (
    COALESCE((SELECT last_update_date FROM dds.srv_wf_settings_ts), '2023-10-22 00:00:00'::TIMESTAMP)
)
ON CONFLICT (user_id) DO UPDATE SET
	user_name = EXCLUDED.user_name,
	user_login = EXCLUDED.user_login;