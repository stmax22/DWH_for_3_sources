import logging
from contextlib import contextmanager

from airflow.providers.postgres.hooks.postgres import PostgresHook


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Данные для подключения к БД.
PG_SOURCE = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
PG_DWH = 'PG_WAREHOUSE_CONNECTION'
BATCH_SIZE = 1000

# SQL скрипты:
sql_insert_outbox = """
    INSERT INTO stg.bonussystem_events (
        id,
        event_ts,
        event_type,
        event_value
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (event_ts) DO UPDATE SET
        id = EXCLUDED.id,
        event_type = EXCLUDED.event_type,
        event_value = EXCLUDED.event_value;
"""
sql_insert_ranks = """
    INSERT INTO stg.bonussystem_ranks (
        id,
        name,
        bonus_percent,
        min_payment_threshold
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        bonus_percent = EXCLUDED.bonus_percent,
        min_payment_threshold = EXCLUDED.min_payment_threshold;
"""
sql_insert_users = """
    INSERT INTO stg.bonussystem_users (id, order_user_id)
    VALUES (%s, %s)
    ON CONFLICT (id) DO UPDATE SET
        order_user_id = EXCLUDED.order_user_id;
"""

table_names = {
    'outbox': sql_insert_outbox,
    'ranks': sql_insert_ranks,
    'users': sql_insert_users
}


@contextmanager
def postgres_connection(conn_id):
    """Метод автоматически открывает и закрывает соединение."""

    hook = PostgresHook(conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        logging.info(f'Подключение к {conn_id} прошло успешно!')
        yield cursor, conn
    except Exception as e:
        conn.rollback()
        logging.error(f'Ошибка при работе с {conn_id}: {e}')
        raise
    finally:
        cursor.close()
        conn.close()


def select_last_id(dwh_cursor):
    """Метод получает последний обработанный id."""

    dwh_cursor.execute('SELECT MAX(id) FROM stg.srv_wf_settings_posgresql')
    id = dwh_cursor.fetchone()
    max_id = (id[0] if id else 0)

    logging.info(f'Последний обработанный id: {max_id}')

    return max_id


def insert_last_id(dwh_cursor, rows):
    """Метод обновляет последний обработанный id."""

    last_id, event_ts, event_type, event_value = rows[-1]
    dwh_cursor.execute("""
        INSERT INTO stg.srv_wf_settings_posgresql (
            id,
            workflow_key,
            workflow_settings
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (workflow_key) DO UPDATE SET
            id = EXCLUDED.id,
            workflow_settings = EXCLUDED.workflow_settings;
        """, (
            last_id,
            event_type,
            event_value
        )
    )

    logging.info(f'Последний обработанный id: {last_id}')


def uploading_data(table_name, sql_insert, batch_size=BATCH_SIZE):
    """Метод производит загрузку данных из источника в DWH."""

    logging.info(f'Начался процесс миграции данных из источника "{table_name}" в DWH.')

    with postgres_connection(PG_SOURCE) as (src_cursor, _), \
         postgres_connection(PG_DWH) as (dwh_cursor, dwh_conn):

        if table_name == 'outbox':
            max_id = select_last_id(dwh_cursor)
            src_cursor.execute(f'SELECT * FROM public.{table_name} WHERE id > %s', (max_id,))
        else:
            src_cursor.execute(f'SELECT * FROM public.{table_name}')

        total_rows = 0
        last_batch = []

        while True:
            rows = src_cursor.fetchmany(batch_size)

            if not rows:
                break

            total_rows += len(rows)
            last_batch = rows
            dwh_cursor.executemany(sql_insert, rows)

        logging.info(f'В {table_name} загружено/обновлено {total_rows} записей.')

        if table_name == 'outbox':
            insert_last_id(dwh_cursor, last_batch)

        dwh_conn.commit()

        logging.info(f'Процесс миграции данных из источника "{table_name}" в DWH успешно окончен!')
