import json
import logging
from contextlib import contextmanager
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from bson import ObjectId
from pymongo import MongoClient


# Задаем формат лог-сообщений.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Данные для подключения к БД.
PG_DWH = 'PG_WAREHOUSE_CONNECTION'
MONGO_DB_URL = Variable.get('MONGO_DB_CONNECTION')

table_names = ('orders', 'restaurants', 'users')
mongodb_name = 'db-mongo'


@contextmanager
def connection(pq_id, m_id):
    """Метод автоматически открывает и закрывает соединение."""

    hook = PostgresHook(pq_id)
    conn_pq = hook.get_conn()
    cursor_pg = conn_pq.cursor()
    conn_m = MongoClient(m_id)

    try:
        logging.info('Подключение к PostgreSQL и MongoDB прошло успешно!')
        yield cursor_pg, conn_pq, conn_m
    except Exception as e:
        conn_pq.rollback()
        logging.error(f'Ошибка при миграции данных: {e}')
        raise
    finally:
        cursor_pg.close()
        conn_pq.close()
        conn_m.close()


def clean_data(obj):
    """Метод сериализует объекты в JSON."""

    if isinstance(obj, dict):
        return {
            k: (
                str(v) if isinstance(v, ObjectId) else
                v.strftime('%Y-%m-%d %H:%M:%S') if isinstance(v, datetime) else
                clean_data(v)
            )
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [clean_data(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    return obj


def insert_table(table_name, values, cursor_pg, conn_pq):
    """Метод заполняет таблицы DWH данными."""

    cursor_pg.executemany(f"""
        INSERT INTO stg.ordersystem_{table_name} (
            object_id,
            object_value,
            update_ts
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (object_id) DO UPDATE SET
            object_value = EXCLUDED.object_value,
            update_ts = EXCLUDED.update_ts
        """, values)

    conn_pq.commit()


def last_update(cursor_pg):
    """Метод узнает дату последней загрузки данных в DWH."""

    cursor_pg.execute("""
        SELECT
            last_update_ts
        FROM stg.srv_wf_settings_mongodb
    """)

    update_date = cursor_pg.fetchone()
    last_update_date = (update_date[0] if update_date else datetime(2023, 10, 22, 0, 0, 0))

    logging.info(f'Дата последнего обновления: {last_update_date}')

    return last_update_date


def insert_incremental_table(table_name, cursor_pg, conn_pq):
    """Метод заполняет таблицу для инкрементальной загрузки."""

    cursor_pg.execute(f"""
                INSERT INTO stg.srv_wf_settings_mongodb (
                    id,
                    last_update_ts
                )
                SELECT
                    1,
                    MAX(update_ts)
                FROM stg.ordersystem_{table_name}
                ON CONFLICT (id) DO UPDATE SET
                    last_update_ts = EXCLUDED.last_update_ts
                RETURNING last_update_ts;
                """)

    update_date = cursor_pg.fetchone()
    last_update_date = update_date[0]
    logging.info(f'Последняя дата обновления в stg.srv_wf_settings_mongodb изменена на: {last_update_date}')

    conn_pq.commit()


def uploading_data(db_name, table_name):
    """Метод загружает данные из источника в DWH."""

    logging.info(f'Начался процесс миграции данных из источника "{table_name}" в DWH.')

    with connection(PG_DWH, MONGO_DB_URL) as (cursor_pg, conn_pq, conn_m):
        # Подключаемся к нужной таблице в источнике.
        mongo_table = conn_m[db_name][table_name]
        logging.info(f'Подключение к таблице "{table_name}" прошло успешно!')

        if table_name == 'orders':
            last_update_date = last_update(cursor_pg)

        rows = mongo_table.find()
        values = []

        for row in rows:
            id = str(row.get('_id'))
            update_ts = row.get('update_ts')
            processed_row = clean_data(row)
            json_str  = json.dumps(processed_row, ensure_ascii=False)

            if table_name == 'orders' and update_ts > last_update_date:
                values.append((id, json_str, update_ts))

            elif table_name != 'orders':
                values.append((id, json_str , update_ts))

        if values:
            insert_table(table_name, values, cursor_pg, conn_pq)

        logging.info(f'Загружено/обновлено записей: {len(values)} шт.')

        if table_name == 'orders':
            insert_incremental_table(table_name, cursor_pg, conn_pq)

        logging.info(f'Процесс миграции данных из источника "{table_name}" в DWH успешно окончен!')
