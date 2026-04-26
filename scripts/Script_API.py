import json
import logging
import requests
from contextlib import contextmanager
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Задаем формат лог-сообщений.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Данные для подключения.
PG_DWH = 'PG_WAREHOUSE_CONNECTION'
HEADERS = Variable.get('HEADERS', deserialize_json=True)
CONN_ID = 'HTTP_CONN_ID'
BATCH_SIZE = 50

http_conn_id = BaseHook.get_connection(CONN_ID)
api_endpoint = http_conn_id.host

table_names = ('couriers', 'deliveries')


@contextmanager
def connection(pq_id):
    """Метод автоматически открывает и закрывает соединение."""

    hook = PostgresHook(pq_id)
    conn_pq = hook.get_conn()
    cursor_pg = conn_pq.cursor()

    try:
        logging.info('Подключение к PostgreSQL прошло успешно!')
        yield cursor_pg, conn_pq
    except Exception as e:
        conn_pq.rollback()
        logging.error(f'Ошибка при миграции данных: {e}')
        raise
    finally:
        cursor_pg.close()
        conn_pq.close()


def uploading_data(table_name, params=None, headers=HEADERS):
    """Метод выгружает данные из источника."""

    try:
        method_url = f'{api_endpoint}/{table_name}'
        response = requests.get(
            method_url,
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        response_dict = json.loads(response.content)

    except requests.exceptions.Timeout:
        logging.error(f'Таймаут при запросе к "{table_name}".')
        raise

    except requests.exceptions.ConnectionError:
        logging.error(f'Ошибка подключения к источнику "{table_name}".')
        raise

    except json.JSONDecodeError:
        logging.error(f'Невалидный JSON в ответе от источника "{table_name}".')
        raise

    values = [(json.dumps(row, ensure_ascii=False),) for row in response_dict]

    return values


def insert_data(table_name, values):
    """Метод загружает данные в DWH."""

    with connection(PG_DWH) as (cursor_pg, conn_pq):

        cursor_pg.executemany(f"""
            INSERT INTO stg.{table_name}
            VALUES (%s)
            ON CONFLICT DO NOTHING
            """, values)

        conn_pq.commit()

        logging.info(f'Таблица stg.{table_name} в DWH заполнена данными, в количестве {len(values)} шт.')


def increment_date(date, context):
    """Метод получает даты для инкрементальной загрузки."""

    try:
        current_date = datetime.strptime(date, '%Y-%m-%d')

        # Узнаем дату последней загрузки из XCom.
        ti = context['ti']
        prev_ti = ti.get_previous_ti()

        if prev_ti is not None:
            last_run_date = prev_ti.xcom_pull(key='last_run_date')
            logging.info(f'Последняя дата загрузки: {last_run_date}')
        else:
            # При первом запуске выгружаем данные за последние 7 дней.
            last_run_date = current_date - timedelta(days=7)
            last_run_date = datetime.strftime(last_run_date, '%Y-%m-%d %H:%M:%S')

            logging.info(f'Дата последней загрузки неизвестна, это первый запуск.')

        current_date = datetime.strftime(current_date, '%Y-%m-%d %H:%M:%S')

        # Сохраняем дату для следующего запуска.
        ti.xcom_push(key='last_run_date', value=current_date)
        logging.info(f'Дата с которой начнётся следующая загрузка: {current_date}')

    except Exception as e:
        logging.error(f'Ошибка при получении дат для инкрементальной загрузки: {e}')
        raise

    return last_run_date, current_date


def migration_data(table_name, date, **context):
    """Метод производит миграцию данных из источника в DWH."""

    logging.info(f'Начался процесс миграции данных из источника "{table_name}" в DWH.')

    values = []

    if table_name == 'deliveries':
        from_date, current_date = increment_date(date, context)
        limit = BATCH_SIZE
        offset = 0

        while True:
            params = {
                'from': from_date,
                'to': current_date,
                'sort_field': 'order_ts',
                'sort_direction': 'asc',
                'limit': limit,
                'offset': offset
            }
            response_dict = uploading_data(table_name, params)

            if not response_dict:
                break

            values.extend(response_dict)
            offset += limit
    else:
        values = uploading_data(table_name)

    logging.info(f'Данные выгружены из источника "{table_name}", в количестве: {len(values)} шт.')

    insert_data(table_name, values)

    logging.info(f'Процесс миграции данных из источника "{table_name}" в DWH успешно окончен!')
