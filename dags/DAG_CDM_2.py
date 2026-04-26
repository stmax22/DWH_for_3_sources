from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Данные для подключения к БД.
postgres_DWH = 'PG_WAREHOUSE_CONNECTION'


with DAG(
    'DML_dm_courier_ledger',
    start_date=datetime(2023, 10, 22),
    schedule_interval='0 1 10 * *',  # Данные должны быть подготовлены 10 числа каждого месяца.
    catchup=False,
    is_paused_upon_creation=False,
    tags=['postgres', 'cdm_2']
) as dag:

    DML_dm_courier_ledger = PostgresOperator(
        task_id='DML_dm_courier_ledger',
        postgres_conn_id=postgres_DWH,
        sql='sql/DML_dm_courier_ledger.sql'
        )

    DML_dm_courier_ledger 
