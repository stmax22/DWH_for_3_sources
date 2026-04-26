from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup


# Данные для подключения к БД.
postgres_DWH = 'PG_WAREHOUSE_CONNECTION'


with DAG(
    'DML_script',
    start_date=datetime(2023, 10, 22),
    schedule_interval='0 1 * * *',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['postgres', 'dds']
) as dag:

    with TaskGroup('group_1') as group_1:
        DML_dm_users = PostgresOperator(
            task_id='DML_dm_users',
            postgres_conn_id=postgres_DWH,
            sql='sql/DML_dm_users.sql'
        )

        DML_dm_restaurants = PostgresOperator(
            task_id='DML_dm_restaurants',
            postgres_conn_id=postgres_DWH,
            sql='sql/DML_dm_restaurants.sql'
        )

        DML_dm_timestamps = PostgresOperator(
            task_id='DML_dm_timestamps',
            postgres_conn_id=postgres_DWH,
            sql='sql/DML_dm_timestamps.sql'
        )

        DML_couriers = PostgresOperator(
            task_id='DML_couriers',
            postgres_conn_id=postgres_DWH,
            sql='sql/DML_couriers.sql'
        )

    with TaskGroup('group_2') as group_2:
        DML_dm_products = PostgresOperator(
            task_id='DML_dm_products',
            postgres_conn_id=postgres_DWH,
            sql='sql/DML_dm_products.sql'
        )

        DML_deliveries = PostgresOperator(
            task_id='DML_deliveries',
            postgres_conn_id=postgres_DWH,
            sql='sql/DML_deliveries.sql'
        )

    DML_dm_orders = PostgresOperator(
        task_id='DML_dm_orders',
        postgres_conn_id=postgres_DWH,
        sql='sql/DML_dm_orders.sql'
    )

    DML_fct_product_sales = PostgresOperator(
        task_id='DML_fct_product_sales',
        postgres_conn_id=postgres_DWH,
        sql='sql/DML_fct_product_sales.sql'
    )

    DML_srv_wf_settings_ts = PostgresOperator(
        task_id='DML_srv_wf_settings_ts',
        postgres_conn_id=postgres_DWH,
        sql='sql/DML_srv_wf_settings_ts.sql'
    )

    group_1 >> group_2 >> DML_dm_orders >> DML_fct_product_sales >> DML_srv_wf_settings_ts
