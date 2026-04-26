from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.Script_API import (
    migration_data,
    table_names
)


with DAG(
    'Uploading_data_from_API',
    start_date=datetime(2023, 10, 22),
    schedule_interval='0 0 * * *',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['API', 'staging']
) as dag:

    for name in table_names:
        uploading_data = PythonOperator(
            task_id=f'uploading_data_{name}',
            python_callable=migration_data,
            provide_context=True,
            op_kwargs={
                'table_name': name,
                'date': '{{ ds }}'
                }
        )

    uploading_data
