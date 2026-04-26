from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.Script_MongoDB import (
    uploading_data,
    table_names,
    mongodb_name
)


with DAG(

    'Uploading_data_from_MongoDB',
    start_date=datetime(2023, 10, 22),
    schedule_interval='0 0 * * *',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['mongodb', 'staging']
) as dag:

    for name in table_names:
        uploading = PythonOperator(
            task_id=f'uploading_data_{name}',
            python_callable=uploading_data,
            op_kwargs={
                'db_name': mongodb_name,
                'table_name': name
            }
        )

    uploading
