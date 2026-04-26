from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.Script_PG import (
    uploading_data,
    table_names
)

names = table_names.keys()

with DAG(
    'Uploading_data_from_PostgreSQL',
    start_date=datetime(2023, 10, 22),
    schedule_interval='0 0 * * *',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['postgres', 'staging']
) as dag:

    for name in names:
        uploading = PythonOperator(
            task_id=f'uploading_{name}',
            python_callable=uploading_data,
            op_kwargs={
                'table_name': name,
                'sql_insert': table_names[name]
            }
        )

    uploading
