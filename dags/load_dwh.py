from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0
}

with DAG('4_load_to_dwh', default_args=default_args, schedule_interval='@once') as dag:
    load_to_dwh = BashOperator(
        task_id='load_to_dwh',
        bash_command='cd /kaggle_project && cd /kaggle_project && dbt run --models dwh --profiles-dir .',
        dag=dag
    )

load_to_dwh
