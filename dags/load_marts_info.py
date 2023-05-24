from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0
}

with DAG('5_load_marts', default_args=default_args, schedule_interval='@once') as dag:
    load_marts = BashOperator(
        task_id='load_marts',
        bash_command='cd /kaggle_project && cd /kaggle_project && dbt run --models mart --profiles-dir .',
        dag=dag
    )

load_marts
