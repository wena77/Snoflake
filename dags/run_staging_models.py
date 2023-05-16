from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0
}

with DAG('3_run_staging_models', default_args=default_args, schedule_interval='@once') as dag:
    run_staging_models = BashOperator(
        task_id='run_staging_models',
        bash_command='cd /kaggle_project && cd /kaggle_project && dbt run --models staging --profiles-dir .',
        dag=dag
    )

run_staging_models
