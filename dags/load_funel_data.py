from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0
}

with DAG('2_load_raw_funel_data', default_args=default_args, schedule_interval='@once') as dag:
    closed_deals = BashOperator(
        task_id='load_closed_deals',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: funel, file: closed_deals}'",
        dag=dag
    )
    marketing_qualified_leads = BashOperator(
        task_id='load_marketing_qualified_leads',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: funel, file: marketing_qualified_leads}'",
        dag=dag
    )

closed_deals >> marketing_qualified_leads