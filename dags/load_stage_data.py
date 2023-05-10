from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0
}

with DAG('1_load_stage_data', default_args=default_args, schedule_interval='@once') as dag:
    customers = BashOperator(
        task_id='load_customers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: customers}'",
        dag=dag
    )
    sellers = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: sellers}'",
        dag=dag
    )
    geolocation = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: geolocation}'",
        dag=dag
    )
    order_items = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: order_items}'",
        dag=dag
    )
    order_payments = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: order_payments}'",
        dag=dag
    )
    order_reviews_raw = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: order_reviews_raw}'",
        dag=dag
    )
    orders = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: orders}'",
        dag=dag
    )
    product_category_name_translation = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: product_category_name_translation}'",
        dag=dag
    )
    products = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: products}'",
        dag=dag
    )



customers >> sellers >> geolocation >> order_items >> order_payments >> order_reviews_raw >> orders >> product_category_name_translation >> products
