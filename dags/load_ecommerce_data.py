from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0
}

with DAG('1_load_raw_ecommerce_data', default_args=default_args, schedule_interval='@once') as dag:
    customers = BashOperator(
        task_id='load_customers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: customers}'",
        dag=dag
    )
    sellers = BashOperator(
        task_id='load_sellers',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: sellers}'",
        dag=dag
    )
    geolocation = BashOperator(
        task_id='load_geolocation',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: geolocation}'",
        dag=dag
    )
    order_items = BashOperator(
        task_id='load_order_items',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: order_items}'",
        dag=dag
    )
    order_payments = BashOperator(
        task_id='load_order_payments',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: order_payments}'",
        dag=dag
    )
    order_reviews = BashOperator(
        task_id='load_order_reviews',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: order_reviews}'",
        dag=dag
    )
    orders = BashOperator(
        task_id='load_orders',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: orders}'",
        dag=dag
    )
    product_category_name_translation = BashOperator(
        task_id='load_product_category_name_translation',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: product_category_name_translation}'",
        dag=dag
    )
    products = BashOperator(
        task_id='load_products',
        bash_command="cd /kaggle_project && dbt run-operation load_raw_data --args '{source: ecommerce, file: products}'",
        dag=dag
    )



customers >> sellers >> geolocation >> order_items >> order_payments >> order_reviews >> orders >> product_category_name_translation >> products
