from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from x_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_cwd():
    import os
    print("Current working dir:", os.getcwd())

dag = DAG(
    'x_etl_dag',
    default_args=default_args,
    description='A simple DAG to run Twitter ETL',
)

print_cwd_task = PythonOperator(
    task_id='print_cwd',
    python_callable=print_cwd,
    dag=dag,
)

run_etl_task = PythonOperator(
    task_id='run_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag,
)
print_cwd_task >> run_etl_task