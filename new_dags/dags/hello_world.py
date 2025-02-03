from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello World")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 2),
    'retries': 1,
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world,
    )

    task1