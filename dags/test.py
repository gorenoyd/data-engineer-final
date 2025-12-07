from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello from Airflow DAG")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello,
    )

    task_hello
