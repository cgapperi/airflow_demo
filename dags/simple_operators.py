from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

params = {
    "schedule_interval":"@once",
    "catchup":False
}

def hello_world(**kwargs):
    print("Hello World!")

with DAG(
        dag_id="simple_dag",
        start_date=datetime(2022, 2, 1),
        params=params) as dag:
    start = DummyOperator(
        task_id="start"
    )

    helloWorld = PythonOperator(
        task_id="hello_world",
        python_callable=hello_world,
    )

    task2 = DummyOperator(
        task_id="task2"
    )

    task3 = DummyOperator(
        task_id="task3"
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> helloWorld >> end
    start >> task2 >> task3 >> end

