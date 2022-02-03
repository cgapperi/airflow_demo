from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

params = {
    "schedule_interval":"@once",
    "catchup":False
}

with DAG(
        dag_id="simple_dag",
        start_date=datetime(2022, 2, 1),
        params=params) as dag:
    start = DummyOperator(
        task_id="start"
    )

    task1 = DummyOperator(
        task_id="task1"
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

    start >> task1 >> end
    start >> task2 >> task3 >> end

