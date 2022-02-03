from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from random import randint

params = {
    "schedule_interval":"@once",
    "catchup":False
}

def hello_world(**kwargs):
    print("Hello World!")

def add_ints(int1, int2):
    return int1 + int2

def sub_ints(int1, int2):
    return int1 - int2

def mult_ints(int1, int2):
    return int1 * int2

def div_ints(int1, int2):
    return int1/int2

def perform_math(operator, int1, int2):
    ops = {}
def do_math(**kwargs):
    ti = kwargs['ti']
    operator = kwargs['operator']

    start = kwargs['start_int']
    end = kwargs['end_int']
    int1 = randint(start, end)
    int2 = randint(start, end)

    print(int1)
    print(int2)
    print(operator)

    if operator == "add":
        result = add_ints(int1, int2)
    elif operator == "sub":
        result = sub_ints(int1, int2)
    elif operator ==  "mul":
        result = mult_ints(int1, int2)
    elif operator == "div":
        result = div_ints(int1, int2)
    else:
        result = "BAD OPERATOR"
    return operator, int1, int2, result

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

    date = BashOperator(
        task_id="date",
        bash_command = 'date'
    )

    simple_ops = DummyOperator(
        task_id="simple_ops"
    )

    complex_ops = DummyOperator(
        task_id="complex_ops"
    )

    add  = PythonOperator(
        task_id="add",
        python_callable=do_math,
        op_kwargs={"operator": "add",
                   "start_int": 1,
                   "end_int": 10
                   }
    )

    sub = PythonOperator(
        task_id="sub",
        python_callable=do_math,
        op_kwargs={"operator": "sub",
                   "start_int": 10,
                   "end_int": 20
                   }
    )

    mult = PythonOperator(
        task_id="mult",
        python_callable=do_math,
        op_kwargs={"operator": "mul",
                   "start_int": 5,
                   "end_int": 25
                   }
    )

    div = PythonOperator(
        task_id="div",
        python_callable=do_math,
        op_kwargs={"operator": "div",
                   "start_int": 10,
                   "end_int": 100
                   }
    )

    bad_op = PythonOperator(
        task_id="bad_op",
        python_callable=do_math,
        op_kwargs={"operator": "mod",
               "start_int": 1,
               "end_int": 30
               }
    )

    end = DummyOperator(
    task_id="end"
    )

    start >> helloWorld >> end
    start >> date >> end
    start >> simple_ops >> [ add, sub] >> end
    start >> complex_ops >> [ mult, div, bad_op] >> end

