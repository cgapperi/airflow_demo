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

def do_math(**kwargs):
    ti = kwargs['ti']
    print(ti)
    operator = kwargs['operator']

    start = kwargs['start_int']
    end = kwargs['end_int']
    int1 = randint(start, end)
    int2 = randint(start, end)

    print(int1)
    print(int2)
    print(operator)

    if operator == "add":
        ti.xcom_push(key='add_int1', value=int1)
        ti.xcom_push(key='add_int2', value=int2)
        result = add_ints(int1, int2)
    elif operator == "sub":
        ti.xcom_push(key='sub_int1', value=int1)
        ti.xcom_push(key='sub_int2', value=int2)
        result = sub_ints(int1, int2)
    elif operator ==  "mul":
        print(f"rand_int1: {int1}")
        print(f"rand_int2: {int2}")

        int1 = ti.xcom_pull(key='add_int1', task_ids='add')
        int2 = ti.xcom_pull(key='add_int2', task_ids='add')

        print(f"add_int1: {int1}")
        print(f"add_int2: {int2}")
        result = mult_ints(int1, int2)
    elif operator == "div":
        print(f"rand_int1: {int1}")
        print(f"rand_int2: {int2}")

        int1 = ti.xcom_pull(key='sub_int1', task_ids='sub')
        int2 = ti.xcom_pull(key='sub_int2', task_ids='sub')

        print(f"add_int1: {int1}")
        print(f"add_int2: {int2}")
        result = div_ints(int1, int2)
    else:
        result = "BAD OPERATOR"
    return operator, int1, int2, result

with DAG(
        dag_id="more_complex",
        start_date=datetime(2022, 2, 1),
        params=params) as dag:

    start = DummyOperator(
        task_id="start"
    )

    simple_path = DummyOperator(
        task_id="simple_path"
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

    simple_path_exit = DummyOperator(
        task_id = "simple_path_exit"
    )

    simple_ops_exit = DummyOperator(
        task_id = "simple_ops_exit"
    )

    complex_ops_exit = DummyOperator(
        task_id = "complex_ops_exit"
    )

    end = DummyOperator(
    task_id="end"
    )

    start >> simple_path >> [ helloWorld, date] >> simple_path_exit
    start >> simple_ops >> [ add, sub] >> simple_ops_exit
    start >> complex_ops >> [ mult, div, bad_op] >> complex_ops_exit
    add >> mult
    sub >> div
    [simple_path_exit, simple_ops_exit, complex_ops_exit] >> end

