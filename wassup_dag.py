from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
    "owner": "Husseljo",
    "start_date": datetime(2023, 3, 4),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate a DAG object
dag = DAG(
    dag_id="wassup_dag",
    default_args=default_args,
    description="Wassup DAG",
    schedule_interval="* * * * *",
    catchup=False,
    tags=["WASSUP, GANG"],
)


def print_hello():
    return "Hello world!"


start_task = DummyOperator(task_id="start_task", dag=dag)

# Creating second task
wassup_task = PythonOperator(
    task_id="wassup_task", python_callable=print_hello, dag=dag
)

# Creating third task
end_task = DummyOperator(task_id="end_task", dag=dag)

start_task >> wassup_task >> end_task
