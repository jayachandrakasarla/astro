from __future__ import annotations

from airflow.sdk import Variable, dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "secrets"],
)
def hello_dag():
    @task
    def print_hello_world():
        value = Variable.get("hello-world")
        print(f"hello-world = {value}")

        return value

    @task
    def print_value(val):
        print(f"value from prev task: {val}")

    t1 = print_hello_world()
    t2 = print_value(t1)

    t1 >> t2

hello_dag()
