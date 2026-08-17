from __future__ import annotations

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "state-store"],
)
def task_state_store_demo():
    @task
    def store_and_fetch(**context):
        task_state_store = context["task_state_store"]

        task_state_store.set("greeting", "hello from task_state_store")
        task_state_store.set("run_count", 1)
        task_state_store.set("payload", {"source": "demo", "records": [1, 2, 3]})

        greeting = task_state_store.get("greeting")
        run_count = task_state_store.get("run_count", default=0)
        payload = task_state_store.get("payload")
        missing = task_state_store.get("does_not_exist", default="fallback")

        print(f"greeting = {greeting}")
        print(f"run_count = {run_count}")
        print(f"payload = {payload}")
        print(f"missing (with default) = {missing}")

    @task(retries=2)
    def checkpoint_across_retries(**context):
        task_state_store = context["task_state_store"]

        cursor = task_state_store.get("cursor", default=0)
        print(f"resuming from cursor = {cursor}")

        for i in range(cursor, 5):
            print(f"processing record {i}")
            cursor = i + 1
            task_state_store.set("cursor", cursor)

        print(f"final cursor = {cursor}")

    store_and_fetch() >> checkpoint_across_retries()


task_state_store_demo()
