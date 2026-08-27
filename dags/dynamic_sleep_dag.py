from __future__ import annotations

import time

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "dynamic-task-mapping"],
)
def dynamic_sleep_dag():
    @task
    def sleep_task(index: int):
        time.sleep(5)
        return index

    sleep_task.expand(index=list(range(20)))


dynamic_sleep_dag()
