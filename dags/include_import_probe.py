from __future__ import annotations

from airflow.sdk import dag, task
from pendulum import datetime

from include.probe_lib import get_probe_message


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["probe", "include"],
)
def include_import_probe():
    @task
    def log_probe_message():
        message = get_probe_message()
        print(message)
        return message

    log_probe_message()


include_import_probe()
