"""Example: run file-processing logic in a dedicated Kubernetes pod via @task.kubernetes.

The decorated function's body executes inside the pod built from `image`. Its return
value (a dict here) is serialized to XCom automatically when do_xcom_push=True, so
downstream tasks pull it exactly like a normal @task return.
"""

from __future__ import annotations

from pendulum import datetime

from airflow.configuration import conf
from airflow.decorators import dag, task


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["kubernetes", "example"],
)
def process_files_k8s():
    @task
    def list_files() -> list[str]:
        # Stand-in for however you discover work (bucket listing, DB query, etc.).
        return ["s3://data/a.csv", "s3://data/b.csv", "s3://data/c.csv"]

    @task.kubernetes(
        image="python:3-slim",
        namespace="re",
        name="process-files",
        get_logs=True,
        log_events_on_failure=True,
        do_xcom_push=True,
    )
    def process_files(files: list[str]) -> dict:
        # This whole body runs inside the pod. Put your real logic here.
        # The image must have any libraries this code imports.
        processed = []
        total_bytes = 0
        for path in files:
            # ... download, transform, write results ...
            size = len(path)  # placeholder for real per-file work
            total_bytes += size
            processed.append(path)

        # Returned dict is pushed to XCom via /airflow/xcom/return.json (handled by the decorator).
        return {
            "processed_count": len(processed),
            "total_bytes": total_bytes,
            "paths": processed,
        }

    @task
    def report(result: dict) -> None:
        print(f"Processed {result['processed_count']} files, {result['total_bytes']} bytes")
        for path in result["paths"]:
            print(f"  - {path}")

    report(process_files(list_files()))


process_files_k8s()
