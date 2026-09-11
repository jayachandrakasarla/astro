from __future__ import annotations

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import dag, task
from pendulum import DateTime, datetime

HTTP_CONN_ID = "airflow_api"
NON_MANUAL_RUN_TYPES = ["scheduled", "backfill", "asset_triggered"]


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "rest-api"],
)
def dag_runs_in_range():
    @task
    def get_dag_runs_in_range(
        dag,
        data_interval_start: DateTime,
        data_interval_end: DateTime,
    ) -> list[dict]:
        hook = HttpHook(method="GET", http_conn_id=HTTP_CONN_ID)
        response = hook.run(
            endpoint=f"/api/v2/dags/{dag.dag_id}/dagRuns",
            data={
                "logical_date_gte": data_interval_start.isoformat(),
                "logical_date_lte": data_interval_end.isoformat(),
                "run_type": NON_MANUAL_RUN_TYPES,
            },
        )
        return response.json()["dag_runs"]

    @task
    def summarize(dag_runs: list[dict]):
        print(f"Found {len(dag_runs)} non-manual run(s) in range")
        for run in dag_runs:
            print(f"  {run['run_id']}: {run['state']}")

    summarize(get_dag_runs_in_range())


dag_runs_in_range()
