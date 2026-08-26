from __future__ import annotations

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
from pendulum import datetime

SAMPLE_CODE = """
import platform
import sys

print("Hello from a Kubernetes pod launched by KPO!")
print(f"Python version: {sys.version}")
print(f"Platform: {platform.platform()}")

total = sum(range(1, 101))
print(f"Sum of 1..100 = {total}")
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "kpo", "kubernetes"],
)
def kpo_sample_dag():
    @task
    def start():
        print("Preparing to launch a Kubernetes pod via KPO.")

    run_in_pod = KubernetesPodOperator(
        task_id="run_python_in_pod",
        namespace=conf.get("kubernetes_executor", "namespace", fallback="default"),
        name="kpo-python-slim",
        image="python:slim",
        cmds=["python", "-c", SAMPLE_CODE],
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        random_name_suffix=True,
    )

    @task
    def finish():
        print("Kubernetes pod task completed.")

    start() >> run_in_pod >> finish()


kpo_sample_dag()
