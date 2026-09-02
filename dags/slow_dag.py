import datetime
from airflow.sdk import dag, task
import time

#sleep for 10 seconds at top level
# time.sleep(10)

@dag(
    dag_id="slow_dag",
    start_date=datetime.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def slow_dag():
    @task
    def hello_airflow():
        print("Hello from Airflow 3!")

    hello_airflow()

slow_dag()
