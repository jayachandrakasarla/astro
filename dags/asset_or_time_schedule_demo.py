"""Sample DAG demonstrating AssetOrTimeSchedule: runs on a daily cron AND on asset updates."""

from airflow.sdk import Asset, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime

upstream_asset_1 = Asset("asset_or_time_schedule_demo_asset_1")
upstream_asset_2 = Asset("asset_or_time_schedule_demo_asset_2")


@dag(
    dag_id="asset_or_time_schedule_demo",
    start_date=datetime(2026, 1, 1),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        # () rather than [] enables conditional (OR) asset scheduling
        assets=(upstream_asset_1 | upstream_asset_2),
    ),
    catchup=False,
)
def asset_or_time_schedule_demo():
    @task
    def report_trigger(**context):
        print(f"Triggered by: {context['dag_run'].run_type}")

    report_trigger()


asset_or_time_schedule_demo()
