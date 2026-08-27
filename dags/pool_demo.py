from __future__ import annotations

import time

from airflow.sdk import dag, task
from pendulum import datetime

# This DAG demonstrates how an Airflow pool caps the number of tasks that hit a
# shared, fragile resource (here, a pretend rate-limited API) at the same time.
#
# All the "fetch" tasks below are assigned to a pool called `api_pool`. Even
# though five of them are runnable at once, the pool only lets a few run
# concurrently. The rest queue until a slot frees up.
#
# Pools are NOT created from DAG code — create `api_pool` in your environment
# first, e.g.:
#     af config pools                              # inspect existing pools
#     astro deployment pool create --name api_pool --slots 3   # on Astro
#     airflow pools set api_pool 3 "Limit API concurrency"     # OSS CLI / UI


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "pools"],
)
def pool_demo():
    def _call_api(name: str, seconds: int) -> str:
        # Stand-in for a real API call so the DAG has no external dependency.
        print(f"[{name}] calling API...")
        time.sleep(seconds)
        print(f"[{name}] done")
        return name

    # POOL TO CREATE: api_pool  |  SLOTS: 3
    # Occupies 1 slot. Highest priority, so it drains the pool first when full.
    @task(pool="api_pool", pool_slots=1, priority_weight=3)
    def fetch_orders():
        return _call_api("fetch_orders", 10)

    # POOL TO CREATE: api_pool  |  SLOTS: 3
    # Occupies 1 slot.
    @task(pool="api_pool", pool_slots=1, priority_weight=2)
    def fetch_customers():
        return _call_api("fetch_customers", 10)

    # POOL TO CREATE: api_pool  |  SLOTS: 3
    # Occupies 1 slot.
    @task(pool="api_pool", pool_slots=1, priority_weight=1)
    def fetch_products():
        return _call_api("fetch_products", 10)

    # POOL TO CREATE: api_pool  |  SLOTS: 3
    # HEAVY task: pool_slots=2 means it consumes TWO of the three slots while
    # running, so it noticeably reduces how many other fetches run alongside it.
    @task(pool="api_pool", pool_slots=2, priority_weight=1)
    def fetch_full_export():
        return _call_api("fetch_full_export", 15)

    # NO POOL: runs in default_pool (128 slots). This task does not touch the
    # rate-limited API, so it is deliberately left out of api_pool and can run
    # freely regardless of how full api_pool is.
    @task
    def summarize(results: list[str]):
        print(f"fetched {len(results)} datasets: {', '.join(results)}")

    fetched = [
        fetch_orders(),
        fetch_customers(),
        fetch_products(),
        fetch_full_export(),
    ]
    summarize(fetched)


pool_demo()
