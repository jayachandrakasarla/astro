# """
# ## Dynamic DAG Generator

# Reads DAG definitions from `include/dynamic_dags.yaml` and registers one
# richly structured DAG per entry. Every generated DAG follows the same
# shard -> validate -> quality gate -> transform -> region-aware load ->
# summarize pipeline, demonstrating dynamic task mapping (`.expand`),
# branching (`@task.branch`), task groups, and trigger-rule-based
# notification/cleanup. Keep this file thin — all DAG configuration lives
# in the YAML.
# """

# import os

# import yaml
# from airflow.sdk import dag, task, task_group
# from pendulum import from_format

# CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "include", "dynamic_dags.yaml")

# GLOBAL_SUB_REGIONS = ["us-east", "us-west", "eu-west", "apac"]


# def build_dag(config: dict):
#     dag_id = config["dag_id"]
#     params = config.get("params", {})
#     source_table = params.get("source_table")
#     target_table = params.get("target_table")
#     region = params.get("region", "global")
#     shard_count = config.get("shard_count", 3)
#     quality_threshold = config.get("quality_threshold", 0.7)

#     @dag(
#         dag_id=dag_id,
#         schedule=config.get("schedule"),
#         start_date=from_format(config["start_date"], "YYYY-MM-DD"),
#         catchup=config.get("catchup", False),
#         doc_md=__doc__,
#         default_args={"owner": config.get("owner", "Astro"), "retries": config.get("retries", 1)},
#         tags=config.get("tags", []),
#     )
#     def dynamic_dag():
#         @task
#         def list_shards() -> list[dict]:
#             return [{"shard_id": i, "source_table": source_table, "region": region} for i in range(shard_count)]

#         @task
#         def extract_shard(shard: dict) -> dict:
#             row_count = (shard["shard_id"] + 1) * 1000
#             print(f"Extracted {row_count} rows for shard {shard['shard_id']} from {shard['source_table']}")
#             return {**shard, "row_count": row_count}

#         @task
#         def validate_shard(extracted: dict) -> dict:
#             passed = extracted["row_count"] % 4000 != 0
#             print(f"Validated shard {extracted['shard_id']}: passed={passed}")
#             return {**extracted, "passed": passed}

#         @task.branch
#         def branch_on_quality(validations: list[dict]) -> str:
#             pass_rate = sum(v["passed"] for v in validations) / len(validations)
#             print(f"Pass rate {pass_rate:.2f} vs threshold {quality_threshold}")
#             return "transform.dedupe" if pass_rate >= quality_threshold else "send_quality_alert"

#         @task
#         def send_quality_alert(validations: list[dict]):
#             failed = [v["shard_id"] for v in validations if not v["passed"]]
#             print(f"ALERT: quality gate failed for {dag_id}, shards={failed}")

#         @task_group
#         def transform(validations: list[dict]):
#             @task
#             def dedupe(validations: list[dict]) -> dict:
#                 total_rows = sum(v["row_count"] for v in validations if v["passed"])
#                 return {"stage": "dedupe", "rows": total_rows}

#             @task
#             def enrich(deduped: dict) -> dict:
#                 return {**deduped, "stage": "enrich"}

#             @task
#             def aggregate(enriched: dict) -> dict:
#                 return {**enriched, "stage": "aggregate"}

#             deduped = dedupe(validations)
#             aggregated = aggregate(enrich(deduped))
#             return aggregated, deduped

#         @task.branch
#         def branch_on_region() -> str:
#             return "load_multi_region" if region == "global" else "load_single_region"

#         @task
#         def load_single_region(aggregated: dict):
#             print(f"Loading {aggregated} into {target_table} for region {region}")

#         @task
#         def load_multi_region(aggregated: dict, sub_region: str):
#             print(f"Loading {aggregated} into {target_table} for sub-region {sub_region}")

#         @task(trigger_rule="none_failed_min_one_success")
#         def generate_summary():
#             print(f"Summary generated for {dag_id}")

#         @task(trigger_rule="all_done")
#         def notify_completion():
#             print(f"Notification sent for {dag_id}")

#         @task(trigger_rule="all_done")
#         def cleanup_temp_resources():
#             print(f"Cleaned up temp resources for {dag_id}")

#         shards = list_shards()
#         extracted = extract_shard.expand(shard=shards)
#         validations = validate_shard.expand(extracted=extracted)

#         quality_branch = branch_on_quality(validations)
#         alert = send_quality_alert(validations)
#         aggregated, dedupe_entry = transform(validations)
#         quality_branch >> [dedupe_entry, alert]

#         region_branch = branch_on_region()
#         aggregated >> region_branch

#         single_load = load_single_region(aggregated)
#         multi_load = load_multi_region.partial(aggregated=aggregated).expand(sub_region=GLOBAL_SUB_REGIONS)
#         region_branch >> [single_load, multi_load]

#         summary = generate_summary()
#         [single_load, multi_load, alert] >> summary

#         notify = notify_completion()
#         cleanup = cleanup_temp_resources()
#         summary >> notify >> cleanup

#     return dynamic_dag()


# with open(CONFIG_PATH) as f:
#     for dag_config in yaml.safe_load(f)["dags"]:
#         build_dag(dag_config)
