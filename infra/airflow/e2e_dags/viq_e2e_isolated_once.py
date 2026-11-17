"""Only this folder is mounted into the isolated Airflow DAG bundle."""
from datetime import datetime, timedelta, timezone
from airflow.sdk import DAG, get_current_context, task

STAGES = (
    "guard_run_and_inputs", "verify_empty_run_resources", "pin_raw_and_scope",
    "publish_raw_kafka", "consume_kafka_bronze", "verify_bronze",
    "silver_12_windows", "build_real_probe_dimension", "enrich_silver",
    "gold_full_hour", "load_clickhouse_insert_only", "verify_clickhouse_rows",
    "verify_fastapi_same_run", "final_acceptance",
)
TIMEOUTS = (120, 120, 120, 180, 180, 120, 600, 180, 300, 300, 180, 180, 180, 120)


def spark_temporary_storage(stage, manifest, env):
    """Bind Python/JVM/Spark temp to scheduler scratch before spark-submit."""
    import os
    import stat
    from pathlib import Path
    from processing.e2e.guards import require
    from processing.e2e.layout import reject_links

    require(stage in ("silver_12_windows", "build_real_probe_dimension", "enrich_silver", "gold_full_hour"),
            "SPARK_TEMP_STAGE")
    root = Path("/opt/e2e/scratch")
    require(root.is_dir() and os.getuid() == 50000, "SPARK_TEMP_OWNER")
    directory = root
    for name in ("spark-temporary", manifest["resources"]["rid"], stage):
        require(name and "/" not in name and "\\" not in name and name not in (".", ".."), "SPARK_TEMP_COMPONENT")
        directory = directory/name
        reject_links(directory)
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.stat()
        require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700, "SPARK_TEMP_DIRECTORY")
    paths = {}
    for name in ("java", "local", "python"):
        path = directory/name
        reject_links(path)
        path.mkdir(mode=0o700, exist_ok=True)
        info = path.stat()
        require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700, "SPARK_TEMP_DIRECTORY")
        paths[name] = str(path)
    capacity = os.statvfs(directory)
    require(capacity.f_bavail*capacity.f_frsize >= 1024**3, "SPARK_TEMP_CAPACITY")
    env.update(TMPDIR=paths["python"], SPARK_LOCAL_DIRS=paths["local"])
    return paths


@task(retries=0, pool="viq_e2e_once", trigger_rule="all_success")
def run_stage(stage, upstream=None):
    import json
    import os
    from pathlib import Path
    import sys
    from processing.e2e.guards import gate_c, load_manifest
    from processing.e2e.runtime import bounded_process

    context = get_current_context()
    path = os.environ["VIQ_E2E_MANIFEST"]; manifest = load_manifest(path)
    gate_c(manifest, path, os.environ["VIQ_E2E_APPROVAL"], "run")
    ti = context["ti"]
    env = dict(os.environ)
    env.update(VIQ_ACTUAL_DAG_ID=ti.dag_id, VIQ_ACTUAL_RUN_ID=context["dag_run"].run_id,
               VIQ_ACTUAL_TASK_ID=ti.task_id, VIQ_ACTUAL_TRY_NUMBER=str(ti.try_number))
    if upstream:
        env.update(VIQ_UPSTREAM_URI=upstream["uri"], VIQ_UPSTREAM_SHA256=upstream["sha256"])
    root = Path(manifest["container_evidence"])
    script = "/opt/project/scripts/e2e_stage.py"
    argv = [sys.executable, "-u", script, "--stage", stage]
    if stage in ("silver_12_windows", "build_real_probe_dimension", "enrich_silver", "gold_full_hour"):
        temp = spark_temporary_storage(stage, manifest, env)
        argv = ["spark-submit", "--master", "local[1]", "--driver-memory", "1g",
                "--driver-java-options", "-XX:ActiveProcessorCount=1 -XX:ParallelGCThreads=1 -XX:ConcGCThreads=1 -Xss512k -Djava.io.tmpdir="+temp["java"],
                "--conf", "spark.local.dir="+temp["local"],
                "--jars", "/opt/e2e/jars/hadoop-aws-3.3.4.jar,/opt/e2e/jars/aws-java-sdk-bundle-1.12.262.jar",
                script, "--stage", stage]
    bounded_process(argv, env, root / (stage + ".log"), TIMEOUTS[STAGES.index(stage)])
    return json.loads((root / (stage + ".result.json")).read_bytes())


with DAG(
    dag_id="viq_e2e_isolated_once", schedule=None, catchup=False,
    start_date=datetime(2026, 10, 18, tzinfo=timezone.utc),
    is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=45), default_args={"retries": 0},
    tags=["e2e", "isolated", "historical-replay", "manual-only"],
) as dag:
    previous = None
    for stage, timeout in zip(STAGES, TIMEOUTS):
        previous = run_stage.override(task_id=stage, execution_timeout=timedelta(seconds=timeout+15))(stage, previous)
