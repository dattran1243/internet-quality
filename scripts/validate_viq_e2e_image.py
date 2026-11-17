#!/usr/bin/env python
"""Gate B.5 only: package/JVM/CLI/real SDK graph checks, never execute a task.

Run in a new container with --network none, no credentials, a read-only DAG mount,
and temporary AIRFLOW_HOME. No SparkContext/SparkSession or metadata DB is created.
"""
from __future__ import annotations

import contextlib
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys
from unittest.mock import patch
import zipfile

STAGES = (
    "guard_run_and_inputs", "verify_empty_run_resources", "pin_raw_and_scope",
    "publish_raw_kafka", "consume_kafka_bronze", "verify_bronze", "silver_12_windows",
    "build_real_probe_dimension", "enrich_silver", "gold_full_hour", "load_clickhouse_insert_only",
    "verify_clickhouse_rows", "verify_fastapi_same_run", "final_acceptance",
)


@contextlib.contextmanager
def no_database_or_network():
    """Fail and record any connection attempt before it reaches a driver/socket."""
    import sqlite3
    import psycopg2
    from sqlalchemy.engine import Engine
    attempts = []
    def denied(*args, **kwargs):
        attempts.append("CONNECTION_ATTEMPT_FORBIDDEN")
        raise RuntimeError("CONNECTION_ATTEMPT_FORBIDDEN")
    with patch.object(Engine, "connect", denied), patch.object(Engine, "raw_connection", denied), \
         patch.object(sqlite3, "connect", denied), patch.object(psycopg2, "connect", denied), \
         patch.object(socket.socket, "connect", denied), patch.object(socket.socket, "connect_ex", denied), \
         patch.object(socket, "create_connection", denied):
        yield attempts


def cli_probe(args):
    output = io.StringIO(); error = io.StringIO(); code = 0
    with no_database_or_network() as attempts, contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
        try:
            from airflow.__main__ import main
            sys.argv = ["airflow", *args]
            main()
        except SystemExit as exc:
            code = exc.code or 0
        except Exception as exc:
            code = 1
            error.write(type(exc).__name__)
    text = output.getvalue()+error.getvalue()
    result = {"args": args, "exit_code": code, "connection_attempts": len(attempts),
              "output_sha256": hashlib.sha256(text.encode()).hexdigest(),
              "version_is_3_3_2": "3.3.2" in text,
              "has_host_port_workers": all(x in text for x in ("--host", "--port", "--workers")),
              "stderr_tail": error.getvalue()[-1200:] if code else ""}
    print(json.dumps(result))
    return 0 if code == 0 and not attempts else 1


def versions():
    expected = {"apache-airflow": "3.3.2", "apache-airflow-core": "3.3.2", "pyspark": "3.5.3",
                "boto3": "1.43.75", "botocore": "1.43.75", "requests": "2.34.2",
                "confluent-kafka": "2.6.1", "pyarrow": "18.1.0"}
    found = {name: importlib.metadata.version(name) for name in expected}
    assert found == expected, "PACKAGE_VERSION_MISMATCH"
    import pyspark
    assert pyspark.SparkContext._active_spark_context is None
    java = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=15)
    assert java.returncode == 0 and 'version "17.' in java.stderr, "JAVA_NOT_17"
    pip = subprocess.run([sys.executable, "-m", "pip", "check"], capture_output=True, text=True, timeout=60)
    assert pip.returncode == 0, "PIP_CHECK_FAILED:" + pip.stdout[-1500:]
    freeze = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True, timeout=30)
    assert freeze.returncode == 0
    return {"status": "PASS", "packages": found, "python": sys.version.split()[0],
            "java": java.stderr.strip(), "pip_check": pip.stdout.strip(),
            "package_freeze": freeze.stdout.splitlines(), "spark_context_created": False}


def real_dag(dag_path):
    with no_database_or_network() as attempts:
        from airflow.sdk import DAG, task, get_current_context
        namespace = runpy.run_path(dag_path, run_name="viq_b5_real_dag_import")
        dag = namespace["dag"]
        assert isinstance(dag, DAG)
        assert dag.dag_id == "viq_e2e_isolated_once" and dag.schedule is None
        assert dag.is_paused_upon_creation and not dag.catchup
        assert dag.max_active_tasks == 1 and dag.max_active_runs == 1
        assert dag.dagrun_timeout.total_seconds() == 2700
        ordered = [t.task_id for t in dag.topological_sort()]
        assert ordered == list(STAGES) and len(dag.tasks) == 14
        edges = []
        for i, stage in enumerate(STAGES):
            operator = dag.get_task(stage)
            expected = {STAGES[i-1]} if i else set()
            assert operator.upstream_task_ids == expected
            assert operator.retries == 0 and operator.pool == "viq_e2e_once"
            assert getattr(operator.trigger_rule, "value", operator.trigger_rule) == "all_success", "TRIGGER_RULE_NOT_ALL_SUCCESS"
            edges.extend((previous, stage) for previous in sorted(expected))
        assert not attempts, "DAG_IMPORT_ATTEMPTED_CONNECTION"
    return {"status": "PASS", "dag_class": type(dag).__module__+"."+type(dag).__name__,
            "task_count": len(dag.tasks), "edges": edges, "task_ids": ordered,
            "connection_attempts": len(attempts), "metadata_initialized": False,
            "tasks_executed": 0, "task_sdk_import": "PASS"}


def jar_dependencies():
    import pyspark
    from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway
    spark_jars = Path(pyspark.__file__).parent/"jars"
    jars = [Path("/opt/e2e/jars/hadoop-aws-3.3.4.jar"), Path("/opt/e2e/jars/aws-java-sdk-bundle-1.12.262.jar")]
    records = []
    for jar, artifact, version in zip(jars, ("hadoop-aws", "aws-java-sdk-bundle"), ("3.3.4", "1.12.262")):
        with zipfile.ZipFile(jar) as z:
            names = [n for n in z.namelist() if n.endswith("/"+artifact+"/pom.properties")]
            assert len(names) == 1, "JAR_MAVEN_METADATA"
            properties = z.read(names[0]).decode()
            assert "version="+version in properties, "JAR_VERSION"
        records.append({"file": jar.name, "version": version, "sha256": hashlib.sha256(jar.read_bytes()).hexdigest()})
    classpath = os.pathsep.join([str(spark_jars/"*"), *map(str, jars)])
    port, process = launch_gateway(classpath=classpath, javaopts=["-Xmx1g", "-XX:ActiveProcessorCount=1",
        "-XX:ParallelGCThreads=1", "-XX:ConcGCThreads=1", "-Xss512k"], return_proc=True, die_on_exit=True)
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
    try:
        jvm = gateway.jvm
        version = jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        assert version == "3.3.4", "HADOOP_CORE_MISMATCH"
        aws = jvm.com.amazonaws.util.VersionInfoUtils.getVersion()
        assert aws == "1.12.262", "AWS_SDK_VERSION"
        loader = jvm.java.lang.ClassLoader.getSystemClassLoader()
        classes = {}
        for name in ("org.apache.hadoop.fs.s3a.S3AFileSystem", "com.amazonaws.services.s3.AmazonS3Client"):
            cls = jvm.java.lang.Class.forName(name, False, loader)
            classes[name] = str(cls.getProtectionDomain().getCodeSource().getLocation())
        assert pyspark.SparkContext._active_spark_context is None, "SPARK_CONTEXT_MUST_NOT_START"
        return {"status": "PASS", "hadoop_core": version, "aws_sdk": aws, "jars": records,
                "class_load_without_initialization": classes,
                "spark_context_created": False, "spark_etl_executed": False}
    finally:
        gateway.shutdown()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate(); process.wait(timeout=10)


def main():
    if sys.argv[1:2] == ["--cli-probe"]:
        return cli_probe(sys.argv[2:])
    result = {"scope": "GATE_B5_IMAGE_ONLY", "e2e_executed": False, "production_data_writes": False}
    failed = False
    for name, function in (("versions_and_pip", versions),
                            ("real_dag_import", lambda: real_dag("/opt/validation/viq_e2e_isolated_once.py")),
                            ("s3a_dependencies", jar_dependencies)):
        try:
            result[name] = function()
        except Exception as exc:
            result[name] = {"status": "FAIL", "error_type": type(exc).__name__, "error": str(exc)[:1500]}
            failed = True
    cli = []
    for args in (["version"], ["api-server", "--help"], ["dag-processor", "--help"], ["scheduler", "--help"],
                 ["pools", "set", "--help"], ["dags", "list", "--help"]):
        probe = subprocess.run([sys.executable, __file__, "--cli-probe", *args], capture_output=True, text=True, timeout=45)
        try:
            value = json.loads(probe.stdout.splitlines()[-1])
        except Exception:
            value = {"args": args, "parse_error": True, "stderr_tail": probe.stderr[-1000:]}
        value["status"] = "PASS" if probe.returncode == 0 else "FAIL"
        if value["status"] == "FAIL": failed = True
        cli.append(value)
    result["airflow_cli"] = cli
    db = Path("/tmp/airflow/forbidden.db")
    assert not db.exists(), "METADATA_DATABASE_WAS_CREATED"
    result["metadata_database_created"] = False
    result["status"] = "FAIL" if failed else "PASS"
    print("VIQ_B5_RESULT="+json.dumps(result, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
