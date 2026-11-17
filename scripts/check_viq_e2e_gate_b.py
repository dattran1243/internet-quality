#!/usr/bin/env python
"""Static/offline checks only. docker compose config never starts a container."""
from __future__ import annotations
import argparse
import ast
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    from processing.e2e.guards import load_manifest, file_hash, verify_sources, require
    from processing.e2e.clickhouse import render_schema
    parser = argparse.ArgumentParser(description="Offline E2E checks; never start application services")
    parser.add_argument("--output-dir", type=Path, help="Store new gate evidence separately; default keeps Gate B behavior")
    parser.add_argument("--baseline", type=Path, help="Protected source baseline for this gate")
    args = parser.parse_args()
    latest = args.output_dir or Path((ROOT/"output/e2e_gate_b/LATEST.txt").read_text(encoding="utf-8"))
    latest.mkdir(parents=True, exist_ok=True)
    manifest = ROOT/"docs/e2e/viq_e2e_run_manifest.json"
    m = load_manifest(manifest); verify_sources(m, ROOT)
    checks = {}
    sources = list((ROOT/"processing/e2e").glob("*.py")) + [ROOT/"api/e2e_isolated.py"]
    sources += list((ROOT/"scripts").glob("*e2e*.py")) + list((ROOT/"infra/airflow/e2e_dags").glob("*.py"))
    for path in sources:
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
    checks["python_compile"] = {"status": "PASS", "files": len(sources)}
    result = subprocess.run([sys.executable, str(ROOT/"tests/test_e2e_gate_b.py")], capture_output=True, text=True, timeout=60)
    (latest/"unit_tests.txt").write_text(result.stdout+result.stderr, encoding="utf-8")
    require(result.returncode == 0, "UNIT_TESTS_FAILED")
    checks["guard_contract_unit_tests"] = {"status": "PASS", "details": "unit_tests.txt; synthetic fixtures/mocks only"}
    compose = subprocess.run(["docker", "compose", "--env-file", str(ROOT/m["host_evidence"]/"compose.env"),
        "-f", str(ROOT/"infra/airflow/docker-compose.viq-e2e-isolated.yml"), "--profile", "e2e-manual",
        "config", "--format", "json"], capture_output=True, text=True, timeout=30)
    require(compose.returncode == 0, "COMPOSE_RENDER_FAILED")
    config = json.loads(compose.stdout); services = config["services"]
    require(len(services) == 5 and config["name"] == m["resources"]["project"], "COMPOSE_SERVICE_SET")
    for name, service in services.items():
        require(service["restart"] == "no" and service["profiles"] == ["e2e-manual"] and
                "build" not in service and service["command"] != ["standalone"], "COMPOSE_AUTOSTART_OR_BUILD")
        require(service.get("mem_limit") and service.get("pids_limit"), "COMPOSE_RESOURCE_LIMITS")
        for mount in service["volumes"]:
            require(not str(mount.get("source", "")).endswith("/.env") and
                    "docker.sock" not in mount["target"] and "/dags" != mount["target"], "UNSAFE_MOUNT")
        if name != "e2e-api":
            require(service["environment"]["AIRFLOW__CORE__PARALLELISM"] == "1", "AIRFLOW_CONCURRENCY")
    for service in (services["e2e-api"], services["e2e-airflow-api"]):
        require(all(p["host_ip"] == "127.0.0.1" for p in service["ports"]), "NON_LOOPBACK_BINDING")
    (latest/"compose.rendered.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    checks["compose_render_and_isolation"] = {"status": "PASS", "services": 5, "daemon_contacted": False}
    ddl = render_schema(m, (ROOT/"infra/clickhouse/e2e_schema.sql").read_text())
    require("CREATE DATABASE "+m["resources"]["database"]+";" in ddl, "DDL_DATABASE")
    checks["sql_template_safety"] = {"status": "PASS", "executed": False}
    baseline = json.loads((args.baseline or latest/"baseline.json").read_bytes())
    protected = {n: file_hash(ROOT/n) == baseline["hashes"][n] for n in baseline["protected"]}
    require(all(protected.values()), "EXISTING_PROTECTED_SOURCE_CHANGED")
    checks["protected_source"] = {"status": "PASS", "files": len(protected)}
    modified = {n: {"before": baseline["hashes"][n], "after": file_hash(ROOT/n),
                   "backup": str(latest/"backups"/n)} for n in baseline["modified_allowed"]}
    (latest/"change_ledger.json").write_text(json.dumps({"modified": modified, "protected": protected}, indent=2), encoding="utf-8")
    # Scan deliverables for exact secret values without ever emitting those values.
    secrets = []
    for line in (ROOT/".env").read_text(encoding="utf-8-sig").splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1); value = value.strip().strip('\"').strip("'")
            if any(s in key.upper() for s in ("PASSWORD", "SECRET", "TOKEN", "ACCESS_KEY")) and len(value) >= 6:
                secrets.append(value)
    deliverables = sources + list((ROOT/"docs/e2e").glob("*")) + [ROOT/"requirements-e2e.txt", ROOT/"infra/clickhouse/e2e_schema.sql"]
    require(not any(secret in p.read_text(encoding="utf-8") for p in deliverables if p.is_file() for secret in secrets), "DELIVERABLE_SECRET_DETECTED")
    checks["secrets_in_deliverables"] = {"status": "PASS", "values_printed": False}
    installed = {}
    for name in ("apache-airflow", "pyspark", "pyarrow", "confluent-kafka", "fastapi", "boto3"):
        try: installed[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError: installed[name] = None
    checks["image_dependencies_real_airflow_import"] = {"status": "DEFERRED", "reason": "This offline command does not validate the image; use separate image validation evidence", "host_package_metadata": installed}
    report = {"status": "STATIC_PASS_RUNTIME_NOT_EXECUTED", "checks": checks,
        "manifest_sha256": file_hash(manifest), "production_data_writes": False, "airflow_started": False,
        "e2e_executed": False, "gate_c_ready": False}
    (latest/"static_checks.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"status": report["status"], "checks_pass": sum(x["status"] == "PASS" for x in checks.values()),
                      "checks_deferred": 1, "report": str(latest/"static_checks.json")}))


if __name__ == "__main__":
    main()
