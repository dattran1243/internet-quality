"""Fail closed before imports of project settings or any network client."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit, unquote

from processing.e2e.contracts import ARCHIVE_SHA256, DAG_ID, RAW_SHA256, STAGES


class GuardError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise GuardError(code)


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def namespace(run_id):
    require(bool(re.fullmatch(r"manual__viq_e2e_\d{8}T\d{6}Z_[a-f0-9]{12}", run_id)), "RUN_ID_FORMAT")
    rid = "e" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    return {
        "rid": rid, "project": "viq-e2e-" + rid,
        "topic": "viq.e2e.raw." + rid, "dlq": "viq.e2e.dlq." + rid,
        "group": "viq.e2e.bronze." + rid, "bucket": "viq-e2e-" + rid,
        "database": "viq_e2e_" + rid, "metadata_database": "viq_e2e_meta_" + rid,
        "metadata_user": "viq_e2e_meta_" + rid,
        "ch_loader_user": "viq_e2e_loader_" + rid,
        "ch_api_user": "viq_e2e_api_" + rid,
    }


ENDPOINTS = {
    "kafka": "internet-quality-kafka:39092",
    "minio": "http://vietnam-internet-quality-minio-1:9000",
    "clickhouse_host": "viq-g1-clickhouse", "clickhouse_port": 8123,
    "postgres_host": "internet-quality-airflow-postgres", "postgres_port": 5432,
    "airflow_internal": "http://e2e-airflow-api:8080",
    "airflow_host": "http://127.0.0.1:18080",
    "api_internal": "http://e2e-api:8000", "api_host": "http://127.0.0.1:18000",
}


def load_manifest(path):
    m = json.loads(Path(path).read_text(encoding="utf-8"))
    require(m.get("schema_version") == 1 and m.get("dag_id") == DAG_ID, "MANIFEST_SCHEMA")
    require(m.get("resources") == namespace(m.get("run_id", "")), "NAMESPACE_MISMATCH")
    require(m.get("endpoints") == ENDPOINTS, "ENDPOINT_MISMATCH")
    require(m.get("raw_sha256") == RAW_SHA256 and m.get("archive_sha256") == ARCHIVE_SHA256, "INPUT_PINS")
    require(m.get("input_scope") == {"country": "DE", "measurement": 177401244,
            "start": "2026-10-18T13:00:00Z", "stop": "2026-10-18T14:00:00Z",
            "records": 375, "probes": 24, "windows": 12}, "INPUT_SCOPE")
    require(m.get("host_evidence") == "output/e2e/" + m["resources"]["rid"], "EVIDENCE_PATH")
    require(m.get("container_evidence") == "/opt/e2e/evidence", "CONTAINER_EVIDENCE_PATH")
    require(m.get("minimum_vm_bytes") == int(11.5*1024**3), "MEMORY_GATE_CHANGED")
    require(m.get("minimum_available_bytes") == 3*1024**3 and m.get("dashboard_accepted") is False and
            m.get("live_ingestion") is False and m.get("acceptance") == "E2E_FULL_HOUR_SANDBOX_TO_API_PASS", "ACCEPTANCE_SCOPE_CHANGED")
    from processing.e2e.layout import layout_for
    require(m.get("runtime_layout") == layout_for(m["resources"]["rid"]), "RUNTIME_LAYOUT_BINDING")
    mode = m.get("provision_attestation", {}).get("mode")
    require(mode in ("creation_v1", "security_rebind_v2"), "ATTESTATION_MODE")
    if mode == "security_rebind_v2":
        require(all(re.fullmatch(r"[a-f0-9]{64}", m["provision_attestation"].get(k, ""))
                    for k in ("original_manifest_sha256", "original_attestation_sha256")), "ATTESTATION_ANCHOR_FORMAT")
    require(bool(m.get("source_sha256")), "SOURCE_PINS_MISSING")
    for name, value in m["source_sha256"].items():
        require(not Path(name).is_absolute() and ":" not in name and "\\" not in name and ".." not in Path(name).parts and
                bool(re.fullmatch(r"[a-f0-9]{64}", value)), "SOURCE_PIN_FORMAT")
    return m


def gate_c(m, manifest_path, approval_path, action):
    a = json.loads(Path(approval_path).read_text(encoding="utf-8"))
    require(a.get("approved") is True and a.get("manifest_sha256") == file_hash(manifest_path), "GATE_C_NOT_APPROVED")
    require(a.get("run_id") == m["run_id"] and action in a.get("actions", []), "GATE_C_ACTION_NOT_APPROVED")
    require(bool(a.get("approved_by")) and bool(a.get("approved_at_utc")), "APPROVAL_IDENTITY_MISSING")
    if action != "stop":
        require(m.get("gate_b_only") is False, "GATE_B_MANIFEST_NOT_RELEASED")
        require(bool(re.fullmatch(r"(?:[^\s]+@)?sha256:[a-f0-9]{64}", m.get("image") or "")), "IMAGE_NOT_BUILT_AND_PINNED")


def verify_sources(m, root):
    for name, value in m["source_sha256"].items():
        require((Path(root)/name).resolve().is_relative_to(Path(root).resolve()), "SOURCE_PATH_ESCAPE")
        require(file_hash(Path(root) / name) == value, "SOURCE_DRIFT:" + name)


def runtime_env(m, role="worker"):
    r, e = m["resources"], m["endpoints"]
    want = {"KAFKA_BOOTSTRAP_SERVERS": e["kafka"], "RIPE_ATLAS_PING_TOPIC": r["topic"],
            "RIPE_ATLAS_PING_DLQ_TOPIC": r["dlq"], "BRONZE_RIPE_ATLAS_CONSUMER_GROUP": r["group"],
            "MINIO_ENDPOINT": e["minio"], "MINIO_BUCKET": r["bucket"], "MONITORING_COUNTRIES": "DE",
            "SPARK_MASTER": "local[1]"}
    if role == "api":
        want = {"CLICKHOUSE_HOST": e["clickhouse_host"], "CLICKHOUSE_HTTP_PORT": str(e["clickhouse_port"]),
                "CLICKHOUSE_DATABASE": r["database"], "CLICKHOUSE_USER": r["ch_api_user"]}
    for key, value in want.items():
        require(os.environ.get(key) == value, "ENV_BINDING:" + key)
    needed = ("CLICKHOUSE_PASSWORD",) if role == "api" else ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "VIQ_CH_LOADER_PASSWORD")
    for key in needed:
        require(bool(os.environ.get(key)), "SECRET_MISSING:" + key)


def metadata_env(m):
    u = urlsplit(os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", ""))
    r, e = m["resources"], m["endpoints"]
    require(u.scheme == "postgresql+psycopg2" and u.hostname == e["postgres_host"] and
            u.port == e["postgres_port"] and unquote(u.path) == "/" + r["metadata_database"] and
            unquote(u.username or "") == r["metadata_user"] and bool(u.password) and not u.query,
            "METADATA_PRODUCTION_OR_UNBOUND")


def actual_context(m, stage):
    require(stage in STAGES, "UNKNOWN_STAGE")
    require(os.environ.get("VIQ_ACTUAL_DAG_ID") == DAG_ID and
            os.environ.get("VIQ_ACTUAL_RUN_ID") == m["run_id"] and
            os.environ.get("VIQ_ACTUAL_TASK_ID") == stage and
            os.environ.get("VIQ_ACTUAL_TRY_NUMBER") == "1", "ACTUAL_CONTEXT_MISMATCH")


def safe_message(text):
    # Do not emit exception text/tracebacks from third-party clients unsanitized.
    for key, value in os.environ.items():
        if any(x in key.upper() for x in ("PASSWORD", "SECRET", "TOKEN", "ACCESS_KEY", "SQL_ALCHEMY_CONN")) and value:
            text = text.replace(value, "[REDACTED]")
            if "SQL_ALCHEMY_CONN" in key.upper():
                password = urlsplit(value).password
                for part in (password, unquote(password or "")):
                    if part:
                        text = text.replace(part, "[REDACTED]")
    return text
