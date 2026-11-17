#!/usr/bin/env python
"""Explicit isolated service commands; no standalone and no startup on import."""
from pathlib import Path
import argparse
import json
import os
import sys
import re
import stat

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def seed_private_auth(passwords):
    """Seed only a per-container private tmpfs file; the secret input stays RO."""
    from processing.e2e.guards import require
    from processing.e2e.layout import reject_links
    target = Path('/tmp/viq-simple-auth/passwords.json')
    require(os.environ.get('AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE') == str(target), 'AUTH_RUNTIME_PATH')
    require(os.getuid() == 50000 and os.getgid() == 0, 'AUTH_RUNTIME_UID')
    mounts = [line.split() for line in Path('/proc/self/mountinfo').read_text().splitlines()]
    require(any(x[4] == '/tmp' and x[x.index('-')+1] == 'tmpfs' for x in mounts), 'AUTH_RUNTIME_NOT_TMPFS')
    reject_links(target)
    directory = target.parent
    if not directory.exists():
        directory.mkdir(mode=0o700)
    ds = directory.stat()
    require(ds.st_uid == os.getuid() and stat.S_IMODE(ds.st_mode) == 0o700, 'AUTH_RUNTIME_DIRECTORY_MODE')
    if not target.exists():
        fd = os.open(target, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(json.dumps(passwords, separators=(',', ':')).encode('utf-8'))
                stream.flush(); os.fsync(stream.fileno())
        except BaseException:
            # Preserve incomplete files; never overwrite/retry or include values in errors.
            raise
    reject_links(target)
    fs = target.stat()
    require(stat.S_ISREG(fs.st_mode) and fs.st_nlink == 1 and fs.st_uid == os.getuid() and
            stat.S_IMODE(fs.st_mode) == 0o600, 'AUTH_RUNTIME_FILE_MODE')
    require(json.loads(target.read_bytes()) == passwords, 'AUTH_RUNTIME_PRESEED_BINDING')


def service_log(service, root):
    from processing.e2e.guards import require
    name = os.environ.get('VIQ_E2E_RESUME_LOG_BASENAME', service+'.service.log')
    require(name == service+'.service.log' or bool(re.fullmatch(r'resume_[a-f0-9]{12}\.service\.log', name)), 'SERVICE_LOG_NAME')
    return Path(root)/name


def main():
    from processing.e2e.guards import gate_c, load_manifest, metadata_env, require, runtime_env, verify_sources
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=("init", "api", "dag-processor", "scheduler", "fastapi"))
    args = parser.parse_args()
    path = os.environ["VIQ_E2E_MANIFEST"]; m = load_manifest(path)
    gate_c(m, path, os.environ["VIQ_E2E_APPROVAL"], "start")
    verify_sources(m, "/opt/project")
    secrets = json.loads(Path("/run/secrets/runtime.json").read_bytes())
    allowed = {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__API_AUTH__JWT_SECRET",
               "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "VIQ_CH_LOADER_PASSWORD", "CLICKHOUSE_PASSWORD",
               "VIQ_AIRFLOW_ADMIN_PASSWORD"}
    control_keys = {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__API_AUTH__JWT_SECRET", "VIQ_AIRFLOW_ADMIN_PASSWORD"}
    expected_keys = ({"CLICKHOUSE_PASSWORD"} if args.service == "fastapi" else
                     control_keys if args.service in ("api", "dag-processor") else
                     control_keys | {"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "VIQ_CH_LOADER_PASSWORD"})
    require(set(secrets) == expected_keys and set(secrets) <= allowed and
            all(isinstance(v, str) and v and "REPLACE_WITH_" not in v for v in secrets.values()), "SECRET_FILE_FORMAT")
    os.environ.update(secrets)
    if args.service == "fastapi":
        runtime_env(m, "api")
        from processing.e2e.runtime import bounded_process
        bounded_process(["uvicorn", "api.e2e_isolated:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"],
                        dict(os.environ), service_log('fastapi', m["runtime_layout"]["container_service_logs"]), 3600)
        return
    metadata_env(m)
    require(len(os.environ.get("AIRFLOW__API_AUTH__JWT_SECRET", "")) >= 32, "JWT_SECRET_TOO_SHORT")
    passwords = json.loads(Path("/run/secrets/airflow_passwords.json").read_bytes())
    require(passwords == {"e2eadmin": os.environ["VIQ_AIRFLOW_ADMIN_PASSWORD"]}, "AUTH_PASSWORDS_NOT_PRESEEDED")
    require(os.environ.get("AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS") == "false", "AUTH_BYPASS_FORBIDDEN")
    seed_private_auth(passwords)
    import psycopg2
    connection = psycopg2.connect(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("postgresql+psycopg2://", "postgresql://", 1))
    try:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user")
            require(cursor.fetchone() == (m["resources"]["metadata_database"], m["resources"]["metadata_user"]), "ACTUAL_METADATA_DATABASE")
            cursor.execute("SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            require(not any(cursor.fetchone()), "METADATA_ROLE_TOO_PRIVILEGED")
            cursor.execute("SELECT pg_has_role(current_user, 'airflow', 'member')")
            require(cursor.fetchone() == (False,), "METADATA_ROLE_INHERITS_PRODUCTION")
    finally:
        connection.close()
    if args.service == "init":
        from processing.e2e.io import Evidence, write_new
        from processing.e2e.runtime import bounded_process
        from processing.e2e.kafka import broker_state
        from processing.e2e.clickhouse import ClickHouse
        from processing.e2e.contracts import utc_now
        runtime_env(m); e = Evidence(m, path)
        gate_c(m, path, os.environ["VIQ_E2E_APPROVAL"], "provision")
        from processing.e2e.attestation import verify_attestation
        verified = verify_attestation(m, path, m["runtime_layout"]["container_control"])
        state = broker_state(m); r = m["resources"]
        require(all(state[n] == {"low": 0, "high": 0} for n in (r["topic"], r["dlq"])) and
                state["committed"] < 0 and not e.keys(), "PROVISION_NOT_EMPTY")
        require(e.s3.get_bucket_versioning(Bucket=e.bucket).get("Status") == "Enabled", "PROVISION_VERSIONING")
        ClickHouse(m).empty()
        root = Path(m["runtime_layout"]["container_bootstrap"])
        bounded_process(["airflow", "db", "migrate"], dict(os.environ), root / "metadata_migrate.log", 120)
        bounded_process(["airflow", "pools", "set", "viq_e2e_once", "1", "Single isolated E2E task"],
                        dict(os.environ), root / "pool_init.log", 60)
        write_new(root / "provision.json", {"manifest_sha256": e.manifest_hash, "resources": r,
                  **verified,
                  "empty_resources_verified": True, "verified_at": utc_now(), "broker_state": state})
        return
    commands = {"api": ["airflow", "api-server", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"],
                "dag-processor": ["airflow", "dag-processor"], "scheduler": ["airflow", "scheduler"]}
    from processing.e2e.runtime import bounded_process
    bounded_process(commands[args.service], dict(os.environ),
                    service_log(args.service, m["runtime_layout"]["container_service_logs"]), 3600)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("E2E bootstrap refused: " + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
