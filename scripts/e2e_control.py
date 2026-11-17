#!/usr/bin/env python
"""Offline prepare by default; start/run/stop require a separately approved manifest."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import copy
import re
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from processing.e2e.contracts import ARCHIVE_SHA256, DAG_ID, RAW_SHA256, STAGES, canonical, utc_now
from processing.e2e.guards import ENDPOINTS, file_hash, gate_c, load_manifest, namespace, require, verify_sources
from processing.e2e.io import write_new
from processing.e2e.layout import host_paths, layout_for, validate_mounts, reject_links

COMPOSE = ROOT / "infra/airflow/docker-compose.viq-e2e-isolated.yml"
# Synthetic unit-test identity only; the CLI requires a fresh explicit run ID.
DEFAULT_RUN = "manual__viq_e2e_19700101T000000Z_000000000000"
SERVICES = ("e2e-airflow-init", "e2e-airflow-api", "e2e-airflow-dag-processor", "e2e-airflow-scheduler", "e2e-api")


def make_manifest(run_id):
    r = namespace(run_id)
    files = []
    for folder in ("processing", "messaging", "ingestion", "storage", "configs", "config", "api", "scripts", "infra/airflow/e2e_dags"):
        files.extend(p for p in (ROOT/folder).rglob("*.py") if "__pycache__" not in p.parts)
    deployment = [COMPOSE, ROOT/"infra/airflow/Dockerfile.e2e", ROOT/"requirements-e2e.txt",
                  ROOT/"infra/clickhouse/e2e_schema.sql", ROOT/"infra/airflow/Dockerfile.e2e.dockerignore"] + list((ROOT/"scripts").glob("*viq_e2e*.ps1"))
    deployment.append(ROOT/".gitignore")
    base = str(Path(os.environ.get("VIQ_E2E_INPUT_DIR") or ROOT/"data/private/e2e").expanduser().resolve()) + "/"
    return {"schema_version": 1, "dag_id": DAG_ID, "run_id": run_id, "resources": r,
        "input_scope": {"country": "DE", "measurement": 177401244, "start": "2026-10-18T13:00:00Z",
                        "stop": "2026-10-18T14:00:00Z", "records": 375, "probes": 24, "windows": 12},
        "raw_sha256": RAW_SHA256, "archive_sha256": ARCHIVE_SHA256,
        "host_raw": base+"FILTERED_177401244_1792328400_1792332000_5be6185236b7c6a8.jsonl",
        "host_archive": base+"PROBES_SNAPSHOT_20261018.json.bz2", "endpoints": ENDPOINTS,
        "container_evidence": "/opt/e2e/evidence", "host_evidence": "output/e2e/"+r["rid"],
        "runtime_layout": layout_for(r["rid"]), "provision_attestation": {"mode": "creation_v1"},
        "minimum_vm_bytes": int(11.5*1024**3), "minimum_available_bytes": 3*1024**3,
        "image": None, "gate_b_only": True,
        "source_sha256": {p.relative_to(ROOT).as_posix(): file_hash(p) for p in sorted(set(files))},
        "deployment_sha256": {p.relative_to(ROOT).as_posix(): file_hash(p) for p in deployment},
        "acceptance": "E2E_FULL_HOUR_SANDBOX_TO_API_PASS", "live_ingestion": False, "dashboard_accepted": False}


def render_env(m, manifest_path, approval):
    r = m["resources"]; paths = host_paths(m, ROOT)
    values = {"VIQ_E2E_ROOT": str(ROOT), "VIQ_E2E_PROJECT": r["project"],
        "VIQ_E2E_TOPIC": r["topic"], "VIQ_E2E_DLQ": r["dlq"], "VIQ_E2E_GROUP": r["group"],
        "VIQ_E2E_BUCKET": r["bucket"], "VIQ_E2E_DATABASE": r["database"], "VIQ_E2E_CH_API_USER": r["ch_api_user"],
        "VIQ_E2E_IMAGE": m["image"] or "viq-e2e:gate-b-unbuilt",
        "VIQ_E2E_MANIFEST_FILE": str(Path(manifest_path).resolve()),
        "VIQ_E2E_APPROVAL_FILE": str(Path(approval).resolve()),
        "VIQ_E2E_RAW_FILE": m["host_raw"], "VIQ_E2E_ARCHIVE_FILE": m["host_archive"],
        "VIQ_E2E_CONTROL_DIR": str(paths["control"]), "VIQ_E2E_RUNTIME_DIR": str(paths["runtime"]),
        "VIQ_E2E_SECRETS_DIR": str(paths["private"])}
    return "\n".join(key+"="+value.replace("\\", "/") for key, value in values.items())+"\n"


def prepare(manifest_path, run_id):
    from processing.e2e.clickhouse import render_schema
    m = make_manifest(run_id)
    require(file_hash(m["host_raw"]) == RAW_SHA256 and file_hash(m["host_archive"]) == ARCHIVE_SHA256, "LOCAL_INPUT_PINS")
    write_new(manifest_path, m)
    root = ROOT/m["host_evidence"]; root.mkdir(parents=True, exist_ok=False)
    control = host_paths(m, ROOT)["control"]; control.mkdir(parents=True, exist_ok=False)
    approval = control/"approval.json"
    write_new(root/"approval.example.json", {"approved": False, "run_id": run_id,
        "manifest_sha256": file_hash(manifest_path), "actions": ["provision", "start", "run", "stop"],
        "approved_by": "", "approved_at_utc": ""})
    (control/"compose.env").write_text(render_env(m, manifest_path, approval), encoding="utf-8")
    (root/"schema.rendered.sql").write_text(render_schema(m, (ROOT/"infra/clickhouse/e2e_schema.sql").read_text()), encoding="utf-8")
    print(json.dumps({"status": "PREPARED_OFFLINE_ONLY", "manifest": str(manifest_path), "rid": m["resources"]["rid"],
                      "image_built": False, "gate_c_ready": False}))


def docker(argv):
    result = subprocess.run(["docker", *argv], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=40)
    require(result.returncode == 0, "DOCKER_COMMAND_FAILED")
    return result.stdout


def compose(m, argv):
    return docker(["compose", "--env-file", str(host_paths(m, ROOT)["control"]/"compose.env"),
                   "-f", str(COMPOSE), "--profile", "e2e-manual", *argv])


def verify_deployment(m):
    verify_sources(m, ROOT)
    for name, value in m["deployment_sha256"].items():
        require(file_hash(ROOT/name) == value, "DEPLOYMENT_DRIFT")
    require(m.get("gate_b_only") is False, "GATE_B_MANIFEST_NOT_RELEASED")


def container_name(m, service):
    suffix = service.replace("e2e-", "", 1)
    return m["resources"]["project"]+"-"+suffix


def inspect_e2e(m, service):
    c = json.loads(docker(["inspect", container_name(m, service)]))[0]
    labels = c["Config"].get("Labels", {})
    require(labels.get("com.docker.compose.project") == m["resources"]["project"] and
            labels.get("com.docker.compose.service") == service and
            c["Config"]["Image"] == m["image"], "CONTAINER_IDENTITY_MISMATCH")
    return c


def af_client(m):
    import requests
    passwords = json.loads((host_paths(m, ROOT)["private"]/"airflow_passwords.json").read_bytes())
    session = requests.Session(); session.trust_env = False
    response = session.post(m["endpoints"]["airflow_host"]+"/auth/token",
                            json={"username": "e2eadmin", "password": passwords["e2eadmin"]}, timeout=10)
    require(response.status_code == 201 or response.status_code == 200, "AIRFLOW_AUTH_FAILED")
    session.headers["Authorization"] = "Bearer "+response.json()["access_token"]
    return session


def af(m, session, method, path, body=None):
    response = session.request(method, m["endpoints"]["airflow_host"]+"/api/v2"+path, json=body, timeout=15)
    if response.status_code == 401 and method == "GET":
        fresh = af_client(m); session.headers["Authorization"] = fresh.headers["Authorization"]
        response = session.get(m["endpoints"]["airflow_host"]+"/api/v2"+path, timeout=15)
    require(response.status_code in (200, 201), "AIRFLOW_API_REQUEST_FAILED")
    return response.json()


def runtime_precheck(m):
    memory = int(docker(["info", "--format", "{{.MemTotal}}|{{.NCPU}}"]).split('|')[0])
    require(memory >= m["minimum_vm_bytes"], "DOCKER_RAM_NO_GO")
    for name in ("internet-quality-airflow", "internet-quality-bronze-consumer"):
        old = json.loads(docker(["inspect", name]))[0]
        require(old["State"]["Status"] == "exited", "PRODUCTION_COMPUTE_NOT_STOPPED")


def start(m):
    runtime_precheck(m)
    paths = host_paths(m, ROOT)
    require(not any((ROOT/m["host_evidence"]/x).exists() for x in
                    ("startup_attempt.json", "started.json", "triggered.json", "FINAL_E2E_RESULT.json")), "LEGACY_RUNTIME_ALREADY_ATTEMPTED")
    config = json.loads(compose(m, ["config", "--format", "json"]))
    validate_mounts(m, ROOT, config, ROOT/"docs/e2e/viq_e2e_run_manifest.json", paths["control"]/"approval.json")
    from processing.e2e.attestation import verify_attestation
    verify_attestation(load_manifest(ROOT/"docs/e2e/viq_e2e_run_manifest.json"),
                       ROOT/"docs/e2e/viq_e2e_run_manifest.json", paths["control"])
    # Only fresh runtime output directories are created; never control/private payloads.
    require(not (paths["runtime"]/"controller/startup_attempt.json").exists(), "START_ALREADY_ATTEMPTED")
    for folder in ("controller", "bootstrap", "pipeline", "services/api", "services/dag-processor", "services/scheduler", "services/fastapi"):
        directory = paths["runtime"]/folder; reject_links(directory)
        directory.mkdir(parents=True, exist_ok=True)
    active = set(docker(["ps", "-a", "--format", "{{.Names}}"]).splitlines())
    require(not any(container_name(m, s) in active for s in SERVICES), "CONTAINER_NAMES_ALREADY_EXIST")
    write_new(host_paths(m, ROOT)["runtime"]/"controller/startup_attempt.json", {"run_id": m["run_id"], "at": utc_now()})
    compose(m, ["up", "--no-build", "--no-recreate", "--pull", "never", "-d", "e2e-airflow-init"])
    deadline = time.monotonic()+180
    while True:
        c = inspect_e2e(m, "e2e-airflow-init")
        if c["State"]["Status"] == "exited":
            require(c["State"]["ExitCode"] == 0, "ISOLATED_INIT_FAILED"); break
        require(time.monotonic() < deadline, "INIT_TIMEOUT"); time.sleep(2)
    provision = json.loads((paths["runtime"]/"bootstrap/provision.json").read_bytes())
    require(provision.get("manifest_sha256") == m["_manifest_sha256"] and
            provision.get("creation_workflow_verified") is True and provision.get("resources_created_in_this_init") is False,
            "BOOTSTRAP_RECEIPT_NOT_VERIFIED")
    compose(m, ["up", "--no-build", "--no-recreate", "--pull", "never", "-d",
                "e2e-airflow-api", "e2e-airflow-dag-processor", "e2e-api"])
    deadline = time.monotonic()+120
    while True:
        try:
            session = af_client(m)
            dags = af(m, session, "GET", "/dags?limit=100")["dags"]
            require(len(dags) == 1 and dags[0]["dag_id"] == DAG_ID and dags[0]["is_paused"], "DAG_SET_NOT_ISOLATED_PAUSED")
            require(af(m, session, "GET", f"/dags/{DAG_ID}/dagRuns?limit=100")["total_entries"] == 0, "EXISTING_DAGRUN")
            break
        except Exception:
            require(time.monotonic() < deadline, "API_OR_DAG_SERIALIZATION_NOT_READY"); time.sleep(2)
    compose(m, ["up", "--no-build", "--no-recreate", "--pull", "never", "-d", "e2e-airflow-scheduler"])
    write_new(host_paths(m, ROOT)["runtime"]/"controller/started.json", {"run_id": m["run_id"], "at": utc_now(), "triggered": False})


def stop(m):
    stopped = []
    existing = set(docker(["ps", "-a", "--format", "{{.Names}}"]).splitlines())
    for service in reversed(SERVICES):
        if container_name(m, service) in existing:
            c = inspect_e2e(m, service)
            docker(["stop", "--time", "20", c["Id"]])
            stopped.append({"id": c["Id"], "service": service})
    state = host_paths(m, ROOT)["runtime"]/"controller"
    reject_links(state); state.mkdir(parents=True, exist_ok=True)
    write_new(state/("stopped_"+str(time.time_ns())+".json"),
              {"run_id": m["run_id"], "at": utc_now(), "containers": stopped, "data_cleanup": False})


def terminal_audit(m, root, run, instances):
    from processing.e2e.io import Evidence
    require(run["dag_run_id"] == m["run_id"] and run["state"] == "success" and bool(run.get("end_date")), "DAGRUN_NOT_SUCCESS_TERMINAL")
    require(len(instances) == len(STAGES) and {t["task_id"] for t in instances} == set(STAGES), "TASK_SET_MISMATCH")
    by_name = {t["task_id"]: t for t in instances}
    previous_end = None
    def dt(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(all(t["state"] == "success" and t["try_number"] == 1 and t.get("start_date") and t.get("end_date")
                and t.get("dag_id", DAG_ID) == DAG_ID and t.get("dag_run_id", m["run_id"]) == m["run_id"]
                for t in instances), "TASK_STATE_OR_ATTEMPT")
    previous_hash = None
    for stage in STAGES:
        path = root/"receipts"/(stage+".json"); receipt = json.loads(path.read_bytes()); task = by_name[stage]
        require(receipt["status"] == "success" and receipt["run_id"] == m["run_id"] and receipt["dag_id"] == DAG_ID and
                receipt["stage"] == stage and receipt["manifest_sha256"] == m["_manifest_sha256"] and
                receipt["previous_receipt_sha256"] == previous_hash, "TERMINAL_RECEIPT_BINDING")
        require(dt(task["start_date"]) <= dt(receipt["started_at"]) <= dt(receipt["ended_at"]) <= dt(task["end_date"]), "RECEIPT_OUTSIDE_TASK")
        require(previous_end is None or previous_end <= dt(task["start_date"]), "TASK_DEPENDENCY_TIME")
        previous_end = dt(task["end_date"]); previous_hash = file_hash(path)
    data = receipt["output"]
    require(data["status"] == "DATA_CHECKS_PASSED", "DATA_NOT_ACCEPTED")
    return {"status": "E2E_FULL_HOUR_SANDBOX_TO_API_PASS", "run_id": m["run_id"],
            "manifest_sha256": m["_manifest_sha256"], "dagrun": run, "task_instances": instances,
            "last_receipt_sha256": previous_hash, "production_e2e_accepted": False,
            "dashboard_accepted": False, "live_ingestion": False, "verified_at": utc_now()}


def fresh_checkpoints(m):
    """Published hashes of actual initial migration/receipt, never H2 aliases."""
    paths = host_paths(m, ROOT)
    approval = json.loads((paths['control']/'approval.json').read_bytes())
    gate_c(load_manifest(ROOT/'docs/e2e/viq_e2e_run_manifest.json'),
           ROOT/'docs/e2e/viq_e2e_run_manifest.json', paths['control']/'approval.json', 'stop')
    expected = approval.get('fresh_checkpoint_sha256', {})
    require(set(expected) == {'bootstrap/provision.json','bootstrap/metadata_migrate.log','bootstrap/pool_init.log',
            'controller/fresh_initialized.json'} and all(isinstance(v,str) and re.fullmatch('[a-f0-9]{64}',v)
            for v in expected.values()), 'FRESH_CHECKPOINT_SCHEMA')
    for name,digest in expected.items():
        path = paths['runtime']/name;reject_links(path)
        require(file_hash(path) == digest, 'FRESH_CHECKPOINT_DRIFT')
    initialized = json.loads((paths['runtime']/'controller/fresh_initialized.json').read_bytes())
    require(initialized.get('manifest_sha256') == m['_manifest_sha256'] and initialized.get('run_id') == m['run_id'] and
            initialized.get('metadata_database') == m['resources']['metadata_database'] and
            initialized.get('migration_invocations') == 1 and initialized.get('pool_set_invocations') == 1,
            'FRESH_INITIALIZATION_BINDING')
    return expected


def resume_checkpoints(m):
    return fresh_checkpoints(m) if m['provision_attestation']['mode'] == 'creation_v1' else H2_RECEIPTS


def fresh_resume_authority(m, resume_id, approval_path):
    paths = host_paths(m, ROOT);resume_identity(m,resume_id)
    require(resume_id not in ('c0b000000001','c0d82debd95f') and m['run_id'] != DEFAULT_RUN, 'FRESH_OLD_ID')
    expected = paths['control']/('resume_'+resume_id+'.json')
    require(approval_path is not None and Path(approval_path).resolve() == expected.resolve(), 'RESUME_APPROVAL_PATH')
    reject_links(expected);a = json.loads(expected.read_bytes())
    require(a.get('schema_version') == 1 and a.get('approved') is True and a.get('review_only') is False and
            a.get('approval_type') == 'CD_FRESH_GENERATION' and bool(a.get('approved_by')) and bool(a.get('approved_at_utc')),
            'FRESH_RESUME_NOT_APPROVED')
    require(a.get('manifest_sha256') == m['_manifest_sha256'] and a.get('run_id') == m['run_id'] and
            a.get('resume_id') == resume_id and a.get('actions') == ['start','stop'] and
            a.get('migration_allowed') is False and a.get('provisioning_allowed') is False and
            a.get('max_dagruns') == 1 and a.get('checkpoint_sha256') == fresh_checkpoints(m), 'FRESH_RESUME_BINDING')
    approval = json.loads((paths['control']/'approval.json').read_bytes())
    require(approval.get('runtime_scope',{}).get('resume_id') == resume_id and
            a.get('owner_authorization_request_sha256') == approval.get('owner_authorization_request_sha256'),
            'FRESH_RESUME_OWNER_SCOPE')
    return a


def fresh_metadata_readonly(m, require_dag=True):
    sql = """BEGIN READ ONLY;
SELECT json_build_object('database',current_database(),
 'owner',(SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()),
 'revision',(SELECT version_num FROM alembic_version),
 'dagruns',(SELECT count(*) FROM dag_run),'tasks',(SELECT count(*) FROM task_instance),
 'pools',(SELECT coalesce(json_agg(json_build_object('name',pool,'slots',slots)),'[]'::json) FROM slot_pool WHERE pool='viq_e2e_once'),
 'dags',(SELECT coalesce(json_agg(json_build_object('dag_id',dag_id,'is_paused',is_paused)),'[]'::json) FROM dag));
ROLLBACK;"""
    d = json.loads(docker(['exec','internet-quality-airflow-postgres','psql','-X','-q','-A','-t',
        '-v','ON_ERROR_STOP=1','-U','airflow','-d',m['resources']['metadata_database'],'-c',sql]))
    dags = d.get('dags');require(dags == [{'dag_id':DAG_ID,'is_paused':True}] or
            (not require_dag and dags == []), 'FRESH_METADATA_DAG')
    require({k:v for k,v in d.items() if k != 'dags'} == {'database':m['resources']['metadata_database'],
        'owner':m['resources']['metadata_user'],'revision':'d2f4e1b3c5a7','dagruns':0,'tasks':0,
        'pools':[{'name':'viq_e2e_once','slots':1}]}, 'FRESH_METADATA_DRIFT')
    return d


def fresh_resume_preflight(m, resume_id, auth):
    runtime_precheck(m);verify_deployment(m)
    paths = host_paths(m,ROOT);mp = ROOT/'docs/e2e/viq_e2e_run_manifest.json'
    from processing.e2e.attestation import verify_attestation
    from processing.e2e.stages import verify_bootstrap_lineage
    clean = load_manifest(mp);verify_attestation(clean,mp,paths['control'])
    verify_bootstrap_lineage(clean,mp,paths['control']/'approval.json',paths['runtime']/'bootstrap/provision.json')
    require(auth == fresh_resume_authority(m,resume_id,paths['control']/('resume_'+resume_id+'.json')),
            'FRESH_AUTHORITY_OBJECT')
    state = paths['runtime']/'controller';reject_links(state,recursive=True)
    require(not any((state/name).exists() for name in ('started.json','triggered.json','FINAL_E2E_RESULT.json')) and
            not (state/'resumes'/resume_id).exists(), 'FRESH_PRIOR_RUN_OR_RESUME')
    pipeline = paths['runtime']/'pipeline';reject_links(pipeline,recursive=True)
    require({p.name for p in pipeline.iterdir()} == {'provision.json'} and
            (pipeline/'provision.json').stat().st_size == 0, 'FRESH_PIPELINE_NOT_EMPTY')
    _,names = resume_identity(m,resume_id)
    existing = set(docker(['ps','-a','--format','{{.Names}}']).splitlines())
    require(not set(names.values()) & existing, 'FRESH_CONTAINER_COLLISION')
    require(file_hash(m['host_raw']) == m['raw_sha256'] and file_hash(m['host_archive']) == m['archive_sha256'],
            'FRESH_INPUT_DRIFT')
    return fresh_metadata_readonly(m,require_dag=False)


def fresh_initialize(m):
    """One actual init only in a genuinely new, attested generation/metadata DB."""
    paths = host_paths(m,ROOT);mp = ROOT/'docs/e2e/viq_e2e_run_manifest.json';clean = load_manifest(mp)
    require(m == dict(clean,_manifest_sha256=file_hash(mp)) and m['provision_attestation']['mode'] == 'creation_v1' and
            m['run_id'] != DEFAULT_RUN, 'FRESH_INIT_MANIFEST')
    for action in ('provision','start','stop'):
        gate_c(clean,mp,paths['control']/'approval.json',action)
    approval = json.loads((paths['control']/'approval.json').read_bytes());scope = approval.get('runtime_scope',{})
    require(scope.get('fresh_generation') is True and scope.get('initial_metadata_migration_allowed') is True and
            scope.get('max_dagruns') == 1 and scope.get('metadata_database') == m['resources']['metadata_database'],
            'FRESH_INIT_SCOPE')
    from processing.e2e.attestation import verify_attestation
    verified = verify_attestation(clean,mp,paths['control']);runtime_precheck(m);verify_deployment(m)
    creation = json.loads((paths['control']/'admin_provision.json').read_bytes())
    require(creation.get('owner_authorization_request_sha256') == approval.get('owner_authorization_request_sha256') and
            bool(creation.get('owner_authorization_request_sha256')), 'FRESH_INIT_CREATION_OWNER')
    state = paths['runtime']/'controller';reject_links(paths['runtime'],recursive=True)
    require(not any((state/name).exists() for name in ('fresh_init.attempt.json','fresh_initialized.json','triggered.json','started.json')) and
            not (paths['runtime']/'bootstrap/provision.json').exists(), 'FRESH_INIT_ALREADY_ATTEMPTED')
    for folder in ('controller','bootstrap','pipeline','services/api','services/dag-processor','services/scheduler','services/fastapi'):
        (paths['runtime']/folder).mkdir(parents=True,exist_ok=True)
    pipeline = paths['runtime']/'pipeline'
    require(not list(pipeline.iterdir()), 'FRESH_INIT_PIPELINE_NOT_EMPTY')
    with (pipeline/'provision.json').open('xb'):
        pass  # Empty mountpoint only; actual receipt is created solely by init.
    base = json.loads(compose(m,['config','--format','json']))
    validate_mounts(clean,ROOT,base,mp,paths['control']/'approval.json')
    existing = set(docker(['ps','-a','--format','{{.Names}}']).splitlines())
    require(not any(container_name(m,s) in existing for s in SERVICES), 'FRESH_INIT_CONTAINER_COLLISION')
    volumes = set(docker(['volume','ls','--format','{{.Name}}']).splitlines())
    networks = set(docker(['network','ls','--format','{{.Name}}']).splitlines())
    require(not {v['name'] for v in base['volumes'].values()} & volumes and
            base['networks']['e2e']['name'] not in networks, 'FRESH_COMPUTE_RESOURCE_COLLISION')
    write_new(state/'fresh_init.attempt.json',{'run_id':m['run_id'],'manifest_sha256':file_hash(mp),'at':utc_now(),
              'creation_attestation_sha256':verified['admin_attestation_sha256'],'metadata_database':m['resources']['metadata_database']})
    # Creation is bounded to five names and one network already validated by
    # the unchanged layout guard; no if-exists/adopt, driver options or aliases.
    for key,value in base['volumes'].items():
        docker(['volume','create','--driver','local','--label','com.docker.compose.project='+m['resources']['project'],
                '--label','com.docker.compose.volume='+key,value['name']])
    docker(['network','create','--driver','bridge','--label','com.docker.compose.project='+m['resources']['project'],
            '--label','com.docker.compose.network=e2e',base['networks']['e2e']['name']])
    try:
        compose(m,['up','--no-build','--no-recreate','--pull','never','-d','e2e-airflow-init'])
        deadline = time.monotonic()+180
        while True:
            c = inspect_e2e(m,'e2e-airflow-init')
            if c['State']['Status'] == 'exited':
                require(c['State']['ExitCode'] == 0 and not c['State']['OOMKilled'], 'FRESH_INIT_FAILED');break
            require(time.monotonic() < deadline, 'FRESH_INIT_TIMEOUT');time.sleep(2)
        provision = json.loads((paths['runtime']/'bootstrap/provision.json').read_bytes())
        require(provision.get('manifest_sha256') == file_hash(mp) and provision.get('resources') == m['resources'] and
                all(provision.get(k) == v for k,v in verified.items()), 'FRESH_ACTUAL_BOOTSTRAP_RECEIPT')
        metadata = fresh_metadata_readonly(m,require_dag=False)
        write_new(state/'fresh_initialized.json',{'run_id':m['run_id'],'manifest_sha256':file_hash(mp),'at':utc_now(),
            'metadata_database':m['resources']['metadata_database'],'migration_invocations':1,'pool_set_invocations':1,
            'init_container_id':c['Id'],'metadata_readonly':metadata,'creation_attestation_sha256':verified['admin_attestation_sha256']})
        records = {}
        import base64
        for name in ('admin_provision.json','admin/kafka.json','admin/minio.json','admin/postgres.json','admin/clickhouse.json'):
            records[name] = base64.b64encode((paths['control']/name).read_bytes()).decode('ascii')
        approval['fresh_creation'] = {'schema_version':1,'manifest_sha256':file_hash(mp),
            'attestation_sha256':verified['admin_attestation_sha256'],
            'bootstrap_sha256':file_hash(paths['runtime']/'bootstrap/provision.json'),'records':records}
        approval['fresh_checkpoint_sha256'] = {name:file_hash(paths['runtime']/name) for name in
            ('bootstrap/provision.json','bootstrap/metadata_migrate.log','bootstrap/pool_init.log','controller/fresh_initialized.json')}
        (paths['control']/'approval.json').write_bytes(canonical(approval))
        from processing.e2e.stages import verify_bootstrap_lineage
        verify_bootstrap_lineage(clean,mp,paths['control']/'approval.json',paths['runtime']/'bootstrap/provision.json')
    except BaseException as exc:
        write_new(state/'fresh_init.failure.json',{'at':utc_now(),'run_id':m['run_id'],'error_type':type(exc).__name__,
                  'retry_allowed':False,'data_cleanup':False})
        # The only possible running container before successful init is this
        # exact original-project init. Existing stop() validates scoped IDs.
        stop(m)
        raise


def resume_run_context(m, resume_id, approval_path):
    """Bind future Run to a fresh authorized resume and immutable ready IDs."""
    mp = ROOT/'docs/e2e/viq_e2e_run_manifest.json'
    clean = load_manifest(mp)
    paths = host_paths(clean, ROOT)
    require(m == dict(clean, _manifest_sha256=file_hash(mp)), 'RUN_MANIFEST_OBJECT_DRIFT')
    gate_c(clean, mp, paths['control']/'approval.json', 'run')
    gate_c(clean, mp, paths['control']/'approval.json', 'stop')
    authority = resume_authority(m, resume_id, approval_path)
    require(authority['approval_type'] in ('CD_ISOLATED_RESUME','CD_FRESH_GENERATION') and resume_id != 'c0b000000001', 'RUN_REQUIRES_NEW_CD_RESUME')
    run_approval = json.loads((paths['control']/'approval.json').read_bytes())
    scope = run_approval.get('runtime_scope', {})
    require(scope.get('resume_id') == resume_id and scope.get('max_dagruns') == 1 and
            scope.get('migration_allowed') is False and scope.get('provisioning_allowed') is False,
            'RUN_RESUME_SCOPE_NOT_APPROVED')
    project, names = resume_identity(m, resume_id)
    root = paths['runtime']/'controller/resumes'/resume_id
    reject_links(root, recursive=True)
    require(root.is_dir() and not any((root/name).exists() for name in
            ('stopped.json','failure.json','triggered.json','FINAL_E2E_FAILURE.json','FINAL_E2E_RESULT.json')) and
            not (paths['runtime']/'controller/triggered.json').exists(), 'RESUME_CONSUMED_OR_RUN_RESERVED')
    attempt = json.loads((root/'attempt.json').read_bytes())
    ready = json.loads((root/'ready.json').read_bytes())
    require(attempt.get('manifest_sha256') == m['_manifest_sha256'] and attempt.get('run_id') == m['run_id'] and
            attempt.get('project') == project and attempt.get('container_names') == names and
            attempt.get('checkpoint_sha256') == resume_checkpoints(m) and attempt.get('migration_executed') is False and
            attempt.get('resume_approval_sha256') == file_hash(approval_path), 'RESUME_RUN_ATTEMPT_BINDING')
    require(ready.get('status') == 'RESUMED_RUNTIME_READY_REQUIRES_PRETRIGGER_CHECKS' and
            ready.get('manifest_sha256') == m['_manifest_sha256'] and ready.get('run_id') == m['run_id'] and
            ready.get('project') == project and ready.get('resume_id') == resume_id and
            ready.get('attempt_sha256') == file_hash(root/'attempt.json') and
            ready.get('compose_sha256') == attempt['compose_sha256'] and
            ready.get('migration_executed') is False and ready.get('dagrun_triggered') is False and
            ready.get('bootstrap_receipt_sha256',ready.get('h2_bootstrap_receipt_sha256')) == resume_checkpoints(m)['bootstrap/provision.json'], 'RESUME_RUN_READY_BINDING')
    require(file_hash(root/'compose.runtime.json') == attempt['compose_sha256'], 'RESUME_RUN_CONFIG_BYTES')
    base = json.loads(compose(m, ['config','--format','json']))
    validate_mounts(clean, ROOT, base, mp, paths['control']/'approval.json')
    require(json.loads((root/'compose.runtime.json').read_bytes()) == resume_execution_config(m, base, resume_id),
            'RESUME_RUN_CONFIG_DRIFT')
    found = resume_containers(m, resume_id, project)
    require(set(found) == set(SERVICES[1:]) and
            ready.get('container_ids') == {s:c['Id'] for s,c in found.items()}, 'RESUME_RUN_CONTAINER_IDS')
    require(all(c['State']['Status'] == 'running' and not c['State']['OOMKilled'] for c in found.values()),
            'RESUME_RUN_NOT_RUNNING')
    return root, project, names, ready['container_ids']


def run_once(m, resume_id=None, approval_path=None):
    root, project, names, ids = resume_run_context(m, resume_id, approval_path)
    paths = host_paths(m, ROOT)
    def running():
        found = resume_containers(m, resume_id, project)
        require(set(found) == set(ids) and {s:c['Id'] for s,c in found.items()} == ids,
                'RUN_CONTAINER_ID_DRIFT')
        require(all(c['State']['Status'] == 'running' and not c['State']['OOMKilled'] for c in found.values()),
                'CONTROL_SERVICE_OR_OOM_ABORT')
        return found
    try:
        runtime_precheck(m)
        verify_deployment(m)
        from processing.e2e.stages import verify_bootstrap_lineage
        mp = ROOT/'docs/e2e/viq_e2e_run_manifest.json'
        verify_bootstrap_lineage(load_manifest(mp), mp, paths['control']/'approval.json', paths['runtime']/'bootstrap/provision.json')
        for name, expected in resume_checkpoints(m).items():
            require(file_hash(paths['runtime']/name) == expected, 'RUN_H2_CHECKPOINT_DRIFT')
        resume_metadata_readonly(m)
        session = af_client(m)
        path = f'/dags/{DAG_ID}/dagRuns'
        worker = running()['e2e-airflow-scheduler']
        memory = docker(['exec', worker['Id'], 'cat', '/proc/meminfo'])
        available = int(next(x for x in memory.splitlines() if x.startswith('MemAvailable:')).split()[1])*1024
        require(available >= m['minimum_available_bytes'], 'MEMAVAILABLE_BEFORE_TRIGGER_NO_GO')
        pids = list(map(int, docker(['exec', worker['Id'], 'cat', '/sys/fs/cgroup/pids.current', '/sys/fs/cgroup/pids.max']).split()))
        require(len(pids) == 2 and 0 < pids[1] and pids[0]*4 <= pids[1]*3, 'PID_HEADROOM_NO_GO')
        for service, c in running().items():
            events = docker(['exec', c['Id'], 'cat', '/sys/fs/cgroup/memory.events'])
            counters = {x.split()[0]:int(x.split()[1]) for x in events.splitlines()}
            require(counters.get('oom') == 0 and counters.get('oom_kill') == 0, 'PRETRIGGER_CGROUP_OOM')
        require(af(m, session, 'GET', '/monitor/health')['scheduler']['status'] == 'healthy', 'SCHEDULER_HEARTBEAT_NO_GO')
        require(af(m, session, 'GET', path+'?limit=100')['total_entries'] == 0, 'DAGRUN_ALREADY_EXISTS')
        # Both reservations precede PATCH/POST. A partial reservation also blocks
        # a second run/resume: ambiguity must never cause another trigger.
        reservation = {'run_id':m['run_id'], 'manifest_sha256':m['_manifest_sha256'], 'resume_id':resume_id,
                       'project':project, 'ready_sha256':file_hash(root/'ready.json'), 'container_ids':ids,
                       'at':utc_now(), 'status':'TRIGGER_ATTEMPT_RESERVED'}
        write_new(paths['runtime']/'controller/triggered.json', reservation)
        write_new(root/'triggered.json', reservation)
        af(m, session, 'PATCH', f'/dags/{DAG_ID}', {'is_paused':False})
        af(m, session, 'POST', path, {'dag_run_id':m['run_id'], 'logical_date':None,
                                   'conf':{'manifest_sha256':m['_manifest_sha256']}})
        deadline = time.monotonic()+45*60
        pressure = 0
        while True:
            run = af(m, session, 'GET', path+'/'+quote(m['run_id'], safe=''))
            found = running()
            stats = docker(['stats','--no-stream','--format','{{json .}}', *ids.values()])
            c = found['e2e-airflow-scheduler']
            meminfo = docker(['exec', c['Id'], 'cat', '/proc/meminfo'])
            available = int(next(x for x in meminfo.splitlines() if x.startswith('MemAvailable:')).split()[1])*1024
            samples = [json.loads(line) for line in stats.splitlines() if line.strip()]
            require(len(samples) == len(ids), 'RUN_RESOURCE_SAMPLE_SET')
            hot = any(float(x['MemPerc'].rstrip('%')) >= 90 for x in samples)
            pids = list(map(int, docker(['exec', c['Id'], 'cat', '/sys/fs/cgroup/pids.current', '/sys/fs/cgroup/pids.max']).split()))
            require(len(pids) == 2 and pids[0] < pids[1], 'PID_LIMIT_ABORT')
            for service, current in found.items():
                events = docker(['exec', current['Id'], 'cat', '/sys/fs/cgroup/memory.events'])
                counters = {x.split()[0]:int(x.split()[1]) for x in events.splitlines()}
                require(counters.get('oom') == 0 and counters.get('oom_kill') == 0, 'CGROUP_OOM_ABORT')
            require(af(m, session, 'GET', '/monitor/health')['scheduler']['status'] == 'healthy', 'SCHEDULER_HEARTBEAT_ABORT')
            pressure = pressure+1 if hot or available < 1024**3 else 0
            with (root/'resource_samples.jsonl').open('ab') as stream:
                stream.write(canonical({'at':utc_now(), 'available_bytes':available, 'pids':pids, 'stats':samples})+b'\n')
            require(pressure < 3, 'RESOURCE_ABORT')
            if run['state'] in ('success','failed'):
                instances = af(m, session, 'GET', path+'/'+quote(m['run_id'], safe='')+'/taskInstances?limit=100')['task_instances']
                verify_deployment(m)
                result = terminal_audit(m, paths['runtime']/'pipeline', run, instances)
                write_new(root/'FINAL_E2E_RESULT.json', result)
                return
            require(time.monotonic() < deadline, 'DAGRUN_DEADLINE')
            time.sleep(5)
    except BaseException as exc:
        write_new(root/'FINAL_E2E_FAILURE.json', {'status':'E2E_FAIL_OR_AMBIGUOUS', 'run_id':m['run_id'],
                  'manifest_sha256':m['_manifest_sha256'], 'error_type':type(exc).__name__, 'at':utc_now()})
        raise
    finally:
        stop_resume(m, resume_id, approval_path)


H2_HASH = 'bac8d22e9d318e4466909ede10f9a78022e1e3410e531329ba0eaf20a7450ca9'
H2_RECEIPTS = {
    'bootstrap/provision.json': '4a6d74c5c4d8fbfb253862e3a5a91868675fcb6d91a3c53ff833fcac292f4e5e',
    'bootstrap/fs_probe_only_placeholder.20261112T163742Z.txt': '4c8ca4d9a0fd9a31f2626a99006297cbaff007418bd85e446eb2179eba7dc342',
    'controller/startup_attempt.json': '890ffbc39d4d1231b93804c83903af271780a71b70842cb79cf12a6587e0f60a',
    'controller/FINAL_E2E_FAILURE.json': '98188e6aea15147b06092d803d9b5bbf91d749411e58e39c70834d98fd02d0d1',
    'controller/stopped_1794501969691247400.json': '396d04f21e131c12716ef5e041679e8c699d48f0f75cf4ca394eba47303d23d4',
}
H2_CONTROL = {'manifest.json': H2_HASH,
    'approval.json': 'aad64ff4aa9441228940e4d8a63e33f67a4cd02773003313cdd37829e11f2eed',
    'admin_provision.superseding.json': 'e23456b91326ab218f5ff545aba929c511bd0d36c92ae866681c37a0911141bc'}


def resume_identity(m, resume_id):
    require(bool(re.fullmatch(r'[a-f0-9]{12}', resume_id or '')), 'RESUME_ID')
    project = m['resources']['project']+'-cb-'+resume_id
    return project, {s:project+'-'+s.removeprefix('e2e-') for s in SERVICES[1:]}


def resume_authority(m, resume_id, approval_path):
    if m['provision_attestation']['mode']=='creation_v1':
        return fresh_resume_authority(m,resume_id,approval_path)
    paths = host_paths(m, ROOT); resume_identity(m, resume_id)
    expected = paths['control']/('resume_'+resume_id+'.json')
    require(approval_path is not None and Path(approval_path).resolve()==expected.resolve(), 'RESUME_APPROVAL_PATH')
    reject_links(expected)
    a = json.loads(expected.read_bytes())
    require(a.get('schema_version')==1 and a.get('approved') is True and a.get('review_only') is False and
            a.get('approval_type') in ('CB_SIMPLE_AUTH_RESUME','CD_ISOLATED_RESUME') and bool(a.get('approved_by')) and bool(a.get('approved_at_utc')), 'RESUME_NOT_APPROVED')
    require(a.get('from_manifest_sha256')==H2_HASH and a.get('manifest_sha256')==m['_manifest_sha256'] and
            a.get('run_id')==m['run_id'] and a.get('resume_id')==resume_id and
            a.get('actions')==['start','stop'] and a.get('migration_allowed') is False and
            a.get('checkpoint_sha256')==H2_RECEIPTS, 'RESUME_BINDING')
    if m['_manifest_sha256'] != 'ffb54a72da7768461b8be8724118f6f52c93c5a3641b2092c81012d9782cdb24':
        require(a['approval_type']=='CD_ISOLATED_RESUME' and resume_id!='c0b000000001' and
                a.get('provisioning_allowed') is False and a.get('max_dagruns')==1, 'CD_RESUME_AUTHORITY_REQUIRED')
    return a


def resume_metadata_readonly(m):
    if m['provision_attestation']['mode']=='creation_v1':
        return fresh_metadata_readonly(m)
    sql = """BEGIN READ ONLY;
SELECT json_build_object('database',current_database(),
 'owner',(SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()),
 'revision',(SELECT version_num FROM alembic_version),
 'dagruns',(SELECT count(*) FROM dag_run),'tasks',(SELECT count(*) FROM task_instance),
 'pools',(SELECT coalesce(json_agg(json_build_object('name',pool,'slots',slots)),'[]'::json) FROM slot_pool WHERE pool='viq_e2e_once'),
 'dags',(SELECT coalesce(json_agg(json_build_object('dag_id',dag_id,'is_paused',is_paused)),'[]'::json) FROM dag));
ROLLBACK;"""
    value = docker(['exec','internet-quality-airflow-postgres','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1',
                    '-U','airflow','-d',m['resources']['metadata_database'],'-c',sql])
    d = json.loads(value)
    require(d=={'database':m['resources']['metadata_database'],'owner':m['resources']['metadata_user'],
        'revision':'d2f4e1b3c5a7','dagruns':0,'tasks':0,'pools':[{'name':'viq_e2e_once','slots':1}],
        'dags':[{'dag_id':DAG_ID,'is_paused':True}]}, 'RESUME_METADATA_DRIFT')
    return d


def resume_preflight(m, resume_id, auth):
    if m['provision_attestation']['mode']=='creation_v1':
        return fresh_resume_preflight(m,resume_id,auth)
    runtime_precheck(m); paths=host_paths(m,ROOT)
    from processing.e2e.attestation import verify_attestation
    verify_attestation(load_manifest(ROOT/'docs/e2e/viq_e2e_run_manifest.json'),
                       ROOT/'docs/e2e/viq_e2e_run_manifest.json',paths['control'])
    from processing.e2e.stages import verify_bootstrap_lineage
    mp=ROOT/'docs/e2e/viq_e2e_run_manifest.json'
    verify_bootstrap_lineage(load_manifest(mp),mp,paths['control']/'approval.json',paths['runtime']/'bootstrap/provision.json')
    require(auth==resume_authority(m,resume_id,paths['control']/('resume_'+resume_id+'.json')), 'RESUME_AUTHORITY_OBJECT_DRIFT')
    archive=paths['control']/'history'/H2_HASH
    for name,digest in H2_CONTROL.items():
        path=archive/name; reject_links(path)
        require(file_hash(path)==digest,'RESUME_H2_CONTROL_ANCHOR')
    old=load_manifest(archive/'manifest.json')
    ignored={'source_sha256','deployment_sha256'}
    require({k:v for k,v in old.items() if k not in ignored}=={k:v for k,v in m.items() if k not in ignored|{'_manifest_sha256'}},'RESUME_SCOPE_CHANGED')
    for name,digest in H2_RECEIPTS.items():
        path=paths['runtime']/name;reject_links(path);require(file_hash(path)==digest,'RESUME_CHECKPOINT_DRIFT')
    require(not any((paths['runtime']/'controller'/x).exists() for x in ('started.json','triggered.json','FINAL_E2E_RESULT.json')), 'RESUME_PRIOR_RUN_STATE')
    pipeline=paths['runtime']/'pipeline';reject_links(pipeline,recursive=True)
    require({x.name for x in pipeline.iterdir()}=={'.fs_probe_v2_scheduler_20261112T163742Z','provision.json'} and
            (pipeline/'provision.json').stat().st_size==0 and
            (pipeline/'.fs_probe_v2_scheduler_20261112T163742Z').is_file(),'RESUME_PIPELINE_NOT_EMPTY')
    stopped=json.loads((paths['runtime']/'controller/stopped_1794501969691247400.json').read_bytes())
    existing=set(docker(['ps','-a','--format','{{.Names}}']).splitlines())
    for recorded in stopped['containers']:
        c=inspect_e2e(m,recorded['service'])
        require(c['Id']==recorded['id'] and c['State']['Status']=='exited','RESUME_OLD_CONTAINER_DRIFT')
    require(container_name(m,'e2e-airflow-scheduler') not in existing,'RESUME_OLD_SCHEDULER_EXISTS')
    project,names=resume_identity(m,resume_id)
    require(not any(x in existing for x in names.values()),'RESUME_NEW_NAME_COLLISION')
    memory=docker(['exec','internet-quality-airflow-postgres','cat','/proc/meminfo'])
    require(int(next(x for x in memory.splitlines() if x.startswith('MemAvailable:')).split()[1])*1024>=m['minimum_available_bytes'],'RESUME_MEMORY')
    require(file_hash(m['host_raw'])==m['raw_sha256'] and file_hash(m['host_archive'])==m['archive_sha256'],'RESUME_INPUT_DRIFT')
    return resume_metadata_readonly(m)


def resume_execution_config(m, base, resume_id):
    """Derive only names/project/log basenames; preserve every bind/security/resource setting."""
    project,names=resume_identity(m,resume_id); cfg=copy.deepcopy(base);cfg['name']=project
    cfg['services'].pop('e2e-airflow-init')
    for service,s in cfg['services'].items():
        s['container_name']=names[service]
        s['environment']['VIQ_E2E_RESUME_LOG_BASENAME']='resume_'+resume_id+'.service.log'
    for v in cfg['volumes'].values():
        v.clear()
    for key,v in cfg['volumes'].items():v.update({'name':base['volumes'][key]['name'],'external':True})
    cfg['networks']['e2e']={'name':base['networks']['e2e']['name'],'external':True}
    require(set(cfg['services'])==set(SERVICES[1:]),'RESUME_SERVICE_SET')
    for service,s in cfg['services'].items():
        before=copy.deepcopy(base['services'][service]);after=copy.deepcopy(s)
        before.pop('container_name');after.pop('container_name');after['environment'].pop('VIQ_E2E_RESUME_LOG_BASENAME')
        require(before==after,'RESUME_CONFIG_DRIFT')
    return cfg


def resume_containers(m, resume_id, project):
    _,names=resume_identity(m,resume_id);existing=set(docker(['ps','-a','--format','{{.Names}}']).splitlines());found={}
    for service,name in names.items():
        if name not in existing:continue
        c=json.loads(docker(['inspect',name]))[0];labels=c['Config'].get('Labels',{})
        require(labels.get('com.docker.compose.project')==project and labels.get('com.docker.compose.service')==service and
                c['Image']==m['image_id'] and c['Config']['Image']==m['image'] and c['Config']['User']=='50000:0' and
                c['HostConfig']['ReadonlyRootfs'] and not c['HostConfig']['Privileged'],'RESUME_CONTAINER_IDENTITY')
        found[service]=c
    return found


def stop_resume(m,resume_id,approval_path):
    resume_authority(m,resume_id,approval_path);project,names=resume_identity(m,resume_id)
    root=host_paths(m,ROOT)['runtime']/'controller/resumes'/resume_id;reject_links(root,recursive=True)
    attempt=json.loads((root/'attempt.json').read_bytes())
    require(attempt['manifest_sha256']==m['_manifest_sha256'] and attempt['project']==project and attempt['container_names']==names,'RESUME_STOP_BINDING')
    found=resume_containers(m,resume_id,project)  # Validate all identities before any stop.
    if (root/'ready.json').exists() and m['_manifest_sha256']!='ffb54a72da7768461b8be8724118f6f52c93c5a3641b2092c81012d9782cdb24':
        ready=json.loads((root/'ready.json').read_bytes())
        require(ready.get('attempt_sha256')==file_hash(root/'attempt.json') and
                ready.get('container_ids')=={s:c['Id'] for s,c in found.items()}, 'RESUME_STOP_READY_IDS')
    if (root/'stopped.json').exists():
        require(all(c['State']['Status']=='exited' for c in found.values()),'RESUME_RUNNING_AFTER_STOP')
        prior=json.loads((root/'stopped.json').read_bytes())
        require(prior.get('manifest_sha256')==m['_manifest_sha256'] and prior.get('containers')=={s:c['Id'] for s,c in found.items()},'RESUME_STOP_RECEIPT_DRIFT')
        return
    for service in reversed(SERVICES[1:]):
        if service in found:docker(['stop','--time','20',found[service]['Id']])
    require(all(c['State']['Status']=='exited' for c in resume_containers(m,resume_id,project).values()),'RESUME_STOP_INCOMPLETE')
    write_new(root/'stopped.json',{'manifest_sha256':m['_manifest_sha256'],'run_id':m['run_id'],'at':utc_now(),
        'containers':{s:c['Id'] for s,c in found.items()},'data_cleanup':False})


def resume_runtime(m,resume_id,approval_path):
    mp=ROOT/'docs/e2e/viq_e2e_run_manifest.json';paths=host_paths(m,ROOT)
    for action in ('start','stop'):
        gate_c(load_manifest(mp),mp,paths['control']/'approval.json',action)
    verify_deployment(m)
    authority=resume_authority(m,resume_id,approval_path);metadata=resume_preflight(m,resume_id,authority)
    paths=host_paths(m,ROOT);project,names=resume_identity(m,resume_id)
    root=paths['runtime']/'controller/resumes'/resume_id;reject_links(root)
    require(not root.exists(),'RESUME_ALREADY_ATTEMPTED')
    base=json.loads(compose(m,['config','--format','json']))
    validate_mounts(m,ROOT,base,ROOT/'docs/e2e/viq_e2e_run_manifest.json',paths['control']/'approval.json')
    for key,v in base['volumes'].items():
        actual=json.loads(docker(['volume','inspect',v['name']]))[0]
        require(actual['Driver']=='local' and not actual.get('Options') and
                actual['Labels'].get('com.docker.compose.project')==m['resources']['project'] and
                actual['Labels'].get('com.docker.compose.volume')==key,'RESUME_VOLUME_DRIFT')
    for network in base['networks'].values():docker(['network','inspect',network['name']])
    cfg=resume_execution_config(m,base,resume_id)
    root.mkdir(parents=True,exist_ok=False)
    cp=root/'compose.runtime.json';write_new(cp,cfg)
    write_new(root/'attempt.json',{'manifest_sha256':m['_manifest_sha256'],'run_id':m['run_id'],'at':utc_now(),
        'project':project,'container_names':names,'compose_sha256':file_hash(cp),'resume_approval_sha256':file_hash(approval_path),
        'checkpoint_sha256':resume_checkpoints(m),'metadata_readonly':metadata,'migration_executed':False})
    def up(services):
        return docker(['compose','--env-file',str(paths['control']/'compose.env'),'--project-name',project,'-f',str(cp),'--profile','e2e-manual','up',
            '--no-build','--no-recreate','--pull','never','-d',*services])
    try:
        up(['e2e-airflow-api','e2e-airflow-dag-processor','e2e-api'])
        deadline=time.monotonic()+120
        while True:
            running=resume_containers(m,resume_id,project)
            require(all(s in running and running[s]['State']['Status']=='running' and not running[s]['State']['OOMKilled']
                for s in ('e2e-airflow-api','e2e-airflow-dag-processor','e2e-api')),'RESUME_SERVICE_EXITED')
            try:
                session=af_client(m);dags=af(m,session,'GET','/dags?limit=100')['dags']
                ready=len(dags)==1 and dags[0]['dag_id']==DAG_ID and dags[0]['is_paused'] and af(m,session,'GET',f'/dags/{DAG_ID}/dagRuns?limit=100')['total_entries']==0
            except Exception:ready=False
            if ready:break
            require(time.monotonic()<deadline,'RESUME_API_NOT_READY');time.sleep(2)
        up(['e2e-airflow-scheduler'])
        found=resume_containers(m,resume_id,project)
        require(set(found)==set(SERVICES[1:]) and all(c['State']['Status']=='running' and not c['State']['OOMKilled']
                for c in found.values()), 'RESUME_READY_SERVICE_SET')
        write_new(root/'ready.json',{'status':'RESUMED_RUNTIME_READY_REQUIRES_PRETRIGGER_CHECKS','manifest_sha256':m['_manifest_sha256'],
            'run_id':m['run_id'],'resume_id':resume_id,'project':project,'at':utc_now(),
            'attempt_sha256':file_hash(root/'attempt.json'),'compose_sha256':file_hash(cp),
            'container_ids':{s:c['Id'] for s,c in found.items()},'migration_executed':False,'dagrun_triggered':False,
            'c_c_ready':False,'bootstrap_receipt_sha256':resume_checkpoints(m)['bootstrap/provision.json']})
    except BaseException as ex:
        write_new(root/'failure.json',{'at':utc_now(),'error_type':type(ex).__name__,'migration_executed':False,'dagrun_triggered':False})
        stop_resume(m,resume_id,approval_path)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "render", "start", "run", "stop", "resume", "bootstrap"), nargs="?", default="prepare")
    parser.add_argument('--resume-id')
    parser.add_argument('--resume-approval', type=Path)
    parser.add_argument("--manifest", type=Path, default=ROOT/"docs/e2e/viq_e2e_run_manifest.json")
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.action == "prepare":
        require(args.run_id is not None, "EXPLICIT_FRESH_RUN_ID_REQUIRED")
        require(args.run_id != DEFAULT_RUN, "UNIT_ID_NOT_RUNTIME_AUTHORITY")
        prepare(args.manifest, args.run_id); return
    m = load_manifest(args.manifest)
    paths = host_paths(m, ROOT); root = paths["runtime"]/"controller"
    if args.action == "render":
        # Refresh public Compose interpolation after an approved image pin, without service I/O.
        paths["control"].mkdir(parents=True, exist_ok=True)
        (paths["control"]/"compose.env").write_text(render_env(m, args.manifest, args.approval or paths["control"]/"approval.json"), encoding="utf-8")
        return
    require(args.approval is not None, "APPROVAL_FILE_REQUIRED")
    gate_c(m, args.manifest, args.approval, 'start' if args.action=='resume' else 'provision' if args.action=='bootstrap' else args.action)
    gate_c(m, args.manifest, args.approval, "stop")
    if args.action != "stop":
        verify_deployment(m)
    m["_manifest_sha256"] = file_hash(args.manifest)
    if args.action=='bootstrap':
        fresh_initialize(m);return
    if args.action=='run':
        require(args.resume_id is not None and args.resume_approval is not None,'RUN_REQUIRES_EXPLICIT_RESUME')
        run_once(m,args.resume_id,args.resume_approval);return
    if args.action=='resume':
        resume_runtime(m,args.resume_id,args.resume_approval);return
    if args.action=='stop' and args.resume_id:
        stop_resume(m,args.resume_id,args.resume_approval);return
    try:
        {"start": start, "run": run_once, "stop": stop}[args.action](m)
    except BaseException as exc:
        if args.action in ("start", "run") and (root/"startup_attempt.json").exists():
            failure = root/"FINAL_E2E_FAILURE.json"
            if not failure.exists():
                write_new(failure, {"status": "E2E_FAIL_OR_AMBIGUOUS", "run_id": m["run_id"], "error_type": type(exc).__name__, "at": utc_now()})
            stop(m)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("E2E controller refused/failed: "+type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
