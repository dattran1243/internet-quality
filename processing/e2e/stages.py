"""The fourteen concrete stages for the single approved historical hour."""
from __future__ import annotations

import bz2
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sys

from processing.e2e.contracts import (ARCHIVE_SHA256, COUNTRY, DATE_COLUMNS, GOLD_COLUMNS,
    MID, PROBES, RAW_SHA256, START, STOP, STAGES, WINDOW_COUNTS, canonical, check_raw,
    digest, iso, multiset, row_hash, utc_now)
from processing.e2e.guards import file_hash, require, verify_sources
from processing.e2e.io import write_new
from processing.e2e.verification import ch_rows, compare_rows, expected_quality, rows_of

SILVER = "silver/ripe_atlas/ping_by_country/country=DE/"
DIMENSION = "silver/ripe_atlas/probes_e2e/"
ENRICHED = "silver/ripe_atlas/ping_enriched/country=DE/"
GOLD = "gold/ripe_atlas/hourly_network_quality/country=DE/"


def api_get(e, path, params=None):
    import requests
    session = requests.Session(); session.trust_env = False
    response = session.get(e.m["endpoints"]["api_internal"] + path, params=params, timeout=(5, 20))
    body = response.json()
    return {"status": response.status_code, "body": body,
            "run_header": response.headers.get("X-VIQ-E2E-Run-ID"), "body_sha256": digest(body)}


def quality_params():
    return {"measurement_id": MID, "country_code": "DE", "start": iso(START), "stop": iso(STOP)}


# Public historical anchors are fixed before the candidate manifest is hashed.
# Current manifest -> source; attestation -> manifest; approval -> attestation.
# Neither the source nor the manifest contains a hash of the current approval.
CC_HISTORICAL_ANCHORS = {
    "H2/admin_provision.superseding.json": "e23456b91326ab218f5ff545aba929c511bd0d36c92ae866681c37a0911141bc",
    "H2/approval.json": "aad64ff4aa9441228940e4d8a63e33f67a4cd02773003313cdd37829e11f2eed",
    "H2/manifest.json": "bac8d22e9d318e4466909ede10f9a78022e1e3410e531329ba0eaf20a7450ca9",
    "H3/admin_provision.superseding.json": "f21140b6aa5811f85706cb9e5a81981b1ecb6a4bac85ceece3ffb46bcc3386be",
    "H3/approval.json": "e6d6b56396e88f0385fe25d874f9f65690bbecbf02d1bc140329553d9f28453b",
    "H3/manifest.json": "3e43a16864dae3aca7843ee748ad0b6c080131d3e6e7dc849a41385296086343",
    "H4/admin_provision.superseding.json": "26a84c1dadf599bff971c344421498026a71441e728cd0630cb8039771e28a5a",
    "H4/approval.json": "29a93603cc586fb86a3a75714da9f6173cab58ff995f5f3411b263ea42d21907",
    "H4/manifest.json": "ffb54a72da7768461b8be8724118f6f52c93c5a3641b2092c81012d9782cdb24",
    "admin/clickhouse.json": "b87eef2df59d4fa9662941133d2364d8cad2ea9a4f35c2b3141f5364bb1b9f10",
    "admin/kafka.json": "e8c4de4b8bb074e50cf52c5264073e74d3c1a039f0977801e41c0e916a168c07",
    "admin/minio.json": "b4cbdea5378954cc3a91c02182d441012afa53c7eaa863e04d1c8f192ee75783",
    "admin/postgres.json": "4ab9ba28ff40b342cc582a73003aec9838f9bef6ee9d734c98980a6425173f74",
    "original_admin_provision.json": "eba75415724f99bf7151bc0cd0f981ac9abd854a29e0cb6b58d27cd6ac01e63a",
    "original_manifest.json": "0ac494439a3ac291e96f43d569ffe39f33bb29c67f477018725d80c281a14bd9"
}
CC_OWNER_AUTHORIZATION_SHA256 = 'aebdcdb7faaf1881fd6b776b038fdc124a24c4a4f39db8f60bb7965fd1b57da6'
CC_PARENT_MANIFEST_SHA256 = 'ffb54a72da7768461b8be8724118f6f52c93c5a3641b2092c81012d9782cdb24'
CC_BOOTSTRAP_SHA256 = '4a6d74c5c4d8fbfb253862e3a5a91868675fcb6d91a3c53ff833fcac292f4e5e'


def verify_fresh_creation_lineage(m, manifest_path, approval_path, provision_path):
    """Verify actual new creation with the existing creation_v1 strict verifier."""
    import base64
    import tempfile
    from processing.e2e.attestation import verify_attestation
    from processing.e2e.guards import gate_c, load_manifest
    from processing.e2e.layout import reject_links
    for path in (manifest_path, approval_path, provision_path):
        reject_links(path)
    require(m == load_manifest(manifest_path) and m['provision_attestation']['mode'] == 'creation_v1',
            'FRESH_MANIFEST_OBJECT')
    gate_c(m, manifest_path, approval_path, 'stop')
    approval = json.loads(Path(approval_path).read_bytes())
    require(approval.get('review_only') is False and approval.get('runtime_scope', {}).get('max_dagruns') == 1,
            'FRESH_APPROVAL_SCOPE')
    proof = approval.get('fresh_creation', {})
    require(set(proof) == {'schema_version','manifest_sha256','attestation_sha256','bootstrap_sha256','records'} and
            proof['schema_version'] == 1 and proof['manifest_sha256'] == file_hash(manifest_path), 'FRESH_PROOF_SCHEMA')
    records = proof['records']
    require(isinstance(records, dict) and set(records) == {'admin_provision.json','admin/kafka.json','admin/minio.json',
            'admin/postgres.json','admin/clickhouse.json'} and all(isinstance(v,str) for v in records.values()) and
            sum(map(len,records.values())) <= 1024*1024, 'FRESH_PUBLIC_RECORDS')
    decoded = {k:base64.b64decode(v,validate=True) for k,v in records.items()}
    require(sha256(decoded['admin_provision.json']).hexdigest() == proof['attestation_sha256'] and
            file_hash(provision_path) == proof['bootstrap_sha256'], 'FRESH_EXACT_BYTES')
    creation = json.loads(decoded['admin_provision.json'])
    require(creation.get('owner_authorization_request_sha256') == approval.get('owner_authorization_request_sha256') and
            bool(creation.get('owner_authorization_request_sha256')), 'FRESH_CREATION_OWNER_BINDING')
    with tempfile.TemporaryDirectory(prefix='viq-fresh-public-') as folder:
        root = Path(folder)
        for name,data in decoded.items():
            path = root/name; path.parent.mkdir(parents=True,exist_ok=True)
            with path.open('xb') as stream:
                stream.write(data)
        verified = verify_attestation(m, manifest_path, root)
    receipt = json.loads(Path(provision_path).read_bytes())
    require(receipt.get('manifest_sha256') == file_hash(manifest_path) and receipt.get('resources') == m['resources'] and
            receipt.get('creation_workflow_verified') is True and receipt.get('resources_created_in_this_init') is False and
            receipt.get('empty_resources_verified') is True and
            receipt.get('admin_attestation_sha256') == proof['attestation_sha256'] and
            receipt.get('original_admin_attestation_sha256') == verified['original_admin_attestation_sha256'],
            'FRESH_BOOTSTRAP_BINDING')
    return {'creation_v1_verified':True,'current_manifest_sha256':file_hash(manifest_path),
            'bootstrap_receipt_sha256':proof['bootstrap_sha256'],'creation_attestation_sha256':proof['attestation_sha256'],
            'resources_created_by_bootstrap':False,'run_authority_granted':False}


def verify_bootstrap_lineage(m, manifest_path, approval_path, provision_path):
    """Verify exact public evidence using the unchanged strict verifier.

    This read-only validation grants no run/start authority. Runtime entrypoints
    must still call gate_c(..., 'run'). Public files are materialized only into
    a private temporary directory; no control/history/private mount is needed.
    """
    if m['provision_attestation']['mode'] == 'creation_v1':
        return verify_fresh_creation_lineage(m,manifest_path,approval_path,provision_path)
    import base64
    import tempfile
    from processing.e2e.attestation import verify_attestation, verify_creation
    from processing.e2e.guards import gate_c, load_manifest
    from processing.e2e.layout import reject_links

    for path in (manifest_path, approval_path, provision_path):
        reject_links(path)
    require(m == load_manifest(manifest_path), 'LINEAGE_MANIFEST_OBJECT_DRIFT')
    gate_c(m, manifest_path, approval_path, 'stop')
    approval = json.loads(Path(approval_path).read_bytes())
    require(approval.get('review_only') is False, 'LINEAGE_OPERATIONAL_APPROVAL_NOT_PUBLISHED')
    capsule = approval.get('bootstrap_lineage', {})
    require(set(capsule) == {'schema_version', 'kind', 'parent_manifest_sha256',
            'bootstrap_receipt_sha256', 'records'} and capsule['schema_version'] == 1 and
            capsule['kind'] == 'VIQ_BOOTSTRAP_LINEAGE_V1' and
            capsule['parent_manifest_sha256'] == CC_PARENT_MANIFEST_SHA256 and
            capsule['bootstrap_receipt_sha256'] == CC_BOOTSTRAP_SHA256, 'LINEAGE_CAPSULE_SCHEMA')
    records = capsule['records']
    require(isinstance(records, dict) and set(records) == set(CC_HISTORICAL_ANCHORS) |
            {'current_admin_provision.superseding.json', 'owner_authorization.txt'}, 'LINEAGE_RECORD_SET')
    require(all(isinstance(v, str) for v in records.values()) and
            sum(map(len, records.values())) <= 1024*1024, 'LINEAGE_RECORD_SIZE')
    decoded = {name: base64.b64decode(value, validate=True) for name, value in records.items()}
    for name, expected in CC_HISTORICAL_ANCHORS.items():
        require(sha256(decoded[name]).hexdigest() == expected, 'LINEAGE_HISTORY_TAMPER:' + name)
    source_rebind = approval.get('source_rebind', {})
    require(sha256(decoded['owner_authorization.txt']).hexdigest() == CC_OWNER_AUTHORIZATION_SHA256 and
            source_rebind.get('owner_authorization_request_sha256') == CC_OWNER_AUTHORIZATION_SHA256,
            'LINEAGE_OWNER_AUTHORIZATION')
    attestation_bytes = decoded['current_admin_provision.superseding.json']
    attestation = json.loads(attestation_bytes)
    require(approval.get('lineage_attestation_sha256') == sha256(attestation_bytes).hexdigest() and
            attestation.get('owner_authorization_request_sha256') == CC_OWNER_AUTHORIZATION_SHA256 and
            source_rebind == {'owner_authorization_request_sha256': CC_OWNER_AUTHORIZATION_SHA256,
                'parent_manifest_sha256': CC_PARENT_MANIFEST_SHA256,
                'attestation_sha256': sha256(attestation_bytes).hexdigest(),
                'operator': attestation.get('operator'), 'recorded_at_utc': attestation.get('recorded_at_utc')},
            'LINEAGE_CURRENT_AUTHORIZATION')
    require(file_hash(provision_path) == CC_BOOTSTRAP_SHA256, 'LINEAGE_BOOTSTRAP_BYTES')
    provision = json.loads(Path(provision_path).read_bytes())
    manifests = {label: json.loads(decoded[label+'/manifest.json']) for label in ('H2', 'H3', 'H4')}
    parent = manifests['H4']
    require({k: v for k, v in parent.items() if k != 'source_sha256'} ==
            {k: v for k, v in m.items() if k != 'source_sha256'}, 'LINEAGE_CURRENT_SCOPE_DRIFT')
    before, after = parent['source_sha256'], m['source_sha256']
    require(set(before) == set(after) and {k for k in before if before[k] != after[k]} ==
            {'processing/e2e/stages.py', 'scripts/e2e_control.py'}, 'LINEAGE_UNAPPROVED_SOURCE_DELTA')
    for older, newer, source_delta, deployment_delta in (
            ('H2', 'H3', {'scripts/e2e_bootstrap.py', 'scripts/e2e_control.py'},
             {'infra/airflow/docker-compose.viq-e2e-isolated.yml'}),
            ('H3', 'H4', {'scripts/e2e_control.py'}, set())):
        old, new = manifests[older], manifests[newer]
        require({k:v for k,v in old.items() if k not in ('source_sha256','deployment_sha256')} ==
                {k:v for k,v in new.items() if k not in ('source_sha256','deployment_sha256')},
                'LINEAGE_HISTORICAL_SCOPE_DRIFT')
        for field, expected in (('source_sha256', source_delta), ('deployment_sha256', deployment_delta)):
            require(set(old[field]) == set(new[field]) and
                    {k for k in old[field] if old[field][k] != new[field][k]} == expected,
                    'LINEAGE_HISTORICAL_PIN_DELTA')
    with tempfile.TemporaryDirectory(prefix='viq-public-lineage-') as folder:
        root = Path(folder)
        def put(relative, data):
            path = root/relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('xb') as stream:
                stream.write(data)
            return path
        shared = {k:v for k,v in decoded.items() if k.startswith('admin/') or
                  k in ('original_manifest.json', 'original_admin_provision.json')}
        for name, data in shared.items():
            put(name, data)
        original = json.loads(decoded['original_admin_provision.json'])
        verify_creation(original, CC_HISTORICAL_ANCHORS['original_manifest.json'], m['resources'], root)
        results = {}
        for label in ('H2', 'H3', 'H4', 'current'):
            sub = root/label
            for name, data in shared.items():
                put(label+'/'+name, data)
            apath = put(label+'/admin_provision.superseding.json', attestation_bytes if label == 'current'
                        else decoded[label+'/admin_provision.superseding.json'])
            mp = Path(manifest_path) if label == 'current' else put(label+'/manifest.json', decoded[label+'/manifest.json'])
            clean = m if label == 'current' else load_manifest(mp)
            results[label] = verify_attestation(clean, mp, sub)
            if label != 'current':
                old_approval = put(label+'/approval.json', decoded[label+'/approval.json'])
                for action in ('start', 'stop'):
                    gate_c(clean, mp, old_approval, action)
    require(provision.get('manifest_sha256') == CC_HISTORICAL_ANCHORS['H2/manifest.json'] and
            provision.get('resources') == m['resources'] and provision.get('creation_workflow_verified') is True and
            provision.get('resources_created_in_this_init') is False and
            provision.get('admin_attestation_sha256') == results['H2']['admin_attestation_sha256'] and
            provision.get('original_admin_attestation_sha256') == results['current']['original_admin_attestation_sha256'],
            'PROVISION_RECEIPT_BINDING')
    return {'bootstrap_receipt_sha256': CC_BOOTSTRAP_SHA256, 'parent_manifest_sha256': CC_PARENT_MANIFEST_SHA256,
            'current_manifest_sha256': file_hash(manifest_path), 'verified_revisions': ['H1','H2','H3','H4','current'],
            'current_attestation_sha256': results['current']['admin_attestation_sha256'], 'run_authority_granted': False}


def guard_run_and_inputs(e):
    verify_sources(e.m, '/opt/project')
    require(file_hash('/opt/e2e/input/raw.jsonl') == RAW_SHA256 and
            file_hash('/opt/e2e/input/probes.json.bz2') == ARCHIVE_SHA256, 'INPUT_DRIFT')
    memory = {line.split(':')[0]: int(line.split()[1])*1024
              for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemTotal:', 'MemAvailable:'))}
    require(memory['MemTotal'] >= e.m['minimum_vm_bytes'] and
            memory['MemAvailable'] >= 3*1024**3, 'VM_RAM_NO_GO')
    lineage = verify_bootstrap_lineage(e.m, os.environ['VIQ_E2E_MANIFEST'],
                                     os.environ['VIQ_E2E_APPROVAL'], e.root/'provision.json')
    return {'memory': memory, 'source_sha256': e.m['source_sha256'], 'image': e.m['image'],
            'provision_sha256': file_hash(e.root/'provision.json'), 'bootstrap_lineage': lineage}


def verify_empty_run_resources(e):
    from processing.e2e.kafka import broker_state
    from processing.e2e.clickhouse import ClickHouse
    state = broker_state(e.m)
    require(all(state[name] == {"low": 0, "high": 0} for name in
                (e.m["resources"]["topic"], e.m["resources"]["dlq"])), "TOPIC_NOT_NEW_EMPTY")
    require(state["committed"] < 0, "GROUP_ALREADY_COMMITTED")
    require(e.s3.get_bucket_versioning(Bucket=e.bucket).get("Status") == "Enabled", "BUCKET_NOT_VERSIONED")
    require(not e.keys(), "BUCKET_NOT_EMPTY")
    versions = e.s3.list_object_versions(Bucket=e.bucket, MaxKeys=1)
    require(not versions.get("Versions") and not versions.get("DeleteMarkers"), "BUCKET_HAS_HISTORY")
    ClickHouse(e.m).empty()
    baseline = api_get(e, "/e2e/run", {"expected_run_id": e.m["run_id"]})
    quality = api_get(e, "/quality/hourly", quality_params())
    require(baseline["status"] == 404 and quality["status"] == 200 and quality["body"] == [], "API_BASELINE_NOT_EMPTY")
    return {"kafka": state, "api_baseline": baseline, "quality_baseline": quality}


def pin_raw_and_scope(e):
    raw = Path("/opt/e2e/input/raw.jsonl").read_bytes()
    archive = Path("/opt/e2e/input/probes.json.bz2").read_bytes()
    require(sha256(raw).hexdigest() == RAW_SHA256 and sha256(archive).hexdigest() == ARCHIVE_SHA256, "INPUT_DRIFT")
    windows = check_raw([json.loads(line) for line in raw.splitlines() if line.strip()])
    url = (f"https://atlas.ripe.net/api/v2/measurements/{MID}/results/?start={START}&stop={STOP}&probe_ids="
           + ",".join(map(str, PROBES)) + "&format=json")
    scope = {"country_code": "DE", "measurement_id": MID, "probe_ids": PROBES,
             "source_mode": "verified_historical_raw_replay", "raw_sha256": RAW_SHA256,
             "snapshot_date": "2026-10-18", "snapshot_time_precision": "day", "run_id": e.m["run_id"]}
    return {"raw": e.put_new("input/raw.jsonl", raw),
            "archive": e.put_new("input/probes.json.bz2", archive),
            "scope": e.put_new("input/probe_scope.json", canonical(scope)),
            "window_counts": list(map(len, windows)), "source_url": url}


def publish_raw_kafka(e):
    from processing.e2e.kafka import publish
    return publish(e)


def consume_kafka_bronze(e):
    from processing.e2e.runtime import bounded_process
    count = e.read("publish_raw_kafka")["expected_messages"]
    require(count == 12, "EVENT_COUNT_DRIFT")
    bounded_process([sys.executable, "-u", "/opt/project/scripts/consume_ripe_kafka_to_bronze.py",
                    "--max-messages", str(count)], dict(os.environ), e.root / "consumer.log", 180, 60, isolate=False)
    return {"consumer_exit": 0, "expected_messages": count, "consumer_source": e.m["source_sha256"]["scripts/consume_ripe_kafka_to_bronze.py"]}


def verify_bronze(e):
    from ingestion.bronze.ripe_atlas import bronze_object_key
    from processing.e2e.kafka import broker_state
    events = e.read("publish_raw_kafka")["events"]
    state = broker_state(e.m); r = e.m["resources"]
    require(state[r["topic"]] == {"low": 0, "high": 12} and state[r["dlq"]] == {"low": 0, "high": 0}
            and state["committed"] == 12, "KAFKA_OFFSET_OR_DLQ_MISMATCH")
    pins, raw = [], []
    started = json.loads(e.receipt_path(STAGES[0]).read_bytes())["started_at"]
    for offset, event in enumerate(events):
        key = bronze_object_key({"window_start_utc": event["window_start"], "country_code": "DE",
                                 "measurement_id": MID, "event_id": event["event_id"]})
        pin = e.pin(key); body = json.loads(e.get(pin)); meta = body["metadata"]
        require(meta["event_id"] == event["event_id"] and meta["window_id"] == event["window_id"] and
                meta["measurement_id"] == MID and meta["country_code"] == "DE" and
                meta["requested_start_utc"] == event["window_start"] and
                meta["chunk_index"] == 0 and meta["chunk_count"] == 1 and
                meta["record_count"] == len(body["records"]) == event["records"], "BRONZE_CONTRACT_MISMATCH")
        require(meta["kafka"] == {"topic": r["topic"], "partition": 0, "offset": offset, "key": event["key"]}, "BRONZE_KAFKA_LINEAGE")
        require(meta["probe_scope_key"] == e.read("pin_raw_and_scope")["scope"]["key"] and
                datetime.fromisoformat(meta["ingested_at_utc"].replace("Z", "+00:00")) >= datetime.fromisoformat(started),
                "OLD_BRONZE_OR_SCOPE")
        pins.append(pin); raw.extend(body["records"])
    expected = [json.loads(line) for line in e.get(e.read("pin_raw_and_scope")["raw"]).splitlines() if line.strip()]
    require(multiset(raw) == multiset(expected) and e.keys("bronze/") == sorted(p["key"] for p in pins), "BRONZE_RAW_MULTISET")
    return {"objects": pins, "raw_multiset_sha256": row_hash(raw), "records": len(raw), "kafka": state}


def silver_12_windows(e):
    from processing.e2e.runtime import spark_session
    from scripts.run_silver_country import main
    from ingestion.ripe_atlas.models import TimeWindow, parse_utc
    from processing.silver.incremental import existing_window_rows, filter_event_window
    from processing.silver.window_manifest import write_silver_window_manifest
    for pin in e.read("verify_bronze")["objects"]:
        e.get(pin)
    spark = spark_session(); checks = []
    try:
        target = f"s3a://{e.bucket}/{SILVER}"
        for i, count in enumerate(WINDOW_COUNTS):
            start, stop = iso(START+i*300), iso(START+(i+1)*300)
            window = TimeWindow(parse_utc(start), parse_utc(stop))
            main(["--country-code", "DE", "--measurement-id", str(MID), "--start", start, "--stop", stop], spark=spark)
            spark.catalog.refreshByPath(target)
            frame = filter_event_window(existing_window_rows(spark, target, window), window)
            rows = rows_of(frame)
            require(len(rows) == count and all(row["validation_status"] == "valid" and
                    row["measurement_id"] == MID and row["country_code"] == "DE" and row["target"] == "fastly.com"
                    for row in rows), "SILVER_WINDOW_SCOPE_OR_VALIDITY")
            marker = write_silver_window_manifest(e.s3, e.bucket, window, ["DE"], [MID])
            result = {"index": i, "start": start, "stop": stop, "records": count, "rows_sha256": row_hash(rows),
                      "marker": e.pin(marker), "run_id": e.m["run_id"]}
            result["sidecar"] = e.put_new(f"lineage/silver_window_{i:02d}.json", canonical(result))
            checks.append(result)
        pins = e.snapshot(SILVER); final = e.spark_read(spark, SILVER, pins)
        all_rows = rows_of(final)
        require(len(all_rows) == 375, "SILVER_FINAL_COUNT")
        for i, result in enumerate(checks):
            window = TimeWindow(parse_utc(result["start"]), parse_utc(result["stop"]))
            require(row_hash(rows_of(filter_event_window(final, window))) == result["rows_sha256"], "SILVER_OVERWRITE_LOSS")
        return {"root": SILVER, "objects": pins, "windows": checks, "records": 375,
                "valid": 375, "invalid": 0, "schema": final.schema.jsonValue(), "rows_sha256": row_hash(all_rows)}
    finally:
        spark.stop()


def write_frame(e, frame, root):
    from processing.e2e.verification import rows_of
    before = rows_of(frame); schema = frame.schema.jsonValue()
    require(not e.keys(root), "DATASET_ALREADY_EXISTS")
    frame.write.mode("errorifexists").parquet(f"s3a://{e.bucket}/{root}")
    pins = e.snapshot(root); readback = e.spark_read(frame.sparkSession, root, pins)
    require(row_hash(before) == row_hash(rows_of(readback)) and
            [(f.name, f.dataType.simpleString()) for f in frame.schema] ==
            [(f.name, f.dataType.simpleString()) for f in readback.schema], "DATASET_READBACK_MISMATCH")
    return {"root": root, "objects": pins, "records": len(before), "schema": schema, "rows_sha256": row_hash(before)}


def build_real_probe_dimension(e):
    from processing.e2e.runtime import spark_session
    from processing.e2e.ripe_archive_adapter import adapt_archive
    from processing.silver.ripe_atlas_probe import build_probe_dimension, validate_probe_dimension
    probes = adapt_archive(json.loads(bz2.decompress(e.get(e.read("pin_raw_and_scope")["archive"]))))
    spark = spark_session()
    try:
        frame = build_probe_dimension(spark, probes, snapshot_at=datetime(2026, 10, 18, tzinfo=timezone.utc))
        validate_probe_dimension(frame, "DE", "Connected")
        result = write_frame(e, frame, DIMENSION)
        require(result["records"] == 24, "DIMENSION_COUNT")
        return {**result, "snapshot_time_precision": "day", "snapshot_time_is_day_label": True}
    finally:
        spark.stop()


def enrich_silver(e):
    from processing.e2e.runtime import spark_session
    from processing.silver.ripe_atlas_ping_country import enrich_ping_country, validate_enriched_ping
    spark = spark_session()
    try:
        silver = e.read("silver_12_windows"); dim = e.read("build_real_probe_dimension")
        ping = e.spark_read(spark, silver["root"], silver["objects"])
        probes = e.spark_read(spark, dim["root"], dim["objects"])
        enriched, metrics = enrich_ping_country(ping, probes, [MID], "DE")
        validate_enriched_ping(enriched, [MID], "DE", expected_rows=375)
        require(metrics.matched_ping_rows == 375 and metrics.unmatched_ping_rows == 0 and
                metrics.matched_distinct_probes == 24, "ENRICHMENT_COVERAGE")
        return {**write_frame(e, enriched, ENRICHED), "join_metrics": asdict(metrics)}
    finally:
        spark.stop()


def gold_full_hour(e):
    from processing.e2e.runtime import spark_session
    from scripts.run_gold_global_hour import require_complete_silver_hour
    from processing.gold.ripe_atlas_hourly import validate_gold_input, build_hourly_network_quality, validate_hourly_gold, write_hourly_gold
    from ingestion.ripe_atlas.models import TimeWindow, parse_utc
    silver = e.read("silver_12_windows")
    require(len(silver["windows"]) == 12, "INCOMPLETE_HOUR")
    for i, result in enumerate(silver["windows"]):
        require(result["index"] == i and result["records"] == WINDOW_COUNTS[i] and result["run_id"] == e.m["run_id"], "WINDOW_RECEIPT_SCOPE")
        marker = json.loads(e.get(result["marker"]))
        require(marker["window_start_utc"] == iso(START+i*300) and marker["countries"] == ["DE"] and
                marker["measurement_ids"] == [MID], "WINDOW_MARKER_SCOPE")
        e.get(result["sidecar"])
    require_complete_silver_hour(e.s3, e.bucket, TimeWindow(parse_utc(iso(START)), parse_utc(iso(STOP))))
    spark = spark_session()
    try:
        source = e.read("enrich_silver"); enriched = e.spark_read(spark, source["root"], source["objects"])
        validate_gold_input(enriched, [MID], "DE")
        gold = build_hourly_network_quality(enriched).cache(); validate_hourly_gold(gold, [MID], "DE")
        rows = rows_of(gold); source_rows = rows_of(enriched)
        require(len(rows) == 24 and sum(r["sample_count"] for r in rows) == 375 and
                all(r["target"] == "fastly.com" and r["country_code"] == "DE" for r in rows), "GOLD_SCOPE_CONSERVATION")
        for metric in ("sent", "received"):
            require(sum(r[f"total_packets_{metric}"] for r in rows) ==
                    sum(r[f"packets_{metric}"] or 0 for r in source_rows if r["validation_status"] == "valid"), "GOLD_PACKET_CONSERVATION")
        require(not e.keys(GOLD), "GOLD_ALREADY_EXISTS")
        write_hourly_gold(gold, f"s3a://{e.bucket}/{GOLD}")
        pins = e.snapshot(GOLD); readback = e.spark_read(spark, GOLD, pins)
        require(row_hash(rows) == row_hash(rows_of(readback)), "GOLD_READBACK_MISMATCH")
        normalized = ch_rows(rows)
        return {"root": GOLD, "objects": pins, "records": 24, "samples": 375,
                "schema_sha256": digest(gold.schema.jsonValue()), "rows_sha256": row_hash(rows),
                "clickhouse_rows_sha256": row_hash(normalized), "production_accepted": False, "registry_committed": False}
    finally:
        spark.stop()


def load_clickhouse_insert_only(e):
    from processing.e2e.clickhouse import ClickHouse, gold_from_pins
    client = ClickHouse(e.m); client.empty()
    rows = gold_from_pins(e); h = file_hash(e.receipt_path("gold_full_hour"))
    extra = {"e2e_run_id": e.m["run_id"], "raw_sha256": RAW_SHA256, "gold_manifest_sha256": h}
    client.insert("hourly_network_quality_rows", [{**row, **extra} for row in rows], (*GOLD_COLUMNS, *extra))
    return {"insert_batches": 1, "records": len(rows), "gold_manifest_sha256": h,
            "timestamp_conversion": "UTC DateTime64(3), truncate sub-millisecond", "retry_on_ambiguous_insert": False}


def verify_clickhouse_rows(e):
    from processing.e2e.clickhouse import ClickHouse, gold_from_pins
    client = ClickHouse(e.m); gold = e.read("gold_full_hour")
    h = file_hash(e.receipt_path("gold_full_hour")); extra = ("e2e_run_id", "raw_sha256", "gold_manifest_sha256")
    actual = client.select(f"SELECT {','.join((*GOLD_COLUMNS, *extra))} FROM {client.db}.hourly_network_quality_rows")
    require(len(actual) == 24 and all(r["e2e_run_id"] == e.m["run_id"] and
            r["raw_sha256"] == RAW_SHA256 and r["gold_manifest_sha256"] == h for r in actual), "CH_LINEAGE_MISMATCH")
    compare_rows(gold_from_pins(e), ch_rows(actual))
    view = client.select(f"SELECT {','.join(GOLD_COLUMNS)} FROM {client.db}.hourly_network_quality")
    compare_rows(gold_from_pins(e), ch_rows(view))
    require(client.select(f"SELECT count() AS n FROM {client.db}.run_receipts")[0]["n"] == 0, "VERIFIED_RECEIPT_ALREADY_EXISTS")
    receipt = {"e2e_run_id": e.m["run_id"], "rid": e.m["resources"]["rid"], "raw_sha256": RAW_SHA256,
               "gold_manifest_sha256": h, "schema_sha256": gold["schema_sha256"], "row_count": 24,
               "sample_count": 375, "status": "verified"}
    client.insert("run_receipts", [receipt], tuple(receipt))
    stored = client.select(f"SELECT {','.join(receipt)} FROM {client.db}.run_receipts")
    require(stored == [receipt], "VERIFIED_RECEIPT_READBACK")
    return {"verified_receipt": receipt, "full_row_comparison": True}


def verify_fastapi_same_run(e):
    from processing.e2e.clickhouse import gold_from_pins
    receipt = api_get(e, "/e2e/run", {"expected_run_id": e.m["run_id"]})
    require(receipt["status"] == 200 and receipt["run_header"] == e.m["run_id"] and
            receipt["body"] == e.read("verify_clickhouse_rows")["verified_receipt"], "API_RUN_RECEIPT")
    quality = api_get(e, "/quality/hourly", quality_params())
    require(quality["status"] == 200 and quality["run_header"] == e.m["run_id"], "API_QUALITY_RESPONSE")
    expected = expected_quality(gold_from_pins(e)); actual = json.loads(json.dumps(quality["body"]))
    for row in actual:
        stamp = datetime.fromisoformat(row["hour_start_utc"].replace("Z", "+00:00"))
        row["hour_start_utc"] = stamp.replace(tzinfo=stamp.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()
    compare_rows(expected, actual, ("hour_start_utc", "measurement_id", "target"))
    negative = [api_get(e, "/e2e/run", {"expected_run_id": "wrong_run"}),
                api_get(e, "/quality/hourly", {**quality_params(), "country_code": "ZZ"}),
                api_get(e, "/quality/hourly", {**quality_params(), "start": "2026-10-18T13:00:00"}),
                api_get(e, "/quality/hourly", {**quality_params(), "start": iso(STOP), "stop": iso(STOP+3600)})]
    require([r["status"] for r in negative] == [409, 422, 422, 200] and negative[-1]["body"] == [], "API_NEGATIVE_CHECK")
    return {"receipt": receipt, "quality": quality, "expected_quality": expected, "negative": negative}


def final_acceptance(e):
    for stage in STAGES[:-1]:
        e.read(stage)
    verify_sources(e.m, "/opt/project")
    return {"status": "DATA_CHECKS_PASSED", "e2e_verdict": "PENDING_TERMINAL_DAGRUN_AUDIT",
            "required_tasks": list(STAGES), "production_accepted": False,
            "dashboard_accepted": False, "live_ingestion": False}


def execute(stage, evidence):
    require(stage in STAGES, "UNKNOWN_STAGE")
    return globals()[stage](evidence)
