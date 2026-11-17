"""TEST ONLY full-hour pinned Silver -> sandbox Gold.

Consumes a signed test-only 60-window plan, confirms the matching PostgreSQL
sandbox head before and after the reads, verifies all S3 VersionId bytes and
all Parquet rows with PATCH02, then invokes VIQ's actual Gold transforms.
Does not publish registry, S3 Gold, ClickHouse, Airflow or production outputs.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat

COUNTRIES = ('DE', 'JP', 'SG', 'US', 'VN')
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
MAX_HOUR_BYTES = 512 * 1024 * 1024

class PinnedGoldBlocked(RuntimeError):
    pass

def require(ok, reason):
    if not ok:
        raise PinnedGoldBlocked(reason)

def sha_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fp:
        for chunk in iter(lambda: fp.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def safe_file(path, max_bytes):
    path = Path(path)
    try:
        st = path.lstat()
    except OSError as exc:
        raise PinnedGoldBlocked('INPUT_NOT_FOUND') from exc
    require(path.is_file() and not path.is_symlink() and
            not (getattr(st,'st_file_attributes',0) & getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',0x400)),
            'REGULAR_FILE_REQUIRED')
    require(0 < st.st_size <= max_bytes, 'FILE_SIZE_GUARD')
    return path

def parse_probe_bundle(bundle):
    require(type(bundle) is dict and set(bundle) == {'schema','probes'}, 'PROBE_BUNDLE_FIELDS')
    require(bundle['schema'] == 'viq-pinned-gold-test-probes-v1', 'PROBE_BUNDLE_SCHEMA')
    probes = bundle['probes']
    require(type(probes) is list and len(probes) == 5, 'FIVE_PROBE_FILES_REQUIRED')
    result = {}
    for record in probes:
        require(type(record) is dict and set(record) == {'country','parquet_path','sha256'}, 'PROBE_ENTRY_FIELDS')
        code = record['country']; digest = record['sha256']
        require(code in COUNTRIES and code not in result, 'PROBE_COUNTRY_DUP_OR_INVALID')
        require(type(digest) is str and HEX64.fullmatch(digest), 'PROBE_SHA256')
        file = safe_file(record['parquet_path'],64*1024*1024)
        require(sha_file(file) == digest, 'PROBE_FILE_HASH_MISMATCH')
        result[code] = str(file.resolve())
    require(set(result)==set(COUNTRIES), 'PROBE_COUNTRY_COVERAGE')
    return result

def parse_oracle_bundle(bundle, receipts):
    require(type(bundle) is dict and set(bundle) == {'schema','entries'}, 'ORACLE_FIELDS')
    require(bundle['schema'] == 'viq-patch04-independent-oracle-v1', 'ORACLE_SCHEMA')
    entries=bundle['entries']
    require(type(entries) is list and len(entries) == 60, 'ORACLE_COVERAGE')
    mapping={}
    for e in entries:
        require(type(e) is dict and set(e) == {'country','window_index','oracle_jsonl','columns','arrow_types'}, 'ORACLE_ENTRY_FIELDS')
        key=(e['country'],e['window_index'])
        require(key not in mapping and key[0] in COUNTRIES and type(key[1]) is int and 0 <= key[1] < 12, 'ORACLE_DUPLICATE_OR_SCOPE')
        require(type(e['columns']) is list and type(e['arrow_types']) is list and len(e['columns'])>0 and len(e['columns'])==len(e['arrow_types']), 'ORACLE_COLUMNS_REQUIRED')
        mapping[key]=e
    require(set(mapping) == {(r['country'],r['window_index']) for r in receipts}, 'ORACLE_PLAN_MISMATCH')
    return mapping

def require_test_registry_head(conn, plan, digest):
    """Read-only PostgreSQL check; no writes or schema DDL."""
    db = conn.info.dbname
    require(type(db) is str and re.fullmatch(r'viq_s14_test_[0-9a-f]{12}', db) is not None,
            'TEST_DATABASE_REQUIRED')
    namespace = 's14test_' + plan['namespace'].split('_',1)[1]
    gen = hashlib.sha256((digest+plan['hour_utc']).encode('ascii')).hexdigest()[:32]
    with conn.cursor() as cur:
        cur.execute('SELECT current_database()')
        require(cur.fetchone()[0] == db, 'PG_DATABASE_IDENTITY')
        cur.execute('SELECT epoch,generation,digest FROM viq_s14test.heads WHERE namespace=%s AND hour_utc=%s',
                    (namespace,plan['hour_utc']))
        row = cur.fetchone()
    require(row is not None, 'PG_HEAD_NOT_COMMITTED')
    require(int(row[0]) == plan['epoch'] and row[1] == gen and row[2] == digest, 'PG_HEAD_MISMATCH')
    return {'epoch':int(row[0]),'generation':row[1],'digest':row[2]}

def verify_pinned_inputs(plan, oracle_map, s3, scratch, *, download=None, verify=None):
    """Download and verify every pinned version; fail on partial coverage or drift.

    scratch must be an already-created EMPTY throwaway directory.
    """
    require(type(plan) is dict and type(plan.get('receipts')) is list and len(plan['receipts']) == 60,
            'COMPLETE_60_WINDOW_PLAN_REQUIRED')
    from processing.registry.verified_s3_sandbox import download_exact_version
    from processing.silver.parquet_independent_verifier import verify_files
    from processing.gold.verified_hour_plan import canonical, sha
    download = download or download_exact_version
    verify = verify or verify_files
    require(Path(scratch).is_dir() and not any(Path(scratch).iterdir()),'SCRATCH_NOT_EMPTY')
    out={c:[] for c in COUNTRIES}; total=0; results=[]
    for r in sorted(plan['receipts'], key=lambda e:(e['country'],e['window_index'])):
        total+=r['byte_count']
        require(total <= MAX_HOUR_BYTES,'HOUR_BYTES_EXCEEDED')
        e=oracle_map[(r['country'],r['window_index'])]
        # Key deliberately derived from validated scope, never from S3 path.
        file=Path(scratch)/(r['country']+'_'+f"{r['window_index']:02d}"+'.parquet')
        downloaded=download(s3,r,file)
        require(downloaded.get('version_id') == r['version_id'] and downloaded.get('byte_sha256') == r['byte_sha256'],'PINNED_GET_MISMATCH')
        result=verify([str(file)], e['oracle_jsonl'],e['columns'],e['arrow_types'])
        schema_hash=sha(canonical({'physical_columns':e['columns'],'physical_types':e['arrow_types']}))
        require(schema_hash == r['schema_sha256'], 'SCHEMA_DIGEST_MISMATCH')
        require(result['row_count'] == r['row_count'] and result['row_multiset_sha256'] == r['row_multiset_sha256'], 'ROWS_OR_DIGEST_MISMATCH')
        require(result['files'][0]['file_sha256'] == r['byte_sha256'] and result['files'][0]['file_size'] == r['byte_count'], 'VERIFIED_BYTES_MISMATCH')
        out[r['country']].append(str(file))
        results.append({'country':r['country'],'window_index':r['window_index'],
                        'version_id':r['version_id'],'file_sha256':r['byte_sha256'],
                        'row_count':r['row_count']})
    require(all(len(out[c])==12 for c in COUNTRIES), 'MISSING_PINNED_WINDOWS')
    return out,results

def gold_for_country(spark, country, parquet_paths, probe_path, measurements, hour):
    """VIQ's actual Spark enrich + Gold functions; strictly no external writes."""
    from pyspark.sql import functions as F
    from processing.silver.ripe_atlas_ping_country import enrich_ping_country, validate_enriched_ping
    from processing.gold.ripe_atlas_hourly import build_hourly_network_quality, validate_gold_input, validate_hourly_gold
    from datetime import datetime,timedelta,timezone
    start=datetime.strptime(hour,'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    stop=start+timedelta(hours=1)
    ping=spark.read.parquet(*parquet_paths)
    require('event_time_utc' in ping.columns, 'SILVER_EVENT_TIME_MISSING')
    ping=ping.filter((F.col('event_time_utc')>=F.lit(start)) & (F.col('event_time_utc')<F.lit(stop)))
    if 'country_code' in ping.columns:
        ping=ping.filter(F.col('country_code')==country)
    count=ping.count()
    require(count>0, 'EMPTY_COUNTRY_HOUR')
    ping=(ping.withColumn('year',F.date_format('event_time_utc','yyyy'))
              .withColumn('month',F.date_format('event_time_utc','MM'))
              .withColumn('day',F.date_format('event_time_utc','dd'))
              .withColumn('hour',F.date_format('event_time_utc','HH')))
    probes=spark.read.parquet(probe_path)
    enriched, join_metrics=enrich_ping_country(ping,probes,measurements,country)
    require(join_metrics.output_rows==join_metrics.input_ping_rows==count,'LOSSLESS_ENRICHMENT_FAILED')
    validate_enriched_ping(enriched,measurements,country,count)
    validate_gold_input(enriched,measurements,country)
    gold=build_hourly_network_quality(enriched)
    validate_hourly_gold(gold,measurements,country)
    rows=gold.count()
    source=int(gold.agg(F.sum('sample_count')).collect()[0][0] or 0)
    require(source==count,'GOLD_SAMPLE_CONSERVATION_FAILED')
    return gold,{'country':country,'silver_rows':count,'gold_rows':rows,'source_samples':source}
