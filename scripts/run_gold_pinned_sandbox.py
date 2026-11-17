#!/usr/bin/env python3
"""Opt-in TEST-ONLY pinned input -> real VIQ Spark Gold into new local evidence.

REQUIRES a complete, independently supplied 60-window test plan, oracles,
probe files, live MinIO S3 VersionIds and a matching live PostgreSQL test head.
No production paths, no Gold/Registry publication, no ClickHouse, no DAG.
"""
from __future__ import annotations
import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import secrets
import sys
from urllib.parse import urlsplit

MIN_FREE_HOST_RAM_GIB = 5.0

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from processing.gold.pinned_hour_sandbox import (
    COUNTRIES, PinnedGoldBlocked, require, safe_file,
    parse_probe_bundle, parse_oracle_bundle, verify_pinned_inputs,
    require_test_registry_head, gold_for_country,
)


def check_test_endpoint(url):
    u=urlsplit(url)
    require(u.scheme=='http' and u.hostname=='127.0.0.1' and
            u.port is not None and 1 <= u.port <= 65535 and
            not (u.username or u.password or u.query or u.fragment) and u.path in ('','/'),
            'LOOPBACK_S3_REQUIRED')
    return url


def check_test_dsn(dsn):
    u=urlsplit(dsn)
    require(u.scheme in ('postgres','postgresql') and u.hostname=='127.0.0.1' and
            u.port is not None and 1 <= u.port <= 65535 and
            re.fullmatch(r'/viq_s14_test_[0-9a-f]{12}',u.path or '') and
            not u.query and not u.fragment,'LOOPBACK_TEST_PG_REQUIRED')
    return dsn


def check_evidence_root(path):
    root=Path(path).resolve()
    require(root.is_dir(), 'EVIDENCE_ROOT_MISSING')
    # Refuse roots inside the source working tree or unrelated drives.
    require(root.name.lower() == 'stage22_review' and root.parent.name.lower() == 'de',
            'EVIDENCE_ROOT_SCOPE')
    return root


def require_windows_host_ram():
    require(sys.platform == 'win32', 'WINDOWS_HOST_REQUIRED_FOR_SANDBOX')
    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [('dwLength', ctypes.c_ulong), ('dwMemoryLoad', ctypes.c_ulong),
                    ('ullTotalPhys', ctypes.c_ulonglong), ('ullAvailPhys', ctypes.c_ulonglong),
                    ('ullTotalPageFile', ctypes.c_ulonglong), ('ullAvailPageFile', ctypes.c_ulonglong),
                    ('ullTotalVirtual', ctypes.c_ulonglong), ('ullAvailVirtual', ctypes.c_ulonglong),
                    ('ullAvailExtendedVirtual', ctypes.c_ulonglong)]
    mem=MEMORYSTATUSEX();mem.dwLength=ctypes.sizeof(mem)
    require(bool(ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem))), 'RAM_QUERY_FAILED')
    gib=mem.ullAvailPhys/(1024**3)
    print(f'WINDOWS_FREE_RAM_GIB={gib:.2f} REQUIRED={MIN_FREE_HOST_RAM_GIB:.1f}',flush=True)
    require(gib >= MIN_FREE_HOST_RAM_GIB, 'FREE_HOST_RAM_BELOW_GOLD_SANDBOX_GATE')


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--signed-plan',required=True)
    p.add_argument('--test-public-keys',required=True)
    p.add_argument('--oracle-bundle',required=True)
    p.add_argument('--probe-bundle',required=True)
    p.add_argument('--s3-endpoint',required=True)
    p.add_argument('--evidence-root',required=True)
    p.add_argument('--measurement-id',action='append',type=int,required=True)
    p.add_argument('--approve-isolated-gold-sandbox',action='store_true')
    args=p.parse_args(argv)
    # Block BEFORE S3/PG/Spark or any evidence writes.
    require(args.approve_isolated_gold_sandbox, 'EXPLICIT_RUNTIME_APPROVAL_REQUIRED')
    endpoint=check_test_endpoint(args.s3_endpoint)
    dsn=check_test_dsn(os.environ.get('VIQ_S14_TEST_PG_DSN',''))
    access=os.environ.get('VIQ_S14_TEST_S3_ACCESS_KEY','')
    secret=os.environ.get('VIQ_S14_TEST_S3_SECRET_KEY','')
    require(bool(access and secret),'EXPLICIT_TEST_S3_CREDS_REQUIRED')
    mids=sorted(set(args.measurement_id))
    require(len(mids)==len(args.measurement_id) and 0 < len(mids)<=32 and all(0<x<2**31 for x in mids),
            'MEASUREMENT_ID_SCOPE')
    root=check_evidence_root(args.evidence_root)
    require_windows_host_ram()
    from processing.gold.verified_hour_plan import read_json, verify_hour_plan_file
    valid=verify_hour_plan_file(args.signed_plan,args.test_public_keys)
    require(valid['receipt_count']==60,'COMPLETE_TEST_HOUR_REQUIRED')
    plan=read_json(args.signed_plan)
    oracle=parse_oracle_bundle(read_json(args.oracle_bundle,1024*1024),plan['receipts'])
    probes=parse_probe_bundle(read_json(args.probe_bundle,64*1024))
    # Source independence still needs separately proven external provenance.
    require(all(safe_file(e['oracle_jsonl'],128*1024*1024) for e in oracle.values()),
            'ORACLE_FILES_REQUIRED')
    try:
        import boto3
        from botocore.config import Config
        import psycopg
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise PinnedGoldBlocked('BOTO3_PSYCOPG3_PYSPARK_REQUIRED_NO_INSTALL') from exc
    s3=boto3.client('s3',endpoint_url=endpoint,region_name='us-east-1',
                    aws_access_key_id=access, aws_secret_access_key=secret,
                    config=Config(signature_version='s3v4',connect_timeout=4,read_timeout=20,
                                  retries={'max_attempts':1},max_pool_connections=1))
    token=secrets.token_hex(8)
    dest=root/('VIQ_GOLD_PINNED_SANDBOX_'+token)
    dest.mkdir(mode=0o700,exist_ok=False)
    print('GOLD_OUTPUT_SCOPE=NEW_LOCAL_SANDBOX '+str(dest),flush=True)
    try:
        with psycopg.connect(dsn, connect_timeout=4, autocommit=True) as conn:
            conn.read_only=True
            before=require_test_registry_head(conn,plan,valid['batch_sha256'])
            print('POSTGRES_TEST_HEAD_PIN=PASS EPOCH='+str(before['epoch']),flush=True)
            scratch=dest/'VERIFIED_INPUTS';scratch.mkdir()
            paths,proofs=verify_pinned_inputs(plan,oracle,s3,scratch)
            print('S3_EXACT_VERSION_PARQUET_ORACLE=PASS OBJECTS=60',flush=True)
            mid=require_test_registry_head(conn,plan,valid['batch_sha256'])
            require(mid==before,'REGISTRY_HEAD_CHANGED_DURING_DOWNLOAD')
            # Spark starts ONLY after all 60 pinned objects have passed verification.
            spark=(SparkSession.builder.master('local[2]').appName('viq-gold-pinned-testonly')
                   .config('spark.sql.session.timeZone','UTC')
                   .config('spark.driver.memory','1g')
                   .config('spark.executor.instances','1')
                   .getOrCreate())
            results=[]
            try:
                spark.conf.set('spark.sql.sources.partitionColumnTypeInference.enabled','false')
                for country in COUNTRIES:
                    gold,metrics=gold_for_country(spark,country,paths[country],probes[country],mids,plan['hour_utc'])
                    output=dest/'GOLD_PARQUET'/('country='+country)
                    output.parent.mkdir(exist_ok=True)
                    # New path + errorifexists: never overwrite ANY Gold generation.
                    gold.write.mode('errorifexists').parquet(str(output))
                    require(spark.read.parquet(str(output)).count()==metrics['gold_rows'],
                            'GOLD_READBACK_ROW_MISMATCH')
                    results.append(metrics)
                    print(f"COUNTRY_GOLD_SANDBOX=PASS COUNTRY={country} SILVER={metrics['silver_rows']} GOLD={metrics['gold_rows']}",flush=True)
            finally:
                spark.stop()
            after=require_test_registry_head(conn,plan,valid['batch_sha256'])
            require(after==before,'REGISTRY_HEAD_CHANGED_AFTER_GOLD')
        receipt={'schema':'viq-gold-pinned-test-only-result-v1','test_only':True,
                 'hour_utc':plan['hour_utc'],'batch_sha256':valid['batch_sha256'],
                 'epoch':before['epoch'],'generation':before['generation'],
                 's3_objects':proofs,'countries':results,
                 'production_gold_published':False,'registry_production_write':False,
                 'independent_oracle_provenance_attested':False,
                 'canonical_production_registry_verified':False}
        output=dest/'PINNED_GOLD_SANDBOX_RESULT.json'
        with output.open('x',encoding='utf8',newline='\n') as f:
            json.dump(receipt,f,sort_keys=True,indent=2);f.write('\n')
        print('EVIDENCE='+str(output),flush=True)
        print('RESULT=PASS_GOLD_PINNED_SANDBOX_TEST_ONLY_NO_PRODUCTION_PUBLISH',flush=True)
    except BaseException:
        # Evidence left in uniquely named folder for analysis. No output is promoted.
        (dest/'BLOCKED_NO_PROMOTION.txt').write_text('INCOMPLETE_SANDBOX_OUTPUT_NOT_VERIFIED\n',encoding='utf8')
        raise

if __name__=='__main__':
    try:main()
    except Exception as exc:
        print('RESULT=BLOCKED_GOLD_PINNED_SANDBOX '+type(exc).__name__+':'+str(exc)[:180],file=sys.stderr)
        sys.exit(2)
