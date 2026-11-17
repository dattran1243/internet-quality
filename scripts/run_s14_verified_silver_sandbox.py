#!/usr/bin/env python3
"""Stage14 PATCH04: genuine S3 VersionId+Parquet oracle and PG test-CAS bridge.

DOES NOT affect the production Gold DAG, Registry, S3 objects or ClickHouse.
Network and PostgreSQL test-write require explicit opt-in flags.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.registry.verified_s3_sandbox import verify_s3_parquet_with_independent_oracle, SandboxBlocked
from processing.registry.postgres_sandbox import compare_and_publish_test_only


def guarded_endpoint(s):
    parts = urlsplit(s)
    if parts.scheme != 'http' or parts.hostname != '127.0.0.1' or not 1 <= (parts.port or 0) <= 65535 or parts.username or parts.password or parts.path not in ('', '/'):
        raise SandboxBlocked('LOOPBACK_HTTP_ENDPOINT_REQUIRED')
    return s


def guarded_database_dsn(dsn):
    # Avoid printing DSN. Parsed connection passed to PostgreSQL with server-side DB guard.
    u = urlsplit(dsn)
    if u.scheme not in ('postgresql', 'postgres') or u.hostname != '127.0.0.1' or not 1 <= (u.port or 0) <= 65535:
        raise SandboxBlocked('LOOPBACK_POSTGRES_DSN_REQUIRED')
    if re.fullmatch(r'/viq_s14_test_[a-f0-9]{12}', u.path) is None:
        raise SandboxBlocked('ISOLATED_POSTGRES_DATABASE_REQUIRED')
    if u.query or u.fragment: raise SandboxBlocked('POSTGRES_DSN_OPTIONS_BLOCKED')
    return u.path.lstrip('/')


def verified_batch(plan, oracle_bundle, client, *, verifier=verify_s3_parquet_with_independent_oracle):
    entries = oracle_bundle.get('entries')
    if oracle_bundle.get('schema') != 'viq-patch04-independent-oracle-v1' or type(entries) is not list or len(entries) != 60:
        raise SandboxBlocked('EXACT_60_INDEPENDENT_ORACLE_ENTRIES')
    lookup = {}
    for entry in entries:
        if type(entry) is not dict or set(entry) != {'country','window_index','oracle_jsonl','columns','arrow_types'}:
            raise SandboxBlocked('ORACLE_ENTRY_FIELDS')
        pair = (entry['country'],entry['window_index'])
        if pair in lookup: raise SandboxBlocked('DUPLICATE_ORACLE_ENTRY')
        lookup[pair] = entry
    if len(plan['receipts']) != 60: raise SandboxBlocked('EXACT_60_PINNED_RECEIPTS')
    expected = {(r['country'],r['window_index']) for r in plan['receipts']}
    if set(lookup) != expected or len(expected) != 60: raise SandboxBlocked('ORACLE_RECEIPT_SET_MISMATCH')
    verified = []
    budget = 512*1024*1024
    for r in sorted(plan['receipts'],key=lambda x:(x['country'],x['window_index'])):
        budget -= r['byte_count']
        if budget < 0: raise SandboxBlocked('AGGREGATE_BYTE_BUDGET')
        e = lookup[(r['country'],r['window_index'])]
        proof = verifier(client,r,e['oracle_jsonl'],e['columns'],e['arrow_types'],r['schema_sha256'])
        if proof.get('s3_exact_version_verified') is not True or proof.get('independent_parquet_rows_verified') is not True:
            raise SandboxBlocked('S3_AND_PARQUET_PROOF_REQUIRED')
        verified.append((r['country'],r['window_index'],r['byte_sha256'],r['version_id']))
    # Bind to already signed PATCH03 plan batch digest, but not a production trust anchor.
    signature_digest = plan['registry_head']['batch_sha256']
    return {'namespace':'s14test_'+plan['namespace'].split('_',1)[1],
            'hour_utc':plan['hour_utc'], 'digest':signature_digest,
            'generation':hashlib.sha256((signature_digest+plan['hour_utc']).encode('ascii')).hexdigest()[:32],
            'country_count':5,'window_count':len(verified),'test_only':True}


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--signed-plan',required=True)
    p.add_argument('--test-public-keys',required=True)
    p.add_argument('--oracle-bundle',required=True)
    p.add_argument('--s3-endpoint',required=True)
    p.add_argument('--approve-isolated-runtime',action='store_true')
    p.add_argument('--approve-test-pg-write',action='store_true')
    p.add_argument('--expected-epoch',type=int,default=0)
    a=p.parse_args(argv)
    if not a.approve_isolated_runtime: raise SandboxBlocked('ISOLATED_RUNTIME_APPROVAL_REQUIRED')
    endpoint=guarded_endpoint(a.s3_endpoint)
    dsn=os.environ.get('VIQ_S14_TEST_PG_DSN','')
    if a.approve_test_pg_write:
        db=guarded_database_dsn(dsn)
    else: db=None
    from processing.gold.verified_hour_plan import read_json, verify_hour_plan_file
    preflight=verify_hour_plan_file(a.signed_plan,a.test_public_keys)
    if preflight['receipt_count']!=60: raise SandboxBlocked('PLAN_INCOMPLETE')
    plan=read_json(a.signed_plan)
    oracle=read_json(a.oracle_bundle,1024*1024)
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise SandboxBlocked('BOTO3_REQUIRED_NO_AUTO_INSTALL') from exc
    # Credentials are environment variables, never printed or persisted.
    access=os.environ.get('VIQ_S14_TEST_S3_ACCESS_KEY')
    secret=os.environ.get('VIQ_S14_TEST_S3_SECRET_KEY')
    if not access or not secret: raise SandboxBlocked('EXPLICIT_TEST_S3_CREDENTIALS_REQUIRED')
    s3=boto3.client('s3',endpoint_url=endpoint,region_name='us-east-1',
                    aws_access_key_id=access,
                    aws_secret_access_key=secret,
                    config=Config(signature_version='s3v4',connect_timeout=4,read_timeout=15,
                                  retries={'max_attempts':1},max_pool_connections=1))
    batch=verified_batch(plan,oracle,s3)
    print('S3_EXACT_VERSION_AND_REAL_PARQUET=PASS OBJECTS=60 TEST_ONLY=True')
    if not a.approve_test_pg_write:
        print('POSTGRES_CAS=NOT_ATTEMPTED NO_TEST_WRITE_APPROVAL')
        print('RESULT=PASS_STAGE14_PATCH04_S3_READONLY_VERIFICATION_ONLY')
        return
    try:
        import psycopg
    except ImportError as exc:
        raise SandboxBlocked('PSYCOPG3_REQUIRED_NO_AUTO_INSTALL') from exc
    with psycopg.connect(dsn,connect_timeout=4,autocommit=False) as conn:
        result=compare_and_publish_test_only(conn,db,batch,a.expected_epoch)
    print('POSTGRES_TEST_ONLY_CAS='+result['status']+' EPOCH='+str(result['epoch']))
    print('RESULT=PASS_STAGE14_PATCH04_TEST_ONLY_S3_PG_NO_GOLD_PUBLISH')

if __name__=='__main__':
    try: main()
    except Exception as exc:
        print('RESULT=BLOCKED_STAGE14_PATCH04 '+type(exc).__name__+':'+str(exc)[:180],file=sys.stderr)
        sys.exit(2)
