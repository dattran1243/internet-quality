"""Read-only, test-only verified-input plan preflight for the existing Gold CLI.

Does NOT fetch S3, read a PostgreSQL head, publish Gold, or authorize release.
An ED25519 signature from an ephemeral TEST key authenticates only this fixture;
it does not prove independent provenance or production signer trust.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
import stat
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = 'viq-patch03-gold-input-plan-test-v1'
DOMAIN = b'VIQ-PATCH03-TEST-ONLY-GOLD-HOUR-PLAN\0'
COUNTRIES = ('DE', 'JP', 'SG', 'US', 'VN')
HEX = re.compile(r'[0-9a-f]{64}\Z')
ID = re.compile(r'[0-9a-f]{12}\Z')
GEN = re.compile(r'[0-9a-f]{32}\Z')
BUCKET = re.compile(r'viq-patch03-test-[0-9a-f]{12}\Z')
MAX_PLAN = 256_000

class PlanBlocked(ValueError):
    pass

def ensure(ok, code):
    if not ok:
        raise PlanBlocked(code)

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def canonical(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf8')

def unique(pairs):
    out = {}
    for k, v in pairs:
        ensure(k not in out, 'DUPLICATE_JSON_KEY')
        out[k] = v
    return out

def reject_constant(_):
    raise PlanBlocked('NONFINITE_JSON')

def read_json(path, max_bytes=MAX_PLAN):
    p = Path(path)
    try:
        st = p.lstat()
        ensure(p.is_file() and not p.is_symlink() and not (getattr(st, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400)), 'FILE_NOT_REGULAR')
        ensure(0 < st.st_size <= max_bytes, 'FILE_SIZE_SCOPE')
        raw = p.read_bytes()
        ensure(len(raw) <= max_bytes, 'FILE_SIZE_SCOPE')
        return json.loads(raw.decode('utf8'), object_pairs_hook=unique, parse_constant=reject_constant)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PlanBlocked('FILE_OR_JSON_INVALID') from exc

def validate_plan(plan):
    ensure(type(plan) is dict and set(plan) == {'schema','test_only','namespace','dataset','hour_utc','countries','epoch','registry_head','receipts','signer_key_id','signature_b64'}, 'PLAN_FIELDS')
    ensure(plan['schema'] == SCHEMA and plan['test_only'] is True, 'TEST_ONLY_SCOPE')
    ensure(isinstance(plan['namespace'], str) and re.fullmatch(r'patch03_[0-9a-f]{12}', plan['namespace']), 'NAMESPACE_SCOPE')
    ensure(plan['dataset'] == 'ping_by_country', 'DATASET_SCOPE')
    h = plan['hour_utc']
    ensure(type(h) is str and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:00:00Z', h), 'UTC_HOUR_FORMAT')
    try:
        dt = datetime.strptime(h, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise PlanBlocked('UTC_HOUR_VALUE') from exc
    ensure(plan['countries'] == list(COUNTRIES), 'COUNTRIES_SCOPE')
    ensure(type(plan['epoch']) is int and 1 <= plan['epoch'] <= 2_000_000_000, 'EPOCH_SCOPE')
    receipts = plan['receipts']
    ensure(type(receipts) is list and len(receipts) == 60, 'EXPECTED_60_SILVER_WINDOWS')
    seen, object_versions = set(), set()
    fields = {'country','window_index','generation','bucket','key','version_id','byte_sha256','schema_sha256','row_multiset_sha256','row_count','byte_count'}
    for r in receipts:
        ensure(type(r) is dict and set(r) == fields, 'RECEIPT_FIELDS')
        c, w = r['country'], r['window_index']
        ensure(c in COUNTRIES and type(w) is int and 0 <= w < 12, 'WINDOW_COUNTRY_SCOPE')
        ensure((c,w) not in seen, 'DUPLICATE_WINDOW')
        seen.add((c,w))
        ensure(type(r['generation']) is str and GEN.fullmatch(r['generation']), 'GENERATION_SCOPE')
        ensure(type(r['bucket']) is str and BUCKET.fullmatch(r['bucket']), 'BUCKET_SCOPE')
        key = r['key']
        ensure(type(key) is str and key.startswith('stage14/candidate/') and '..' not in key and '\\' not in key, 'KEY_SCOPE')
        segments = key.split('/')
        expect = {'dataset=ping_by_country',f'country={c}',f'hour={dt:%Y%m%d%H}',f'window={w*5:02d}', f'generation={r["generation"]}'}
        ensure(expect.issubset(set(segments)), 'KEY_LINEAGE_MISMATCH')
        ensure(type(r['version_id']) is str and 0 < len(r['version_id']) < 128 and r['version_id'] != 'null' and not any(ch.isspace() for ch in r['version_id']), 'VERSION_ID_REQUIRED')
        ensure((r['bucket'],key,r['version_id']) not in object_versions, 'DUPLICATE_VERSIONED_OBJECT')
        object_versions.add((r['bucket'],key,r['version_id']))
        for field in ('byte_sha256','schema_sha256','row_multiset_sha256'):
            ensure(type(r[field]) is str and HEX.fullmatch(r[field]), 'SHA256_FORMAT')
        ensure(type(r['row_count']) is int and 0 < r['row_count'] <= 250_000, 'ROW_COUNT_SCOPE')
        ensure(type(r['byte_count']) is int and 8 < r['byte_count'] <= 64*1024*1024, 'BYTE_COUNT_SCOPE')
    ensure(seen == {(c,w) for c in COUNTRIES for w in range(12)}, 'INCOMPLETE_HOUR')
    ordered = sorted(receipts, key=lambda item:(item['country'], item['window_index']))
    digest = sha(canonical(ordered))
    head = plan['registry_head']
    ensure(type(head) is dict and set(head) == {'epoch','batch_sha256','source','test_only'}, 'HEAD_FIELDS')
    ensure(head == {'epoch':plan['epoch'],'batch_sha256':digest,'source':'offline-test-only','test_only':True},'HEAD_NOT_BOUND_TO_RECEIPTS')
    ensure(type(plan['signer_key_id']) is str and re.fullmatch(r'patch03-test-[0-9a-f]{12}', plan['signer_key_id']), 'TEST_KEY_SCOPE')
    ensure(type(plan['signature_b64']) is str, 'SIGNATURE_TYPE')
    return digest

def verify_hour_plan_file(plan_path, keys_path, expected_hour=None, local_parquet_bundle=None):
    plan = read_json(plan_path)
    digest = validate_plan(plan)
    if expected_hour is not None:
        ensure(plan['hour_utc'] == expected_hour, 'HOUR_MISMATCH')
    keys = read_json(keys_path, 8192)
    ensure(type(keys) is dict and set(keys) == {plan['signer_key_id']}, 'TEST_KEY_ALLOWLIST_SCOPE')
    try:
        key_bytes = base64.b64decode(keys[plan['signer_key_id']], validate=True)
        signature = base64.b64decode(plan['signature_b64'], validate=True)
    except (TypeError, ValueError) as exc:
        raise PlanBlocked('INVALID_BASE64_KEY_OR_SIGNATURE') from exc
    ensure(len(key_bytes) == 32 and len(signature) == 64, 'ED25519_KEY_OR_SIGNATURE_SIZE')
    unsigned = {k:v for k,v in plan.items() if k != 'signature_b64'}
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        Ed25519PublicKey.from_public_bytes(key_bytes).verify(signature, DOMAIN + canonical(unsigned))
    except ImportError as exc:
        raise PlanBlocked('CRYPTOGRAPHY_REQUIRED') from exc
    except Exception as exc:
        raise PlanBlocked('BAD_ED25519_SIGNATURE') from exc
    local_proof = verify_local_parquet_bundle(plan,local_parquet_bundle) if local_parquet_bundle else {'local_parquet_verified':False}
    return {'result':'PASS_TEST_ONLY_GOLD_INPUT_PLAN_PREFLIGHT', 'hour_utc':plan['hour_utc'], 'receipt_count':60,
            'countries':list(COUNTRIES),'batch_sha256':digest,'epoch':plan['epoch'],
            's3_versioned_bytes_verified':False,'postgres_head_read':False,
            'independent_parquet_verified':False,'gold_runtime_authorized':False,
            'production_ready':False, **local_proof}

def verify_local_parquet_bundle(plan, manifest_path):
    """Optionally prove LOCAL Parquet row multiset for all 60 windows.

    Oracle source independence and S3 object VersionId remain UNATTESTED. Each
    window must have exactly one physical Parquet file and explicit Arrow types.
    """
    manifest = read_json(manifest_path, 64_000)
    ensure(type(manifest) is dict and set(manifest) == {'schema','entries'}, 'LOCAL_BUNDLE_FIELDS')
    ensure(manifest['schema'] == 'viq-patch03-local-parquet-bundle-v1', 'LOCAL_BUNDLE_SCHEMA')
    entries = manifest['entries']
    ensure(type(entries) is list and len(entries) == 60, 'LOCAL_BUNDLE_COUNT')
    lookup = {}
    for entry in entries:
        ensure(type(entry) is dict and set(entry) == {'country','window_index','parquet','oracle_jsonl','columns','arrow_types'}, 'LOCAL_ENTRY_FIELDS')
        identity=(entry['country'],entry['window_index'])
        ensure(identity not in lookup, 'DUPLICATE_LOCAL_ENTRY')
        lookup[identity]=entry
    ensure(set(lookup) == {(c,w) for c in COUNTRIES for w in range(12)}, 'LOCAL_ENTRIES_INCOMPLETE')
    try:
        from processing.silver.parquet_independent_verifier import verify_files
    except ImportError as exc:
        raise PlanBlocked('PATCH02_VERIFIER_NOT_INSTALLED') from exc
    for ref in plan['receipts']:
        entry=lookup[(ref['country'],ref['window_index'])]
        ensure(type(entry['parquet']) is str and type(entry['oracle_jsonl']) is str, 'LOCAL_PATH_FIELDS')
        ensure(type(entry['columns']) is list and type(entry['arrow_types']) is list, 'LOCAL_SCHEMA_FIELDS')
        result=verify_files([entry['parquet']], entry['oracle_jsonl'], entry['columns'], entry['arrow_types'])
        ensure(result['row_count'] == ref['row_count'] and result['row_multiset_sha256'] == ref['row_multiset_sha256'], 'LOCAL_PARQUET_ORACLE_MISMATCH')
        ensure(result['files'][0]['file_sha256'] == ref['byte_sha256'] and result['files'][0]['file_size'] == ref['byte_count'], 'LOCAL_PARQUET_BYTES_MISMATCH')
        schema_digest=sha(canonical({'physical_columns':entry['columns'], 'physical_types':entry['arrow_types']}))
        ensure(schema_digest == ref['schema_sha256'], 'LOCAL_PARQUET_SCHEMA_DIGEST_MISMATCH')
    return {'local_parquet_verified':True,'local_parquet_count':60,
            'independent_oracle_provenance_attested':False,'s3_object_versions_verified':False}
