"""Actual S3 VersionId byte reader + PATCH02 independent Parquet verification.

FAIL-CLOSED. Restricted to stage14 TEST buckets. This does not create a production
signer or prove the oracle was obtained independently. No write to S3.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import tempfile

BUCKET = re.compile(r"viq-patch03-test-[0-9a-f]{12}\Z")
COUNTRIES = {"VN", "DE", "SG", "JP", "US"}
MAX_OBJECT_BYTES = 64 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024


class SandboxBlocked(ValueError):
    pass


def require(ok, msg):
    if not ok:
        raise SandboxBlocked(msg)


def validate_ref(ref):
    expected = {'country', 'window_index', 'generation', 'bucket', 'key',
                'version_id', 'byte_sha256', 'schema_sha256',
                'row_multiset_sha256', 'row_count', 'byte_count'}
    require(type(ref) is dict and set(ref) == expected, 'OBJECT_FIELDS')
    require(ref['country'] in COUNTRIES, 'COUNTRY_SCOPE')
    require(type(ref['window_index']) is int and 0 <= ref['window_index'] < 12, 'WINDOW_SCOPE')
    require(type(ref['bucket']) is str and BUCKET.fullmatch(ref['bucket']), 'TEST_BUCKET_REQUIRED')
    require(type(ref['generation']) is str and re.fullmatch(r'[0-9a-f]{32}', ref['generation']), 'GENERATION_SCOPE')
    require(type(ref['key']) is str and ref['key'].startswith('stage14/candidate/')
            and '\\' not in ref['key'] and '..' not in ref['key'].split('/'), 'OBJECT_KEY_SCOPE')
    require(type(ref['version_id']) is str and 1 <= len(ref['version_id']) <= 128
            and ref['version_id'] != 'null' and not any(ch.isspace() for ch in ref['version_id']),
            'VERSION_ID_REQUIRED')
    for field in ('byte_sha256', 'schema_sha256', 'row_multiset_sha256'):
        require(type(ref[field]) is str and re.fullmatch(r'[0-9a-f]{64}', ref[field]), 'DIGEST_FORMAT')
    require(type(ref['byte_count']) is int and 8 < ref['byte_count'] <= MAX_OBJECT_BYTES, 'SIZE_SCOPE')
    require(type(ref['row_count']) is int and 0 < ref['row_count'] <= 250000, 'ROWS_SCOPE')


def download_exact_version(client, ref, output_file):
    """Bounded download. HEAD and GET must return the exact requested S3 VersionId."""
    validate_ref(ref)
    bucket, key, version = ref['bucket'], ref['key'], ref['version_id']
    h = client.head_object(Bucket=bucket, Key=key, VersionId=version)
    require(h.get('VersionId') == version, 'HEAD_VERSION_MISMATCH')
    require(h.get('ContentLength') == ref['byte_count'], 'HEAD_SIZE_MISMATCH')
    g = client.get_object(Bucket=bucket, Key=key, VersionId=version)
    body = g['Body']
    hasher = hashlib.sha256(); count = 0; start = b''; end = b''
    try:
        require(g.get('VersionId') == version, 'GET_VERSION_MISMATCH')
        require(g.get('ContentLength') == ref['byte_count'], 'GET_SIZE_MISMATCH')
        with open(output_file, 'xb') as out:
            while True:
                chunk = body.read(CHUNK_BYTES)
                if not chunk: break
                require(type(chunk) is bytes, 'INVALID_RESPONSE_BYTES')
                count += len(chunk)
                require(count <= ref['byte_count'], 'BYTE_LIMIT_EXCEEDED')
                hasher.update(chunk)
                if len(start) < 4: start = (start + chunk)[:4]
                end = (end + chunk)[-4:]
                out.write(chunk)
            out.flush(); os.fsync(out.fileno())
    finally:
        body.close()
    require(count == ref['byte_count'], 'BODY_LENGTH_MISMATCH')
    require(hasher.hexdigest() == ref['byte_sha256'], 'CONTENT_SHA256_MISMATCH')
    require(start == b'PAR1' and end == b'PAR1', 'NOT_PARQUET')
    return {'version_id': version, 'byte_count': count, 'byte_sha256': hasher.hexdigest()}


def verify_s3_parquet_with_independent_oracle(client, ref, oracle_jsonl, columns, arrow_types,
                                               expected_schema_digest, local_verifier=None):
    """Actual VersionId GET -> local PATCH02 Parquet verifier -> exact digest check.

    TEST-ONLY: separate provenance attestation of oracle is still required.
    local_verifier may be injected for unit testing; default loads installed PATCH02.
    """
    validate_ref(ref)
    require(type(columns) is list and type(arrow_types) is list and len(columns)>0, 'EXPLICIT_SCHEMA_REQUIRED')
    from json import dumps
    schema = {'physical_columns': columns, 'physical_types': arrow_types}
    schema_hash = hashlib.sha256(dumps(schema, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
    require(schema_hash == expected_schema_digest == ref['schema_sha256'], 'SCHEMA_DIGEST_MISMATCH')
    if local_verifier is None:
        from processing.silver.parquet_independent_verifier import verify_files as local_verifier
    with tempfile.TemporaryDirectory(prefix='viq_patch04_testonly_') as d:
        p = Path(d) / 'pinned.parquet'
        exact = download_exact_version(client, ref, p)
        result = local_verifier([str(p)], oracle_jsonl, columns, arrow_types)
        require(result['row_count'] == ref['row_count'], 'PARQUET_ROW_COUNT_MISMATCH')
        require(result['row_multiset_sha256'] == ref['row_multiset_sha256'], 'PARQUET_ROW_DIGEST_MISMATCH')
        require(result['files'][0]['file_sha256'] == ref['byte_sha256'], 'PARQUET_FILE_DIGEST_MISMATCH')
        return {'test_only': True, 's3_exact_version_verified': True,
                'independent_parquet_rows_verified': True, 'oracle_provenance_attested': False,
                **exact, 'row_count': result['row_count'],
                'row_multiset_sha256': result['row_multiset_sha256']}
