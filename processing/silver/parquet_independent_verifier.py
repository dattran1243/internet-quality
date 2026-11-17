"""Local, read-only independent Parquet row-multiset check for VIQ Silver.

An external JSONL file is the oracle. This tool does NOT authenticate its origin,
does NOT verify S3 VersionId or Registry head, and never publishes results.

Spark partitionBy() stores partition values in paths, not physical Parquet schema.
The oracle and --columns here describe physical Parquet columns only; verifying
partition directory/lineage, source provenance, S3 versions and CAS is separate.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

MAX_FILES = 256
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_TOTAL_BYTES = 512 * 1024 * 1024
MAX_ORACLE_BYTES = 128 * 1024 * 1024
MAX_ROWS = 250000
MAX_LINE_BYTES = 1024 * 1024
DOMAIN = b'VIQ-SILVER-LOCAL-ROW-MULTISET-V1\0'


class VerificationBlocked(ValueError):
    pass


def require(ok: bool, code: str):
    if not ok:
        raise VerificationBlocked(code)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def is_symlink_or_reparse(path: Path) -> bool:
    try:
        st = path.lstat()
    except OSError:
        return True
    return path.is_symlink() or bool(getattr(st, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400))


def regular_file(path: Path, max_bytes: int) -> int:
    require(not is_symlink_or_reparse(path) and path.is_file(), 'REGULAR_FILE_REQUIRED')
    st = path.stat()
    require(0 < st.st_size <= max_bytes, 'INPUT_SIZE_LIMIT')
    return st.st_size


def json_no_duplicate_keys(pairs):
    out = {}
    for key, value in pairs:
        require(key not in out, 'DUPLICATE_JSON_KEY')
        out[key] = value
    return out


def finite_number(value):
    require(math.isfinite(value), 'NONFINITE_FLOAT')
    return 0.0 if value == 0 else value


def normalize(value):
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        return finite_number(value)
    if isinstance(value, Decimal):
        require(value.is_finite(), 'NONFINITE_DECIMAL')
        return format(value.normalize(), 'f')
    if isinstance(value, datetime):
        require(value.tzinfo is not None and value.utcoffset() is not None,
                'NAIVE_PARQUET_TIMESTAMP_REQUIRES_EXPLICIT_CONTRACT')
        return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
    raise VerificationBlocked('UNSUPPORTED_CELL_TYPE')


def canonical_row(row: dict, columns: tuple[str, ...]) -> bytes:
    require(isinstance(row, dict), 'ROW_MUST_BE_OBJECT')
    require(set(row) == set(columns), 'ROW_COLUMNS_MISMATCH')
    normalized = {key: normalize(row[key]) for key in columns}
    return json.dumps(normalized, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def add_row(counter: Counter, row: dict, columns: tuple[str, ...], tally: int) -> int:
    tally += 1
    require(tally <= MAX_ROWS, 'ROWS_LIMIT')
    counter[digest(canonical_row(row, columns))] += 1
    return tally


def summarize(counter: Counter, columns: tuple[str, ...], rows: int) -> dict:
    h = hashlib.sha256()
    h.update(DOMAIN)
    h.update(json.dumps(columns, separators=(',', ':'), ensure_ascii=False).encode('utf-8'))
    h.update(b'\0')
    for key in sorted(counter):
        h.update(bytes.fromhex(key))
        h.update(int(counter[key]).to_bytes(8, 'big'))
    return {'row_count': rows, 'row_multiset_sha256': h.hexdigest()}


def read_oracle(path: Path, columns: tuple[str, ...]) -> dict:
    regular_file(path, MAX_ORACLE_BYTES)
    counter = Counter(); n = 0
    with path.open('rb') as f:
        for raw in f:
            require(len(raw) <= MAX_LINE_BYTES, 'ORACLE_LINE_TOO_LONG')
            require(raw.endswith(b'\n'), 'ORACLE_MUST_END_EACH_ROW_WITH_NEWLINE')
            try:
                row=json.loads(raw.decode('utf-8'), object_pairs_hook=json_no_duplicate_keys,
                               parse_constant=lambda _: (_ for _ in ()).throw(VerificationBlocked('NONFINITE_JSON')))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise VerificationBlocked('INVALID_JSONL') from exc
            n = add_row(counter,row,columns,n)
    require(n > 0, 'EMPTY_ORACLE')
    return summarize(counter,columns,n)


def hash_file(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024),b''):
            h.update(chunk)
    return h.hexdigest()


def read_parquet(files: list[Path], columns: tuple[str, ...], types: tuple[str, ...]):
    require(0 < len(files) <= MAX_FILES, 'PARQUET_FILE_COUNT')
    require(len(set(map(str,files))) == len(files), 'DUPLICATE_PARQUET_FILE')
    require(len(types)==len(columns), 'SCHEMA_TYPES_REQUIRED')
    total_bytes = sum(regular_file(p,MAX_FILE_BYTES) for p in files)
    require(total_bytes <= MAX_TOTAL_BYTES, 'PARQUET_TOTAL_BYTES_LIMIT')
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise VerificationBlocked('PYARROW_REQUIRED_FOR_REAL_PARQUET') from exc
    counter=Counter(); n=0; blobs=[]
    try:
        for file in sorted(files, key=lambda x: str(x).lower()):
            with file.open('rb') as f:
                require(f.read(4)==b'PAR1', 'PARQUET_MAGIC_HEADER')
                f.seek(-4,os.SEEK_END)
                require(f.read(4)==b'PAR1', 'PARQUET_MAGIC_FOOTER')
            parquet=pq.ParquetFile(str(file))
            schema=parquet.schema_arrow
            require(tuple(schema.names)==columns, 'PHYSICAL_PARQUET_COLUMNS_MISMATCH')
            require(tuple(str(field.type) for field in schema)==types,
                    'PHYSICAL_PARQUET_TYPES_MISMATCH')
            require(n+parquet.metadata.num_rows<=MAX_ROWS, 'PARQUET_META_ROW_LIMIT')
            for batch in parquet.iter_batches(batch_size=512,use_threads=False):
                for row in batch.to_pylist():
                    n=add_row(counter,row,columns,n)
            blobs.append({'name':file.name, 'file_size':file.stat().st_size,
                          'file_sha256':hash_file(file)})
    except VerificationBlocked:
        raise
    except Exception as exc:
        raise VerificationBlocked('PARQUET_DECODE_ERROR') from exc
    require(n>0,'EMPTY_PARQUET')
    return summarize(counter,columns,n),blobs


def verify_files(parquet_paths, oracle_jsonl, columns, arrow_types):
    cols=tuple(columns); types=tuple(arrow_types)
    require(bool(cols) and len(cols)==len(set(cols)) and
            all(isinstance(c,str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',c) for c in cols),
            'EXPLICIT_VALID_COLUMNS_REQUIRED')
    require(len(types)==len(cols) and all(isinstance(x,str) and x for x in types),
            'EXPLICIT_ARROW_TYPES_REQUIRED')
    parquet=[Path(p) for p in parquet_paths]
    oracle=Path(oracle_jsonl)
    require(all(p.suffix.lower()=='.parquet' for p in parquet), 'PARQUET_ONLY')
    require(oracle.resolve() not in {p.resolve() for p in parquet}, 'ORACLE_CANNOT_BE_PARQUET')
    source=read_oracle(oracle,cols)
    observed,files=read_parquet(parquet,cols,types)
    require(source==observed,'ROW_MULTISET_OR_COUNT_MISMATCH')
    return {'result':'PASS_LOCAL_PARQUET_ROW_MULTISET', 'oracle_provenance':'UNATTESTED_EXTERNAL_JSONL',
            'verified_local_parquet':True,'verified_s3_version':False,
            'registry_v2_committed':False,'production_ready':False,
            'physical_columns':list(cols),'physical_types':list(types),
            **observed,'files':files}
