"""VIQ staging-only Silver Parquet generation writer.

This is NOT a verified generation, receipt, S3 immutable proof or Registry commit.
It writes exclusively to a strict test bucket and never creates _SUCCESS.json.
"""
from __future__ import annotations

import re
from datetime import timedelta

COUNTRIES = frozenset({"VN", "DE", "SG", "JP", "US"})
BUCKET = re.compile(r"viq-s14-test-[0-9a-f]{12}\Z", re.ASCII)
GENERATION = re.compile(r"[0-9a-f]{32}\Z", re.ASCII)
PARTITION_COLUMNS = ("year", "month", "day", "hour")


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ValueError(code)


def candidate_path(bucket: str, country: str, window, generation: str) -> str:
    """Return a pinned test bucket path; reject ambiguous/timezone-less windows."""
    _require(isinstance(bucket, str) and BUCKET.fullmatch(bucket) is not None,
             "TEST_BUCKET_REQUIRED")
    _require(isinstance(country, str) and country in COUNTRIES, "COUNTRY_SCOPE")
    _require(isinstance(generation, str) and GENERATION.fullmatch(generation) is not None,
             "GENERATION_MUST_BE_32_LOWER_HEX")
    start, stop = window.start, window.stop
    for dt in (start, stop):
        _require(dt.tzinfo is not None and dt.utcoffset() == timedelta(0),
                 "EXACT_UTC_WINDOW_REQUIRED")
        _require(dt.second == dt.microsecond == 0 and dt.minute % 5 == 0,
                 "FIVE_MINUTE_ALIGNMENT")
    _require(stop - start == timedelta(minutes=5), "FIVE_MINUTE_DURATION")
    return (
        f"s3a://{bucket}/stage14/candidate/"
        "dataset=ping_by_country/"
        f"country={country}/"
        f"hour={start:%Y%m%d%H}/"
        f"window={start:%M}/"
        f"generation={generation}"
    )


def write_candidate_generation(
    silver_df, bucket: str, country: str, window, generation: str,
    measurement_ids,
) -> str:
    """Write a test-only candidate, fail if path exists; never claim S3 immutability.

    A successful write is NOT verified: a separate oracle must validate Parquet
    bytes/rows, S3 object versions, signature and Registry CAS before promotion.
    """
    target = candidate_path(bucket, country, window, generation)
    ids = tuple(sorted(set(measurement_ids)))
    _require(bool(ids) and all(type(x) is int and x > 0 for x in ids),
             "MEASUREMENT_IDS_SCOPE")

    from pyspark.sql import functions as F

    required = {"measurement_id", "country_code", "event_time_utc", *PARTITION_COLUMNS}
    _require(required.issubset(set(silver_df.columns)), "SILVER_COLUMNS_MISSING")
    _require(silver_df.limit(1).count() > 0, "EMPTY_CANDIDATE")

    out_of_scope = (
        F.col("country_code").isNull()
        | (F.col("country_code") != F.lit(country))
        | F.col("measurement_id").isNull()
        | (~F.col("measurement_id").isin(list(ids)))
        | F.col("event_time_utc").isNull()
        | (F.col("event_time_utc") < F.lit(window.start))
        | (F.col("event_time_utc") >= F.lit(window.stop))
    )
    _require(silver_df.filter(out_of_scope).limit(1).count() == 0,
             "OUT_OF_SCOPE_SILVER_ROWS")

    # 'errorifexists' is a best-effort Spark/Hadoop existence guard. It is NOT
    # S3 conditional object creation, ObjectLock, or a transactional CAS.
    (
        silver_df.write
        .mode("errorifexists")
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(target)
    )
    return target
