"""Build and validate the current RIPE Atlas probe dimension snapshot."""

from datetime import datetime, timezone
from typing import Any


PROBE_DIMENSION_COLUMNS = [
    "probe_id", "country_code", "asn_v4", "asn_v6", "latitude",
    "longitude", "status_id", "status_name", "description", "is_anchor",
    "is_public", "address_v4", "address_v6", "prefix_v4", "prefix_v6",
    "first_connected_utc", "last_connected_utc", "status_since_utc",
    "snapshot_at_utc", "source",
]


def probe_source_schema():
    """Return the subset of the Probe API schema used by the dimension."""
    from pyspark.sql.types import (
        ArrayType, BooleanType, DoubleType, LongType, StringType,
        StructField, StructType,
    )
    return StructType([
        StructField("id", LongType(), True),
        StructField("country_code", StringType(), True),
        StructField("asn_v4", LongType(), True),
        StructField("asn_v6", LongType(), True),
        StructField("geometry", StructType([
            StructField("coordinates", ArrayType(DoubleType()), True),
        ]), True),
        StructField("status", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]), True),
        StructField("description", StringType(), True),
        StructField("is_anchor", BooleanType(), True),
        StructField("is_public", BooleanType(), True),
        StructField("address_v4", StringType(), True),
        StructField("address_v6", StringType(), True),
        StructField("prefix_v4", StringType(), True),
        StructField("prefix_v6", StringType(), True),
        StructField("first_connected", LongType(), True),
        StructField("last_connected", LongType(), True),
        StructField("status_since", LongType(), True),
    ])


def probe_dimension_schema():
    """Return the explicit analytical schema for the Silver dimension."""
    from pyspark.sql.types import (
        BooleanType, DoubleType, IntegerType, LongType, StringType,
        StructField, StructType, TimestampType,
    )
    return StructType([
        StructField("probe_id", LongType(), True),
        StructField("country_code", StringType(), True),
        StructField("asn_v4", LongType(), True),
        StructField("asn_v6", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("status_id", IntegerType(), True),
        StructField("status_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_anchor", BooleanType(), True),
        StructField("is_public", BooleanType(), True),
        StructField("address_v4", StringType(), True),
        StructField("address_v6", StringType(), True),
        StructField("prefix_v4", StringType(), True),
        StructField("prefix_v6", StringType(), True),
        StructField("first_connected_utc", TimestampType(), True),
        StructField("last_connected_utc", TimestampType(), True),
        StructField("status_since_utc", TimestampType(), True),
        StructField("snapshot_at_utc", TimestampType(), False),
        StructField("source", StringType(), False),
    ])


def build_probe_dimension(
        spark, probes: list[dict[str, Any]],
        snapshot_at: datetime | None = None):
    """Normalize Probe API records into a typed current-snapshot DataFrame."""
    from pyspark.sql import functions as f

    snapshot = snapshot_at or datetime.now(timezone.utc)
    if snapshot.tzinfo is None or snapshot.utcoffset() is None:
        raise ValueError("snapshot_at must include a timezone")
    snapshot = snapshot.astimezone(timezone.utc)
    snapshot_utc_naive = snapshot.replace(tzinfo=None)
    raw = spark.createDataFrame(probes, schema=probe_source_schema())
    coordinates = f.col("geometry.coordinates")
    has_coordinates = coordinates.isNotNull() & (f.size(coordinates) >= 2)

    def utc_timestamp(column: str):
        return f.col(column).cast("long").cast("timestamp")

    return raw.select(
        f.col("id").cast("long").alias("probe_id"),
        f.upper(f.col("country_code")).alias("country_code"),
        f.col("asn_v4").cast("long").alias("asn_v4"),
        f.col("asn_v6").cast("long").alias("asn_v6"),
        f.when(has_coordinates, coordinates[1].cast("double")).alias(
            "latitude"
        ),
        f.when(has_coordinates, coordinates[0].cast("double")).alias(
            "longitude"
        ),
        f.col("status.id").cast("int").alias("status_id"),
        f.col("status.name").cast("string").alias("status_name"),
        f.col("description").cast("string").alias("description"),
        f.col("is_anchor").cast("boolean").alias("is_anchor"),
        f.col("is_public").cast("boolean").alias("is_public"),
        f.col("address_v4").cast("string").alias("address_v4"),
        f.col("address_v6").cast("string").alias("address_v6"),
        f.col("prefix_v4").cast("string").alias("prefix_v4"),
        f.col("prefix_v6").cast("string").alias("prefix_v6"),
        utc_timestamp("first_connected").alias("first_connected_utc"),
        utc_timestamp("last_connected").alias("last_connected_utc"),
        utc_timestamp("status_since").alias("status_since_utc"),
        f.lit(snapshot_utc_naive).cast("timestamp").alias("snapshot_at_utc"),
        f.lit("ripe_atlas").alias("source"),
    )


def validate_probe_dimension(
        df, country_code: str = "VN",
        status_name: str = "Connected") -> None:
    """Raise a clear error when core probe dimension invariants fail."""
    from pyspark.sql import functions as f

    expected_country = country_code.upper()
    errors: list[str] = []
    if df.count() == 0:
        errors.append("probe dimension is empty")
    if df.filter(f.col("probe_id").isNull() | (f.col("probe_id") <= 0)).count():
        errors.append("probe_id must be non-null and positive")
    if df.select("probe_id").distinct().count() != df.count():
        errors.append("probe_id must be unique")
    if df.filter(
            f.col("country_code").isNull()
            | (f.col("country_code") != expected_country)).count():
        errors.append(f"country_code must be {expected_country}")
    if df.filter(
            f.col("status_name").isNull()
            | (f.col("status_name") != status_name)).count():
        errors.append(f"status_name must be {status_name}")
    if df.filter(
            f.col("latitude").isNotNull()
            & ~f.col("latitude").between(-90, 90)).count():
        errors.append("latitude is outside [-90, 90]")
    if df.filter(
            f.col("longitude").isNotNull()
            & ~f.col("longitude").between(-180, 180)).count():
        errors.append("longitude is outside [-180, 180]")
    if df.filter(f.col("snapshot_at_utc").isNull()).count():
        errors.append("snapshot_at_utc must be non-null")
    if errors:
        raise ValueError("Invalid probe dimension: " + "; ".join(errors))


def probe_dimension_summary(df) -> dict[str, int]:
    """Collect compact data-quality metrics for logging."""
    from pyspark.sql import functions as f

    row = df.agg(
        f.count("*").alias("probes"),
        f.countDistinct("probe_id").alias("distinct_probe_ids"),
        f.countDistinct("asn_v4").alias("distinct_asn_v4"),
        f.sum(f.col("asn_v4").isNull().cast("int")).alias("missing_asn_v4"),
        f.sum(f.col("asn_v6").isNull().cast("int")).alias("missing_asn_v6"),
        f.sum(
            (f.col("latitude").isNull() | f.col("longitude").isNull())
            .cast("int")
        ).alias("missing_coordinates"),
        f.sum(f.coalesce(f.col("is_anchor"), f.lit(False)).cast("int")).alias(
            "anchors"
        ),
    ).first()
    return {name: int(row[name] or 0) for name in row.asDict()}
