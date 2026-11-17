"""Helpers for safe incremental replacement of Silver partitions."""

from datetime import timedelta
from typing import Iterable

from ingestion.ripe_atlas.models import (
    TimeWindow,
    utc_text,
)


PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
)


def validate_hour_aligned_window(
    window: TimeWindow,
) -> None:
    """
    Legacy validation for jobs that still require whole UTC hours.

    Kept for backward compatibility with existing hourly jobs.
    """

    for name, value in (
        ("start", window.start),
        ("stop", window.stop),
    ):
        if any(
            (
                value.minute,
                value.second,
                value.microsecond,
            )
        ):
            raise ValueError(
                f"{name} must be aligned to an exact UTC hour"
            )


def validate_five_minute_window(
    window: TimeWindow,
) -> None:
    """Require exactly one UTC-aligned five-minute window."""

    duration_seconds = (
        window.stop
        - window.start
    ).total_seconds()

    if duration_seconds != 300:
        raise ValueError(
            "Silver microbatch must be exactly "
            f"5 minutes; got {duration_seconds} seconds"
        )

    for name, value in (
        ("start", window.start),
        ("stop", window.stop),
    ):
        if (
            value.second != 0
            or value.microsecond != 0
            or value.minute % 5 != 0
        ):
            raise ValueError(
                f"{name} must align to an exact "
                "5-minute UTC boundary"
            )


def bronze_window_glob(
    bucket: str,
    window: TimeWindow,
) -> str:
    """
    Legacy RIPE Bronze path.

    Kept because scripts/run_silver.py still uses the
    original non-country Bronze layout.
    """

    start = window.start

    return (
        f"s3a://{bucket}/bronze/ripe_atlas/ping/"
        "measurement_id=*/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        "*.json"
    )


def _metadata_fields(
    bronze_df,
) -> set[str]:
    """Return field names inside the Bronze metadata struct."""

    if "metadata" not in bronze_df.schema.fieldNames():
        raise ValueError(
            "Bronze dataframe does not contain metadata"
        )

    metadata_type = (
        bronze_df.schema["metadata"]
        .dataType
    )

    if not hasattr(
        metadata_type,
        "fieldNames",
    ):
        raise ValueError(
            "Bronze metadata is not a struct"
        )

    return set(
        metadata_type.fieldNames()
    )


def _bronze_window_metadata_columns(
    bronze_df,
) -> tuple[str, str]:
    """
    Resolve window metadata names.

    Old Bronze:
        requested_start_utc
        requested_stop_utc

    Kafka/global Bronze may use:
        window_start_utc
        window_stop_utc
    """

    fields = _metadata_fields(
        bronze_df
    )

    candidates = (
        (
            "requested_start_utc",
            "requested_stop_utc",
        ),
        (
            "window_start_utc",
            "window_stop_utc",
        ),
    )

    for start_field, stop_field in candidates:
        if (
            start_field in fields
            and stop_field in fields
        ):
            return (
                start_field,
                stop_field,
            )

    raise ValueError(
        "Bronze metadata does not contain a supported "
        "window field pair; fields="
        f"{sorted(fields)}"
    )


def filter_exact_bronze_window(
    bronze_df,
    window: TimeWindow,
):
    """Select Bronze envelopes belonging to exactly this window."""

    from pyspark.sql import functions as f

    (
        start_field,
        stop_field,
    ) = _bronze_window_metadata_columns(
        bronze_df
    )

    return bronze_df.filter(
        (
            f.col(
                f"metadata.{start_field}"
            )
            == utc_text(
                window.start
            )
        )
        &
        (
            f.col(
                f"metadata.{stop_field}"
            )
            == utc_text(
                window.stop
            )
        )
    )


def filter_event_window(
    df,
    window: TimeWindow,
):
    """
    Keep event time inside the half-open interval:

        [start, stop)
    """

    from pyspark.sql import functions as f

    return df.filter(
        (
            f.col(
                "event_time_utc"
            )
            >= f.lit(
                window.start
            )
        )
        &
        (
            f.col(
                "event_time_utc"
            )
            < f.lit(
                window.stop
            )
        )
    )


def _hour_starts(
    window: TimeWindow,
):
    """
    Yield every UTC hour partition touched by the window.

    Important for sub-hour windows.

    Example:
        06:55 -> 07:05

    touches:
        hour=06
        hour=07
    """

    current = window.start.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    while current < window.stop:
        yield current

        current += timedelta(
            hours=1
        )


def hourly_partition_paths(
    target: str,
    window: TimeWindow,
) -> list[str]:
    """Build Silver hourly partition paths touched by a window."""

    root = target.rstrip(
        "/"
    )

    return [
        (
            f"{root}/"
            f"year={hour:%Y}/"
            f"month={hour:%m}/"
            f"day={hour:%d}/"
            f"hour={hour:%H}"
        )
        for hour in _hour_starts(
            window
        )
    ]


def path_exists(
    spark,
    path: str,
) -> bool:
    """Check whether a Hadoop/S3A path currently exists."""

    jvm = spark._jvm

    hadoop_path = (
        jvm.org.apache.hadoop.fs.Path(
            path
        )
    )

    fs = (
        hadoop_path
        .getFileSystem(
            spark._jsc.hadoopConfiguration()
        )
    )

    return bool(
        fs.exists(
            hadoop_path
        )
    )


def existing_window_rows(
    spark,
    target: str,
    window: TimeWindow,
):
    """
    Read the complete hourly Silver partitions touched
    by the requested processing window.

    The complete partitions are intentionally read because
    a five-minute update must preserve rows from all other
    minutes inside the same hour.
    """

    paths = [
        path
        for path in hourly_partition_paths(
            target,
            window,
        )
        if path_exists(
            spark,
            path,
        )
    ]

    if not paths:
        return None

    return (
        spark.read
        .option(
            "basePath",
            target.rstrip(
                "/"
            ),
        )
        .parquet(
            *paths
        )
    )


def replace_measurements_in_window(
    incoming_df,
    target: str,
    window: TimeWindow,
    measurement_ids: Iterable[int],
) -> None:
    """
    Replace selected measurements only inside the requested
    event-time window.

    Everything else in the affected hourly partitions is preserved.

    Example:

        existing hour 06:
            06:00-06:05 measurement A
            06:05-06:10 measurement A
            06:10-06:15 measurement A

        update:
            06:05-06:10 measurement A

        result:
            06:00-06:05 KEEP
            06:05-06:10 REPLACE
            06:10-06:15 KEEP

    This makes five-minute microbatch reruns idempotent without
    deleting other windows in the same hourly partition.

    allowMissingColumns=True also keeps additive schema evolution
    non-destructive.
    """

    from pyspark.sql import functions as f

    ids = tuple(
        sorted(
            set(
                int(value)
                for value
                in measurement_ids
            )
        )
    )

    if (
        not ids
        or any(
            value <= 0
            for value in ids
        )
    ):
        raise ValueError(
            "measurement_ids must contain positive IDs"
        )

    spark = incoming_df.sparkSession

    # Preserve partition values such as:
    #
    # month=09
    # hour=06
    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    # Only rows belonging to this exact update window may
    # enter the replacement set.
    incoming = (
        filter_event_window(
            incoming_df,
            window,
        )
        .cache()
    )

    preserved = None

    try:
        if (
            incoming
            .limit(
                1
            )
            .count()
            == 0
        ):
            raise ValueError(
                "No Silver rows fall inside the requested "
                "event-time window"
            )

        # Incoming data must contain only measurements explicitly
        # selected by the caller.
        unexpected = (
            incoming
            .filter(
                ~f.col(
                    "measurement_id"
                ).isin(
                    list(
                        ids
                    )
                )
            )
            .limit(
                1
            )
            .count()
        )

        if unexpected:
            raise ValueError(
                "Incoming Silver contains measurement IDs "
                "outside the update scope"
            )

        existing = (
            existing_window_rows(
                spark,
                target,
                window,
            )
        )

        if existing is not None:
            # --------------------------------------------------
            # Critical five-minute preservation rule
            #
            # Remove ONLY rows satisfying BOTH:
            #
            #   measurement_id belongs to current update
            #   AND
            #   event_time belongs to current window
            #
            # Therefore:
            #
            # - other measurements survive
            # - earlier five-minute windows survive
            # - later five-minute windows survive
            # --------------------------------------------------

            selected_measurement = (
                f.col(
                    "measurement_id"
                ).isin(
                    list(
                        ids
                    )
                )
            )

            inside_update_window = (
                (
                    f.col(
                        "event_time_utc"
                    )
                    >= f.lit(
                        window.start
                    )
                )
                &
                (
                    f.col(
                        "event_time_utc"
                    )
                    < f.lit(
                        window.stop
                    )
                )
            )

            rows_to_replace = (
                selected_measurement
                & inside_update_window
            )

            preserved = (
                existing
                .filter(
                    ~rows_to_replace
                )
                .cache()
            )

            # Materialize before overwriting the same partition.
            #
            # Without this, Spark may lazily try to read files
            # while simultaneously replacing those files.
            preserved.count()

            output = (
                preserved
                .unionByName(
                    incoming,
                    allowMissingColumns=True,
                )
            )

        else:
            output = incoming

        spark.conf.set(
            "spark.sql.sources."
            "partitionOverwriteMode",
            "dynamic",
        )

        (
            output.write
            .mode(
                "overwrite"
            )
            .partitionBy(
                *PARTITION_COLUMNS
            )
            .parquet(
                target
            )
        )

    finally:
        if preserved is not None:
            preserved.unpersist()

        incoming.unpersist()