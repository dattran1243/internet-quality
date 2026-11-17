#!/usr/bin/env python
"""Transform global country-scoped RIPE Atlas Bronze into Silver."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    MEASUREMENTS,
    get_country,
    get_enabled_measurement_ids,
    is_product_measurement,
)
from configs.settings import settings

from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)

from processing.silver.incremental import (
    existing_window_rows,
    filter_event_window,
    filter_exact_bronze_window,
    hourly_partition_paths,
    replace_measurements_in_window,
    validate_five_minute_window,
)

from processing.silver.candidate_generation import (
    candidate_path,
    write_candidate_generation,
)

from processing.silver.ripe_atlas_ping import (
    transform_bronze,
)

from scripts.run_silver import (
    spark_session,
)


def arguments(
    argv: list[str] | None = None,
) -> argparse.Namespace:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
        help=(
            "Configured two-letter country code. "
            f"Default: {DEFAULT_COUNTRY_CODE}"
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
        help="UTC ISO-8601 five-minute window start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 five-minute window stop",
    )

    parser.add_argument(
        "--measurement-id",
        action="append",
        type=int,
        dest="measurement_ids",
        help=(
            "Override enabled product measurement IDs. "
            "Repeat for multiple IDs."
        ),
    )

    parser.add_argument(
        "--candidate-bucket",
        default=None,
        help="Stage14 test-only bucket viq-s14-test-<12 lower hex>; no release",
    )
    parser.add_argument(
        "--candidate-generation",
        default=None,
        help="32 lower-hex generation ID for isolated unverified Parquet candidate",
    )

    return parser.parse_args(argv)


def selected_window(
    args: argparse.Namespace,
) -> TimeWindow:
    """Build and validate one exact five-minute UTC window."""

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_five_minute_window(
        window
    )

    return window


def normalize_measurement_ids(
    values: list[int],
) -> list[int]:
    """Normalize and validate product measurement selection."""

    ids = list(
        dict.fromkeys(
            int(value)
            for value in values
        )
    )

    if (
        not ids
        or any(
            measurement_id <= 0
            for measurement_id in ids
        )
    ):
        raise ValueError(
            "measurement_ids must contain positive IDs"
        )

    non_product = [
        measurement_id
        for measurement_id in ids
        if not is_product_measurement(
            measurement_id
        )
    ]

    if non_product:
        raise ValueError(
            "Country Silver only accepts enabled "
            "product measurements; rejected IDs: "
            f"{sorted(non_product)}"
        )

    return ids


def country_bronze_window_glob(
    bucket: str,
    country_code: str,
    window: TimeWindow,
) -> str:
    """
    Locate Kafka Bronze objects for exactly the window start minute.

    Layout:

        bronze/
          source=ripe_atlas/
          dataset=ping/
          country=XX/
          year=YYYY/
          month=MM/
          day=DD/
          hour=HH/
          minute=MM/
          measurement_id=ID/
          <event_id>.json
    """

    start = window.start

    return (
        f"s3a://{bucket}/"
        "bronze/"
        "source=ripe_atlas/"
        "dataset=ping/"
        f"country={country_code}/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        f"minute={start:%M}/"
        "measurement_id=*/"
        "*.json"
    )


def metadata_fields(
    bronze_df,
) -> set[str]:
    """Return the fields inside the metadata struct."""

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


def resolve_probe_scope_field(
    bronze_df,
) -> str:
    """
    Resolve probe-scope metadata field.

    Current country ingestion uses probe_scope_key.
    source_scope_key is also accepted for forward compatibility.
    """

    fields = metadata_fields(
        bronze_df
    )

    for field in (
        "probe_scope_key",
        "source_scope_key",
    ):
        if field in fields:
            return field

    raise ValueError(
        "Bronze metadata is missing "
        "probe_scope_key/source_scope_key"
    )


def validate_required_metadata(
    bronze_df,
) -> None:
    """Validate fields required by the five-minute Silver job."""

    fields = metadata_fields(
        bronze_df
    )

    required = {
        "country_code",
        "measurement_id",
        "record_count",
        "chunk_index",
        "chunk_count",
        "ingested_at_utc",
    }

    missing = sorted(
        required - fields
    )

    if missing:
        raise ValueError(
            "Bronze metadata is missing required fields: "
            f"{missing}"
        )


def validate_chunks(
    bronze_df,
    measurement_ids: list[int],
) -> dict[int, int]:
    """
    Validate that every selected measurement has a complete,
    non-duplicated chunk set.

    Returns:
        measurement_id -> total record_count
    """

    rows = (
        bronze_df
        .selectExpr(
            "cast(metadata.measurement_id as long) "
            "as measurement_id",
            "cast(metadata.chunk_index as int) "
            "as chunk_index",
            "cast(metadata.chunk_count as int) "
            "as chunk_count",
            "cast(metadata.record_count as long) "
            "as record_count",
        )
        .collect()
    )

    chunks: dict[
        int,
        dict[str, object],
    ] = {}

    for row in rows:
        if row.measurement_id is None:
            raise ValueError(
                "Bronze chunk is missing measurement_id"
            )

        measurement_id = int(
            row.measurement_id
        )

        if (
            row.chunk_index is None
            or row.chunk_count is None
        ):
            raise ValueError(
                "Bronze chunk metadata is incomplete "
                f"for measurement={measurement_id}"
            )

        chunk_index = int(
            row.chunk_index
        )

        chunk_count = int(
            row.chunk_count
        )

        record_count = int(
            row.record_count
            or 0
        )

        if chunk_count <= 0:
            raise ValueError(
                "chunk_count must be positive "
                f"measurement={measurement_id}"
            )

        if (
            chunk_index < 0
            or chunk_index >= chunk_count
        ):
            raise ValueError(
                "Invalid chunk_index "
                f"measurement={measurement_id} "
                f"chunk_index={chunk_index} "
                f"chunk_count={chunk_count}"
            )

        info = chunks.setdefault(
            measurement_id,
            {
                "chunk_count": chunk_count,
                "indexes": [],
                "record_count": 0,
            },
        )

        if (
            int(
                info["chunk_count"]
            )
            != chunk_count
        ):
            raise ValueError(
                "Conflicting chunk_count values "
                f"measurement={measurement_id}"
            )

        info["indexes"].append(
            chunk_index
        )

        info["record_count"] = (
            int(
                info["record_count"]
            )
            + record_count
        )

    actual_measurements = sorted(
        chunks
    )

    expected_measurements = sorted(
        measurement_ids
    )

    if (
        actual_measurements
        != expected_measurements
    ):
        raise ValueError(
            "Bronze measurement scope mismatch: "
            f"expected={expected_measurements} "
            f"actual={actual_measurements}"
        )

    record_counts: dict[int, int] = {}

    for measurement_id, info in chunks.items():
        chunk_count = int(
            info["chunk_count"]
        )

        indexes = [
            int(value)
            for value in info["indexes"]
        ]

        if len(indexes) != len(
            set(indexes)
        ):
            raise ValueError(
                "Duplicate Bronze chunk_index "
                f"measurement={measurement_id} "
                f"indexes={indexes}"
            )

        actual_indexes = set(
            indexes
        )

        expected_indexes = set(
            range(
                chunk_count
            )
        )

        if (
            actual_indexes
            != expected_indexes
        ):
            raise ValueError(
                "Incomplete Bronze chunk set "
                f"measurement={measurement_id} "
                f"expected={sorted(expected_indexes)} "
                f"actual={sorted(actual_indexes)}"
            )

        record_counts[
            measurement_id
        ] = int(
            info["record_count"]
        )

    return record_counts


def probe_scope_mapping(
    bronze_df,
    measurement_ids: list[int],
) -> dict[int, str]:
    """
    Validate probe scope lineage.

    Multiple chunks for one measurement must all reference
    the same frozen scope.
    """

    scope_field = (
        resolve_probe_scope_field(
            bronze_df
        )
    )

    rows = (
        bronze_df
        .selectExpr(
            "cast(metadata.measurement_id as long) "
            "as measurement_id",
            f"metadata.{scope_field} "
            "as probe_scope_key",
        )
        .distinct()
        .collect()
    )

    mapping: dict[
        int,
        str,
    ] = {}

    for row in rows:
        if row.measurement_id is None:
            raise ValueError(
                "Bronze envelope is missing measurement_id"
            )

        measurement_id = int(
            row.measurement_id
        )

        if (
            row.probe_scope_key is None
            or not str(
                row.probe_scope_key
            ).strip()
        ):
            raise ValueError(
                "Bronze envelope is missing probe scope "
                f"measurement={measurement_id}"
            )

        scope_key = str(
            row.probe_scope_key
        )

        previous = mapping.get(
            measurement_id
        )

        if (
            previous is not None
            and previous != scope_key
        ):
            raise ValueError(
                "Measurement references multiple probe scopes "
                f"measurement={measurement_id} "
                f"first={previous} "
                f"second={scope_key}"
            )

        mapping[
            measurement_id
        ] = scope_key

    if (
        sorted(mapping)
        != sorted(measurement_ids)
    ):
        raise ValueError(
            "Probe-scope measurement scope mismatch: "
            f"expected={sorted(measurement_ids)} "
            f"actual={sorted(mapping)}"
        )

    return mapping


def main(
    argv: list[str] | None = None,
    spark=None,
) -> None:
    started = time.monotonic()

    owns_spark = spark is None

    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(
        __name__
    )

    args = arguments(
    argv
    )

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} exists in "
            "monitoring registry but is currently disabled"
        )

    measurement_ids = (
        normalize_measurement_ids(
            args.measurement_ids
            or get_enabled_measurement_ids()
        )
    )

    window = selected_window(
        args
    )

    source = (
        country_bronze_window_glob(
            settings.minio_bucket,
            country.code,
            window,
        )
    )

    # Keep the existing Silver path so downstream jobs
    # do not need to change yet.
    target = (
        f"s3a://{settings.minio_bucket}/"
        "silver/ripe_atlas/"
        "ping_by_country/"
        f"country={country.code}/"
    )

    # Both CLI flags are mandatory for candidate mode, and validated BEFORE
    # any Spark session or network/S3 work. Legacy invocation stays unchanged.
    candidate_target = None
    if args.candidate_bucket is not None or args.candidate_generation is not None:
        if args.candidate_bucket is None or args.candidate_generation is None:
            raise ValueError("CANDIDATE_FLAGS_MUST_BE_PAIRED")
        candidate_target = candidate_path(
            args.candidate_bucket, country.code, window, args.candidate_generation,
        )

    if spark is None:
        spark = spark_session()

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    try:
        from pyspark.sql import (
            functions as f,
        )

        log.info(
            "Country Silver 5-minute start "
            "country=%s "
            "window=%s..%s "
            "measurements=%s",
            country.code,
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
            measurement_ids,
        )

        log.info(
            "Kafka Bronze input glob=%s",
            source,
        )

        bronze_candidates = (
            spark.read
            .option(
                "multiLine",
                "true",
            )
            .json(
                source
            )
        )

        validate_required_metadata(
            bronze_candidates
        )

        exact_window = (
            filter_exact_bronze_window(
                bronze_candidates,
                window,
            )
            .cache()
        )

        try:
            if (
                exact_window
                .limit(1)
                .count()
                == 0
            ):
                raise ValueError(
                    "No Kafka Bronze envelopes found "
                    "for the requested exact window"
                )

            invalid_country = (
                exact_window
                .filter(
                    f.col(
                        "metadata.country_code"
                    ).isNull()
                    |
                    (
                        f.upper(
                            f.col(
                                "metadata.country_code"
                            )
                        )
                        != country.code
                    )
                )
                .limit(1)
                .count()
            )

            if invalid_country:
                raise ValueError(
                    "Bronze metadata contains rows outside "
                    f"country={country.code}"
                )

            bronze = (
                exact_window
                .filter(
                    f.col(
                        "metadata.measurement_id"
                    )
                    .cast(
                        "long"
                    )
                    .isin(
                        measurement_ids
                    )
                )
                .cache()
            )

            try:
                envelope_count = (
                    bronze.count()
                )

                if envelope_count == 0:
                    raise ValueError(
                        "No selected measurement chunks "
                        "found in Bronze"
                    )

                # --------------------------------------------------
                # Chunk completeness
                # --------------------------------------------------

                record_counts = (
                    validate_chunks(
                        bronze,
                        measurement_ids,
                    )
                )

                log.info(
                    "Bronze chunk validation passed "
                    "country=%s "
                    "envelopes=%s "
                    "records_by_measurement=%s",
                    country.code,
                    envelope_count,
                    record_counts,
                )

                # --------------------------------------------------
                # Frozen probe-scope lineage
                # --------------------------------------------------

                probe_scope_by_measurement = (
                    probe_scope_mapping(
                        bronze,
                        measurement_ids,
                    )
                )

                log.info(
                    "Bronze probe scopes validated "
                    "country=%s "
                    "unique_scopes=%s",
                    country.code,
                    len(
                        set(
                            probe_scope_by_measurement
                            .values()
                        )
                    ),
                )

                # --------------------------------------------------
                # Metadata record count vs real exploded count
                # --------------------------------------------------

                metadata_record_count = (
                    bronze
                    .agg(
                        f.sum(
                            f.col(
                                "metadata.record_count"
                            ).cast(
                                "long"
                            )
                        ).alias(
                            "records"
                        )
                    )
                    .first()
                    .records
                )

                metadata_record_count = int(
                    metadata_record_count
                    or 0
                )

                exploded_record_count = (
                    bronze
                    .selectExpr(
                        "explode(records) as record"
                    )
                    .count()
                )

                if (
                    metadata_record_count
                    != exploded_record_count
                ):
                    raise ValueError(
                        "Bronze metadata record_count "
                        "does not match actual records: "
                        f"metadata={metadata_record_count} "
                        f"actual={exploded_record_count}"
                    )

                # --------------------------------------------------
                # Logical target map
                # --------------------------------------------------

                measurement_target_map = (
                    f.create_map(
                        *[
                            item
                            for measurement_id
                            in measurement_ids
                            for item
                            in (
                                f.lit(
                                    measurement_id
                                ).cast(
                                    "long"
                                ),
                                f.lit(
                                    MEASUREMENTS[
                                        measurement_id
                                    ].target
                                ),
                            )
                        ]
                    )
                )

                # --------------------------------------------------
                # Probe scope map
                # --------------------------------------------------

                measurement_scope_map = (
                    f.create_map(
                        *[
                            item
                            for measurement_id
                            in measurement_ids
                            for item
                            in (
                                f.lit(
                                    measurement_id
                                ).cast(
                                    "long"
                                ),
                                f.lit(
                                    probe_scope_by_measurement[
                                        measurement_id
                                    ]
                                ),
                            )
                        ]
                    )
                )

                # --------------------------------------------------
                # Bronze -> Silver
                # --------------------------------------------------

                silver = (
                    transform_bronze(
                        bronze
                    )
                    .withColumn(
                        "target",
                        f.element_at(
                            measurement_target_map,
                            f.col(
                                "measurement_id"
                            ),
                        ),
                    )
                    .withColumn(
                        "country_code",
                        f.lit(
                            country.code
                        ),
                    )
                    .withColumn(
                        "probe_scope_key",
                        f.element_at(
                            measurement_scope_map,
                            f.col(
                                "measurement_id"
                            ),
                        ),
                    )
                    .cache()
                )

                try:
                    silver_total = (
                        silver.count()
                    )

                    if (
                        silver_total
                        != exploded_record_count
                    ):
                        raise ValueError(
                            "Bronze-to-Silver row count changed: "
                            f"bronze={exploded_record_count} "
                            f"silver={silver_total}"
                        )

                    valid_rows = (
                        silver
                        .filter(
                            f.col(
                                "validation_status"
                            )
                            == "valid"
                        )
                        .count()
                    )

                    invalid_rows = (
                        silver
                        .filter(
                            f.col(
                                "validation_status"
                            )
                            == "invalid"
                        )
                        .count()
                    )

                    if (
                        valid_rows
                        + invalid_rows
                        != silver_total
                    ):
                        raise ValueError(
                            "Silver validation-status counts "
                            "do not add up"
                        )

                    missing_scope = (
                        silver
                        .filter(
                            f.col(
                                "probe_scope_key"
                            ).isNull()
                            |
                            (
                                f.length(
                                    f.trim(
                                        f.col(
                                            "probe_scope_key"
                                        )
                                    )
                                )
                                == 0
                            )
                        )
                        .limit(1)
                        .count()
                    )

                    if missing_scope:
                        raise ValueError(
                            "Silver contains rows without "
                            "probe_scope_key"
                        )

                    # --------------------------------------------------
                    # Keep only actual event rows in this 5-minute window
                    # --------------------------------------------------

                    silver_window = (
                        filter_event_window(
                            silver,
                            window,
                        )
                        .cache()
                    )

                    try:
                        expected_written_rows = (
                            silver_window.count()
                        )

                        if (
                            expected_written_rows
                            == 0
                        ):
                            raise ValueError(
                                "No Silver rows fall inside "
                                "the requested five-minute window"
                            )

                        outside_window_rows = (
                            silver_total
                            - expected_written_rows
                        )

                        if outside_window_rows:
                            log.warning(
                                "Silver rows outside [start, stop) "
                                "will not be written: %s",
                                outside_window_rows,
                            )

                        if invalid_rows:
                            log.warning(
                                "Invalid Silver records retained: %s",
                                invalid_rows,
                            )

                        log.info(
                            "Country Silver transform complete "
                            "country=%s "
                            "envelopes=%s "
                            "records=%s "
                            "valid=%s "
                            "invalid=%s "
                            "window_rows=%s",
                            country.code,
                            envelope_count,
                            exploded_record_count,
                            valid_rows,
                            invalid_rows,
                            expected_written_rows,
                        )

                        if candidate_target is None:
                            log.info(
                                "Replacing legacy Silver window partitions=%s",
                                hourly_partition_paths(target, window),
                            )
                        else:
                            log.info(
                                "Writing UNVERIFIED candidate to TEST bucket: %s",
                                candidate_target,
                            )

                        # --------------------------------------------------
                        # Safe five-minute replacement
                        #
                        # The helper preserves:
                        # - other measurements
                        # - previous five-minute windows
                        # - later five-minute windows
                        # --------------------------------------------------

                        if candidate_target is None:
                            replace_measurements_in_window(
                                silver,
                                target,
                                window,
                                measurement_ids,
                            )
                            written = existing_window_rows(
                                spark, target, window,
                            )
                        else:
                            # NEVER update the legacy mutable Silver partitions.
                            # Candidate must later pass independent Parquet/S3
                            # verifier and Registry V2 CAS before it is publishable.
                            write_candidate_generation(
                                silver_window, args.candidate_bucket, country.code,
                                window, args.candidate_generation, measurement_ids,
                            )
                            written = (
                                spark.read
                                .option("basePath", candidate_target)
                                .parquet(candidate_target)
                            )
                            log.warning(
                                "UNVERIFIED_SILVER_CANDIDATE_ONLY path=%s "
                                "NO_REGISTRY_COMMIT NO_SUCCESS_MANIFEST",
                                candidate_target,
                            )

                        # --------------------------------------------------
                        # Readback verification (count/scope only, NOT receipt)
                        # --------------------------------------------------

                        if written is None:
                            raise ValueError(
                                "Silver write produced "
                                "no output partition"
                            )

                        written_selected = (
                            filter_event_window(
                                written,
                                window,
                            )
                            .filter(
                                f.col(
                                    "country_code"
                                )
                                == country.code
                            )
                            .filter(
                                f.col(
                                    "measurement_id"
                                )
                                .isin(
                                    measurement_ids
                                )
                            )
                            .cache()
                        )

                        try:
                            written_count = (
                                written_selected
                                .count()
                            )

                            if (
                                written_count
                                != expected_written_rows
                            ):
                                raise ValueError(
                                    "Silver readback row-count mismatch: "
                                    f"expected={expected_written_rows} "
                                    f"written={written_count}"
                                )

                            # Measurements with zero raw records
                            # naturally produce no Silver rows.
                            expected_output_measurements = sorted(
                                measurement_id
                                for (
                                    measurement_id,
                                    count,
                                ) in record_counts.items()
                                if count > 0
                            )

                            written_measurements = sorted(
                                int(
                                    row.measurement_id
                                )
                                for row in (
                                    written_selected
                                    .select(
                                        "measurement_id"
                                    )
                                    .distinct()
                                    .collect()
                                )
                            )

                            if (
                                written_measurements
                                != expected_output_measurements
                            ):
                                raise ValueError(
                                    "Silver readback measurement "
                                    "scope mismatch: "
                                    f"expected="
                                    f"{expected_output_measurements} "
                                    f"actual="
                                    f"{written_measurements}"
                                )

                            written_countries = sorted(
                                row.country_code
                                for row in (
                                    written_selected
                                    .select(
                                        "country_code"
                                    )
                                    .distinct()
                                    .collect()
                                )
                            )

                            if (
                                written_countries
                                != [
                                    country.code
                                ]
                            ):
                                raise ValueError(
                                    "Silver readback country "
                                    "scope mismatch: "
                                    f"{written_countries}"
                                )

                            log.info(
                                "Country Silver 5-minute complete "
                                "country=%s "
                                "window=%s..%s "
                                "rows=%s "
                                "measurements=%s "
                                "duration_seconds=%.2f",
                                country.code,
                                utc_text(
                                    window.start
                                ),
                                utc_text(
                                    window.stop
                                ),
                                written_count,
                                written_measurements,
                                (
                                    time.monotonic()
                                    - started
                                ),
                            )

                        finally:
                            written_selected.unpersist()

                    finally:
                        silver_window.unpersist()

                finally:
                    silver.unpersist()

            finally:
                bronze.unpersist()

        finally:
            exact_window.unpersist()

    finally:
      if owns_spark:
          spark.stop()


if __name__ == "__main__":
    main()