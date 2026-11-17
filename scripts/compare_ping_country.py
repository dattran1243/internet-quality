#!/usr/bin/env python
"""Compare legacy ping_vietnam with generic country-scoped Ping output."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


sys.path.insert(
    0,
    str(
        Path(__file__)
        .resolve()
        .parents[1]
    ),
)


from configs.settings import settings

from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
)

from processing.silver.incremental import (
    existing_window_rows,
    filter_event_window,
    validate_hour_aligned_window,
)

from scripts.run_silver import spark_session


STABLE_COMPARE_COLUMNS = [
    "event_time_utc",
    "measurement_id",
    "probe_id",
    "target",
    "destination_address",
    "address_family",
    "protocol",
    "rtt_min_ms",
    "rtt_avg_ms",
    "rtt_max_ms",
    "packets_sent",
    "packets_received",
    "packet_loss_pct",
    "validation_status",
    "year",
    "month",
    "day",
    "hour",
    "country_code",
]


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=True,
        help="UTC ISO-8601 start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 stop",
    )

    parser.add_argument(
        "--country-code",
        default="VN",
        help="Country code. Default: VN",
    )

    return parser.parse_args()


def main() -> None:
    args = arguments()

    country_code = (
        args.country_code
        .strip()
        .upper()
    )

    window = TimeWindow(
        parse_utc(
            args.start
        ),
        parse_utc(
            args.stop
        ),
    )

    validate_hour_aligned_window(
        window
    )


    spark = spark_session()

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )


    root = (
        f"s3a://{settings.minio_bucket}/"
        "silver/ripe_atlas"
    )

    legacy_path = (
        f"{root}/ping_vietnam/"
    )

    generic_path = (
        f"{root}/ping_enriched/"
        f"country={country_code}/"
    )


    try:
        legacy = existing_window_rows(
            spark,
            legacy_path,
            window,
        )

        generic = existing_window_rows(
            spark,
            generic_path,
            window,
        )


        if legacy is None:
            raise ValueError(
                "Legacy ping_vietnam output "
                "does not exist for this window"
            )

        if generic is None:
            raise ValueError(
                "Generic ping_enriched output "
                "does not exist for this window"
            )


        legacy = (
            filter_event_window(
                legacy,
                window,
            )
            .cache()
        )

        generic = (
            filter_event_window(
                generic,
                window,
            )
            .cache()
        )


        legacy_count = (
            legacy.count()
        )

        generic_count = (
            generic.count()
        )


        legacy_probe_count = (
            legacy
            .select(
                "probe_id"
            )
            .distinct()
            .count()
        )

        generic_probe_count = (
            generic
            .select(
                "probe_id"
            )
            .distinct()
            .count()
        )


        legacy_measurement_count = (
            legacy
            .select(
                "measurement_id"
            )
            .distinct()
            .count()
        )

        generic_measurement_count = (
            generic
            .select(
                "measurement_id"
            )
            .distinct()
            .count()
        )


        print()
        print(
            "=== REGRESSION SUMMARY ==="
        )

        print(
            f"country={country_code}"
        )

        print(
            f"legacy_rows={legacy_count}"
        )

        print(
            f"generic_rows={generic_count}"
        )

        print(
            "legacy_probes="
            f"{legacy_probe_count}"
        )

        print(
            "generic_probes="
            f"{generic_probe_count}"
        )

        print(
            "legacy_measurements="
            f"{legacy_measurement_count}"
        )

        print(
            "generic_measurements="
            f"{generic_measurement_count}"
        )


        missing_columns = [
            column
            for column
            in STABLE_COMPARE_COLUMNS
            if (
                column
                not in legacy.columns
                or column
                not in generic.columns
            )
        ]


        if missing_columns:
            raise ValueError(
                "Comparison columns missing: "
                f"{missing_columns}"
            )


        legacy_stable = (
            legacy
            .select(
                *STABLE_COMPARE_COLUMNS
            )
        )

        generic_stable = (
            generic
            .select(
                *STABLE_COMPARE_COLUMNS
            )
        )


        legacy_only = (
            legacy_stable
            .exceptAll(
                generic_stable
            )
            .cache()
        )

        generic_only = (
            generic_stable
            .exceptAll(
                legacy_stable
            )
            .cache()
        )


        legacy_only_count = (
            legacy_only.count()
        )

        generic_only_count = (
            generic_only.count()
        )


        print(
            "legacy_only_rows="
            f"{legacy_only_count}"
        )

        print(
            "generic_only_rows="
            f"{generic_only_count}"
        )


        errors: list[str] = []


        if (
            legacy_count
            != generic_count
        ):
            errors.append(
                "row count differs"
            )


        if (
            legacy_probe_count
            != generic_probe_count
        ):
            errors.append(
                "probe count differs"
            )


        if (
            legacy_measurement_count
            != generic_measurement_count
        ):
            errors.append(
                "measurement count differs"
            )


        if (
            legacy_only_count
            != 0
        ):
            errors.append(
                "legacy contains stable rows "
                "missing from generic"
            )


        if (
            generic_only_count
            != 0
        ):
            errors.append(
                "generic contains stable rows "
                "missing from legacy"
            )


        if errors:
            print()
            print(
                "RESULT: FAIL"
            )

            print(
                "; ".join(
                    errors
                )
            )


            if (
                legacy_only_count
                > 0
            ):
                print()
                print(
                    "Example legacy-only rows:"
                )

                legacy_only.show(
                    10,
                    truncate=False,
                )


            if (
                generic_only_count
                > 0
            ):
                print()
                print(
                    "Example generic-only rows:"
                )

                generic_only.show(
                    10,
                    truncate=False,
                )


            raise ValueError(
                "Regression comparison failed"
            )


        print()
        print(
            "RESULT: PASS"
        )

        print(
            "Generic country pipeline matches "
            "legacy Vietnam pipeline on stable fields."
        )


        legacy_only.unpersist()
        generic_only.unpersist()

        legacy.unpersist()
        generic.unpersist()


    finally:
        spark.stop()


if __name__ == "__main__":
    main()