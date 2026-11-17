"""Schedule global RIPE Atlas -> Kafka ingestion every 5 minutes."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

import pendulum

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)


# ------------------------------------------------------------------
# Project paths
# ------------------------------------------------------------------

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/project",
)

PUBLISH_SCRIPT = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "publish_ripe_country_kafka.py",
)


# ------------------------------------------------------------------
# Country configuration
# ------------------------------------------------------------------

def enabled_country_codes() -> list[str]:
    """Return enabled country codes from project monitoring config."""

    from configs.monitoring import (
        get_enabled_countries,
    )

    return sorted(
        country.code
        for country in get_enabled_countries()
    )


COUNTRY_CODES = enabled_country_codes()


if not COUNTRY_CODES:
    raise ValueError(
        "No enabled countries found in monitoring configuration"
    )


# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------

@dag(
    dag_id="ripe_atlas_kafka_ingestion",
    description=(
        "Fetch RIPE Atlas 5-minute country windows "
        "and publish raw events to Kafka."
    ),

    # Run every five minutes.
    schedule="*/5 * * * *",

    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz="UTC",
    ),

    # Do not automatically backfill old intervals.
    catchup=False,

    # Prevent multiple ingestion windows from overlapping.
    max_active_runs=1,
    max_active_tasks=3,
    tags=[
        "ripe-atlas",
        "kafka",
        "global",
        "ingestion",
    ],
)
def ripe_atlas_kafka_ingestion():
    """Global RIPE Atlas -> Kafka ingestion DAG."""

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def publish_country(
        country_code: str,
    ) -> None:
        """Publish one country's RIPE Atlas window to Kafka.

        The DAG runs every five minutes.

        We intentionally ingest one interval behind the current
        aligned time as a lateness buffer.

        Example:

            Airflow logical time:
                12:20

            Aligned:
                12:20

            Lateness buffer:
                window_stop = 12:15

            Final ingestion window:
                12:10 -> 12:15

        This gives RIPE Atlas additional time to expose results
        before we request the window.
        """

        context = get_current_context()

        # ----------------------------------------------------------
        # Resolve Airflow logical time
        # ----------------------------------------------------------

        logical_date = context["logical_date"]

        if logical_date is None:
            raise ValueError(
                "Airflow logical_date is missing"
            )

        logical_date = (
            logical_date
            .in_timezone("UTC")
        )

        # ----------------------------------------------------------
        # Align to exact five-minute boundary
        # ----------------------------------------------------------

        aligned_minute = (
            logical_date.minute // 5
        ) * 5

        aligned_time = logical_date.replace(
            minute=aligned_minute,
            second=0,
            microsecond=0,
        )

        # ----------------------------------------------------------
        # Apply one complete five-minute lateness buffer
        # ----------------------------------------------------------

        window_stop = (
            aligned_time
            - timedelta(minutes=5)
        )

        window_start = (
            window_stop
            - timedelta(minutes=5)
        )

        # ----------------------------------------------------------
        # Validate exact window duration
        # ----------------------------------------------------------

        duration_seconds = (
            window_stop
            - window_start
        ).total_seconds()

        if duration_seconds != 300:
            raise ValueError(
                "Expected exactly one 5-minute window, "
                f"got {duration_seconds} seconds"
            )

        # ----------------------------------------------------------
        # Format timestamps for project ingestion script
        # ----------------------------------------------------------

        start_text = window_start.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        stop_text = window_stop.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        print(
            "RIPE Airflow ingestion "
            f"country={country_code} "
            f"logical_date={logical_date} "
            f"aligned_time={aligned_time} "
            f"window={start_text}..{stop_text}"
        )

        # ----------------------------------------------------------
        # Call the already-tested country Kafka producer
        # ----------------------------------------------------------

        command = [
            sys.executable,
            PUBLISH_SCRIPT,
            "--country-code",
            country_code,
            "--start",
            start_text,
            "--stop",
            stop_text,
        ]

        print(
            "Executing:",
            " ".join(command),
        )

        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
        )

        print(
            "RIPE Airflow ingestion complete "
            f"country={country_code} "
            f"window={start_text}..{stop_text}"
        )

    # --------------------------------------------------------------
    # Dynamic task mapping
    #
    # Airflow creates one mapped task per enabled country.
    #
    # Example with 15 countries:
    #
    # publish_country[0]
    # publish_country[1]
    # ...
    # publish_country[14]
    # --------------------------------------------------------------

    publish_country.expand(
        country_code=COUNTRY_CODES
    )


ripe_atlas_kafka_ingestion()