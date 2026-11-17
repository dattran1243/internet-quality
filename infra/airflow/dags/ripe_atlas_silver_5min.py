

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


PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/project",
)

SILVER_SCRIPT = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "run_silver_window.py",
)


def to_utc(value):
    """
    Convert Airflow datetime values to Pendulum UTC safely.

    Airflow 3 may expose DagRun.logical_date as a standard
    datetime.datetime rather than pendulum.DateTime.
    """

    if value is None:
        raise ValueError(
            "Datetime value is missing"
        )

    return (
        pendulum.instance(value)
        .in_timezone("UTC")
    )


@dag(
    dag_id="ripe_atlas_silver_5min",

    description=(
        "Transform Kafka-backed RIPE Atlas Bronze "
        "five-minute windows into Silver."
    ),

    # RIPE ingestion:
    #   00, 05, 10, 15, ...
    #
    # Silver:
    #   02, 07, 12, 17, ...
    #
    # Example:
    #
    # ingestion logical time 08:20
    #     -> Bronze window 08:10..08:15
    #
    # Silver logical time 08:22
    #     -> Silver window 08:10..08:15
    schedule="2-57/5 * * * *",

    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz="UTC",
    ),

    catchup=False,

    # Only one Silver window may execute at a time.
    #
    # One DAG run now launches one Spark application that
    # processes every enabled country using one shared
    # SparkSession.
    max_active_runs=1,
    max_active_tasks=1,

    tags=[
        "ripe-atlas",
        "silver",
        "spark",
        "global",
        "5-minute",
    ],
)
def ripe_atlas_silver_5min():

    @task(
        pool="silver_spark",
        retries=3,

        retry_delay=timedelta(
            minutes=1
        ),

        execution_timeout=timedelta(
            minutes=15
        ),
    )
    def process_window() -> None:
        """
        Process one deterministic five-minute Silver window
        for all enabled countries using one Spark application.
        """

        context = get_current_context()

        dag_run = context.get(
            "dag_run"
        )

        if dag_run is None:
            raise ValueError(
                "Airflow dag_run is missing from task context"
            )

        raw_logical_date = (
            dag_run.logical_date
        )

        if raw_logical_date is None:
            raise ValueError(
                "Airflow DagRun logical_date is missing. "
                "For manual testing, trigger with --logical-date."
            )

        logical_date = to_utc(
            raw_logical_date
        )

        # Example:
        #
        # logical_date:
        #     08:22
        #
        # aligned_time:
        #     08:20
        #
        # window_stop:
        #     08:15
        #
        # window_start:
        #     08:10

        aligned_minute = (
            logical_date.minute
            // 5
        ) * 5

        aligned_time = (
            logical_date.replace(
                minute=aligned_minute,
                second=0,
                microsecond=0,
            )
        )

        window_stop = (
            aligned_time
            - timedelta(
                minutes=5
            )
        )

        window_start = (
            window_stop
            - timedelta(
                minutes=5
            )
        )

        duration_seconds = (
            window_stop
            - window_start
        ).total_seconds()

        if duration_seconds != 300:
            raise ValueError(
                "Expected exactly one five-minute window, "
                f"got {duration_seconds} seconds"
            )

        start_text = (
            window_start.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

        stop_text = (
            window_stop.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

        print(
            "RIPE Silver window start "
            f"logical_date={logical_date} "
            f"aligned_time={aligned_time} "
            f"window={start_text}..{stop_text}"
        )

        command = [
            sys.executable,
            SILVER_SCRIPT,
            "--start",
            start_text,
            "--stop",
            stop_text,
        ]

        print(
            "Executing:",
            " ".join(
                command
            ),
        )

        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
        )

        print(
            "RIPE Silver window complete "
            f"window={start_text}..{stop_text}"
        )

    process_window()


ripe_atlas_silver_5min()
