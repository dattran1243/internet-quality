"""Schedule global RIPE Atlas Gold aggregation once per hour."""

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

GOLD_SCRIPT = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "run_gold_global_hour.py",
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
    dag_id="ripe_atlas_gold_hourly",

    description=(
        "Build global RIPE Atlas hourly Gold network-quality "
        "aggregates from complete Silver windows."
    ),

    # Run once per hour at minute 30.
    #
    # Example:
    #
    # Gold logical time:
    #     08:30
    #
    # Gold window:
    #     07:00 -> 08:00
    #
    # This gives normal Silver + Silver recovery time
    # to finish the twelve required five-minute windows.
    schedule="30 * * * *",

    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz="UTC",
    ),

    catchup=False,

    # Only one hourly Gold Spark application at a time.
    max_active_runs=1,
    max_active_tasks=1,

    tags=[
        "ripe-atlas",
        "gold",
        "spark",
        "global",
        "hourly",
    ],
)
def ripe_atlas_gold_hourly():

    @task(
        # Share the same single-slot Spark pool with Silver
        # so local Spark applications cannot overlap.
        pool="silver_spark",

        retries=2,

        retry_delay=timedelta(
            minutes=2
        ),

        execution_timeout=timedelta(
            minutes=30
        ),
    )
    def process_hour() -> None:
        """
        Build one deterministic previous-complete-hour Gold window.

        Example:
            logical_date = 08:30
            window_start = 07:00
            window_stop  = 08:00
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

        # Floor to the current UTC hour.
        #
        # 08:30 -> 08:00
        aligned_hour = (
            logical_date.replace(
                minute=0,
                second=0,
                microsecond=0,
            )
        )

        # Process the previous complete hour.
        #
        # 08:30 logical time:
        #     stop  = 08:00
        #     start = 07:00
        window_stop = aligned_hour

        window_start = (
            window_stop
            - timedelta(hours=1)
        )

        duration_seconds = (
            window_stop
            - window_start
        ).total_seconds()

        if duration_seconds != 3600:
            raise ValueError(
                "Expected exactly one hourly Gold window, "
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
            "RIPE Gold hour start "
            f"logical_date={logical_date} "
            f"aligned_hour={aligned_hour} "
            f"window={start_text}..{stop_text}"
        )

        command = [
            sys.executable,
            GOLD_SCRIPT,
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
            "RIPE Gold hour complete "
            f"window={start_text}..{stop_text}"
        )

    process_hour()


ripe_atlas_gold_hourly()
