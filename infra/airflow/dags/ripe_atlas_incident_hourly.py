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

INCIDENT_SCRIPT = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "run_anomaly_incident_global_hour.py",
)


def to_utc(value):
    if value is None:
        raise ValueError(
            "Datetime value is missing"
        )

    return (
        pendulum.instance(value)
        .in_timezone("UTC")
    )


@dag(
    dag_id="ripe_atlas_incident_hourly",

    description=(
        "Build global RIPE Atlas anomaly, degradation "
        "and incident candidates for one completed Gold hour."
    ),

    # Gold:
    #   minute 30
    #
    # Incident:
    #   minute 40
    #
    # Example:
    #   logical_date 08:40
    #   -> process 07:00..08:00
    schedule="40 * * * *",

    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz="UTC",
    ),

    catchup=False,

    max_active_runs=1,
    max_active_tasks=1,

    tags=[
        "ripe-atlas",
        "gold",
        "anomaly",
        "incident",
        "global",
        "hourly",
    ],
)
def ripe_atlas_incident_hourly():

    @task(
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
        context = get_current_context()

        dag_run = context.get(
            "dag_run"
        )

        if dag_run is None:
            raise ValueError(
                "Airflow dag_run is missing"
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

        aligned_hour = (
            logical_date
            .replace(
                minute=0,
                second=0,
                microsecond=0,
            )
        )

        window_stop = aligned_hour

        window_start = (
            window_stop
            - timedelta(
                hours=1
            )
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
            "RIPE Incident hour start "
            f"logical_date={logical_date} "
            f"aligned_hour={aligned_hour} "
            f"window={start_text}..{stop_text}"
        )

        command = [
            sys.executable,
            INCIDENT_SCRIPT,
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
            "RIPE Incident hour complete "
            f"window={start_text}..{stop_text}"
        )

    process_hour()


ripe_atlas_incident_hourly()
