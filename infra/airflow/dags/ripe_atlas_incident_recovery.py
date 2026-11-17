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

RECOVERY_SCRIPT = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "recover_incident_gaps.py",
)


@dag(
    dag_id="ripe_atlas_incident_recovery",

    description=(
        "Detect and automatically recover missing "
        "RIPE Atlas global incident-candidate hours."
    ),

    # Gold recovery:
    #   minute 50
    #
    # Incident recovery:
    #   minute 55
    #
    # Both share silver_spark, so Spark-heavy
    # recovery work cannot overlap.
    schedule="55 * * * *",

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
        "recovery",
        "backfill",
        "global",
        "hourly",
    ],
)
def ripe_atlas_incident_recovery():

    @task(
        pool="silver_spark",

        retries=2,

        retry_delay=timedelta(
            minutes=5
        ),

        execution_timeout=timedelta(
            minutes=90
        ),
    )
    def recover_missing_hours() -> None:
        context = get_current_context()

        dag_run = context.get(
            "dag_run"
        )

        if dag_run is None:
            raise ValueError(
                "Airflow dag_run is missing"
            )

        conf = (
            dag_run.conf
            or {}
        )

        start = conf.get(
            "start"
        )

        stop = conf.get(
            "stop"
        )

        if bool(start) != bool(stop):
            raise ValueError(
                "Manual recovery requires both "
                "start and stop"
            )

        max_windows = int(
            conf.get(
                "max_windows",
                3,
            )
        )

        if max_windows <= 0:
            raise ValueError(
                "max_windows must be positive"
            )

        lookback_hours = int(
            conf.get(
                "lookback_hours",
                24,
            )
        )

        if lookback_hours <= 0:
            raise ValueError(
                "lookback_hours must be positive"
            )

        dry_run_value = conf.get(
            "dry_run",
            False,
        )

        if isinstance(
            dry_run_value,
            str,
        ):
            dry_run = (
                dry_run_value
                .strip()
                .lower()
                in {
                    "1",
                    "true",
                    "yes",
                    "y",
                }
            )
        else:
            dry_run = bool(
                dry_run_value
            )

        command = [
            sys.executable,
            RECOVERY_SCRIPT,
        ]

        if start:
            command.extend(
                [
                    "--start",
                    str(start),
                    "--stop",
                    str(stop),
                ]
            )

        else:
            command.extend(
                [
                    "--lookback-hours",
                    str(
                        lookback_hours
                    ),
                ]
            )

        command.extend(
            [
                "--max-windows",
                str(
                    max_windows
                ),
            ]
        )

        if dry_run:
            command.append(
                "--dry-run"
            )

        print(
            "Executing Incident recovery:",
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
            "Incident recovery task complete"
        )

    recover_missing_hours()


ripe_atlas_incident_recovery()
