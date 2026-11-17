from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

import pendulum

from airflow.sdk import (
    dag,
    task,
)


PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/project",
)

RECOVERY_SCRIPT = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "recover_silver_gaps.py",
)


@dag(
    dag_id="ripe_atlas_silver_recovery",

    description=(
        "Detect and automatically recover missing "
        "RIPE Atlas Silver five-minute windows."
    ),

    # Run every 15 minutes, offset away from the
    # normal Silver schedule.
    #
    # Normal Silver:
    #   02, 07, 12, 17, ...
    #
    # Recovery:
    #   04, 19, 34, 49
    #
    # The recovery script itself uses a 15-minute
    # safety lag before considering a window eligible.
    schedule="4,19,34,49 * * * *",

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
        "silver",
        "recovery",
        "backfill",
        "5-minute",
    ],
)
def ripe_atlas_silver_recovery():

    @task(
        pool="silver_spark",

        retries=2,

        retry_delay=timedelta(
            minutes=2
        ),

        execution_timeout=timedelta(
            minutes=60
        ),
    )
    def recover_missing_windows() -> None:
        command = [
            sys.executable,
            RECOVERY_SCRIPT,
        ]

        print(
            "Executing Silver automatic recovery:",
            " ".join(command),
        )

        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
        )

        print(
            "Silver automatic recovery complete"
        )

    recover_missing_windows()


ripe_atlas_silver_recovery()
