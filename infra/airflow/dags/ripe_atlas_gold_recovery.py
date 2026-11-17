"""Schedule automatic RIPE Atlas Gold hourly recovery."""

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
    "recover_gold_gaps.py",
)


@dag(
    dag_id="ripe_atlas_gold_recovery",

    description=(
        "Detect missing RIPE Atlas Gold hourly windows "
        "and recover only those with complete Silver dependencies."
    ),

    schedule="50 * * * *",

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
        "recovery",
        "backfill",
        "hourly",
    ],
)
def ripe_atlas_gold_recovery():

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

        dag_run = context[
            "dag_run"
        ]

        conf = (
            dag_run.conf
            or {}
        )

        command = [
            sys.executable,
            RECOVERY_SCRIPT,
        ]

        start = conf.get(
            "start"
        )

        stop = conf.get(
            "stop"
        )

        if bool(start) != bool(stop):
            raise ValueError(
                "Manual Gold recovery requires both "
                "'start' and 'stop'"
            )

        if start and stop:
            command.extend(
                [
                    "--start",
                    str(start),
                    "--stop",
                    str(stop),
                ]
            )
        else:
            lookback_hours = int(
                conf.get(
                    "lookback_hours",
                    24,
                )
            )

            command.extend(
                [
                    "--lookback-hours",
                    str(lookback_hours),
                ]
            )

        max_windows = int(
            conf.get(
                "max_windows",
                3,
            )
        )

        command.extend(
            [
                "--max-windows",
                str(max_windows),
            ]
        )

        if conf.get(
            "dry_run",
            False,
        ):
            command.append(
                "--dry-run"
            )

        print(
            "Executing Gold recovery:",
            " ".join(command),
        )

        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
        )

        print(
            "Gold recovery task complete"
        )

    recover_missing_hours()


ripe_atlas_gold_recovery()
