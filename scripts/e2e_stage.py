#!/usr/bin/env python
"""Runtime entrypoint; not a Gate B test command."""
from pathlib import Path
import argparse
import json
import os
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    from processing.e2e.contracts import STAGES, utc_now
    from processing.e2e.guards import actual_context, gate_c, load_manifest, runtime_env, safe_message, verify_sources
    from processing.e2e.io import Evidence, write_new
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=STAGES, required=True)
    args = parser.parse_args()
    path = os.environ["VIQ_E2E_MANIFEST"]
    m = load_manifest(path)
    gate_c(m, path, os.environ["VIQ_E2E_APPROVAL"], "run")
    actual_context(m, args.stage); runtime_env(m); verify_sources(m, "/opt/project")
    e = Evidence(m, path); previous = e.upstream(args.stage); started = utc_now()
    write_new(e.root / (args.stage + ".attempt.json"), {
        "stage": args.stage, "run_id": m["run_id"], "started_at": started,
        "manifest_sha256": e.manifest_hash})
    try:
        from processing.e2e.stages import execute
        output = execute(args.stage, e)
        e.finish(args.stage, started, previous, output)
    except BaseException as exc:
        # Third-party exception messages can contain connection strings; never persist them.
        write_new(e.root / (args.stage + ".failure.json"), {
            "stage": args.stage, "run_id": m["run_id"], "error_type": type(exc).__name__,
            "started_at": started, "failed_at": utc_now(), "status": "E2E_FAIL"})
        print(safe_message("E2E stage failed: " + args.stage + " / " + type(exc).__name__), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
