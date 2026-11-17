#!/usr/bin/env python3
"""Local signed-plan preflight only; NO Spark/S3/Postgres/Gold execution."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.gold.verified_hour_plan import verify_hour_plan_file

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--plan', required=True)
    p.add_argument('--test-public-keys', required=True)
    p.add_argument('--expected-hour', default=None)
    p.add_argument('--local-parquet-bundle', default=None)
    a=p.parse_args()
    print(json.dumps(verify_hour_plan_file(a.plan,a.test_public_keys,a.expected_hour,a.local_parquet_bundle), indent=2))

if __name__ == '__main__':
    main()
