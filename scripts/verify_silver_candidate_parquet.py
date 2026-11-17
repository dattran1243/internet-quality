#!/usr/bin/env python3
"""Local-only independent Parquet validator. No S3, Spark, DB or repo writes.
Example: python scripts/verify_silver_candidate_parquet.py --parquet local.part.parquet \
  --oracle independent.jsonl --columns measurement_id,country_code --types int64,string
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

# Works when invoked from anywhere; imports only local project's verification module.
PROJECT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(PROJECT))
from processing.silver.parquet_independent_verifier import VerificationBlocked, verify_files


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--parquet',action='append',required=True,help='One physical part-*.parquet; repeat for each file')
    p.add_argument('--oracle',required=True,help='Independent JSONL source with precisely physical schema columns')
    p.add_argument('--columns',required=True,help='Comma-separated physical Parquet column order')
    p.add_argument('--types',required=True,help='Comma-separated PyArrow physical type strings in column order')
    args=p.parse_args(argv)
    try:
        result=verify_files(args.parquet,args.oracle,args.columns.split(','),args.types.split(','))
        print(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2))
        return 0
    except (VerificationBlocked, OSError, ValueError) as exc:
        print(json.dumps({'result':'BLOCKED_LOCAL_PARQUET_VERIFICATION','reason':str(exc),
                          'production_ready':False},ensure_ascii=False),file=sys.stderr)
        return 2

if __name__=='__main__':
    raise SystemExit(main())
