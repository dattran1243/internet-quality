"""Small run-specific receipt/object helpers; clients are created only by stages."""
from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path

from processing.e2e.contracts import DAG_ID, STAGES, canonical, utc_now
from processing.e2e.guards import file_hash, require


def write_new(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as f:
        f.write(canonical(value))
    return {"uri": str(path), "sha256": file_hash(path)}


class Evidence:
    def __init__(self, manifest, manifest_path):
        self.m = manifest
        self.manifest_hash = file_hash(manifest_path)
        self.root = Path(manifest["container_evidence"])
        self.bucket = manifest["resources"]["bucket"]
        self._s3 = None

    @property
    def s3(self):
        if self._s3 is None:
            from storage.minio_client import create_s3_client
            self._s3 = create_s3_client(self.m["endpoints"]["minio"],
                                        os.environ["MINIO_ACCESS_KEY"], os.environ["MINIO_SECRET_KEY"])
        return self._s3

    def receipt_path(self, stage):
        require(stage in STAGES, "UNKNOWN_RECEIPT")
        return self.root / "receipts" / (stage + ".json")

    def read(self, stage):
        value = json.loads(self.receipt_path(stage).read_bytes())
        require(value.get("dag_id") == DAG_ID and value.get("run_id") == self.m["run_id"] and
                value.get("manifest_sha256") == self.manifest_hash and value.get("stage") == stage and
                value.get("status") == "success", "CROSS_RUN_OR_FAILED_RECEIPT")
        index = STAGES.index(stage)
        expected = file_hash(self.receipt_path(STAGES[index-1])) if index else None
        require(value.get("previous_receipt_sha256") == expected, "RECEIPT_CHAIN_BROKEN")
        return value["output"]

    def upstream(self, stage):
        index = STAGES.index(stage)
        if not index:
            require(not os.environ.get("VIQ_UPSTREAM_SHA256"), "UNEXPECTED_UPSTREAM")
            return None
        previous = STAGES[index-1]
        self.read(previous)
        h = file_hash(self.receipt_path(previous))
        require(os.environ.get("VIQ_UPSTREAM_SHA256") == h and
                os.environ.get("VIQ_UPSTREAM_URI") == str(self.receipt_path(previous)), "XCOM_RECEIPT_MISMATCH")
        return h

    def finish(self, stage, started, previous, output):
        pointer = write_new(self.receipt_path(stage), {
            "schema_version": 1, "dag_id": DAG_ID, "run_id": self.m["run_id"],
            "rid": self.m["resources"]["rid"], "manifest_sha256": self.manifest_hash,
            "stage": stage, "status": "success", "started_at": started, "ended_at": utc_now(),
            "previous_receipt_sha256": previous, "production_accepted": False, "output": output,
        })
        write_new(self.root / (stage + ".result.json"), pointer)
        return pointer

    def pin(self, key):
        head = self.s3.head_object(Bucket=self.bucket, Key=key)
        version = head.get("VersionId")
        require(bool(version) and version != "null", "OBJECT_VERSION_MISSING")
        body = self.s3.get_object(Bucket=self.bucket, Key=key, VersionId=version)["Body"].read()
        return {"key": key, "version": version, "sha256": sha256(body).hexdigest(), "bytes": len(body)}

    def get(self, pin, current=True):
        if current:
            require(self.s3.head_object(Bucket=self.bucket, Key=pin["key"]).get("VersionId") == pin["version"], "OBJECT_VERSION_DRIFT")
        body = self.s3.get_object(Bucket=self.bucket, Key=pin["key"], VersionId=pin["version"])["Body"].read()
        require(sha256(body).hexdigest() == pin["sha256"] and len(body) == pin["bytes"], "OBJECT_HASH_DRIFT")
        return body

    def keys(self, prefix=""):
        return sorted(x["Key"] for page in self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix) for x in page.get("Contents", []))

    def put_new(self, key, body):
        require(key.startswith(("input/", "lineage/")), "SIDECAR_PREFIX")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch="*")
        return self.pin(key)

    def snapshot(self, prefix):
        keys = [key for key in self.keys(prefix) if key.endswith(".parquet")]
        require(bool(keys), "EMPTY_PARQUET_DATASET")
        return [self.pin(key) for key in keys]

    def spark_read(self, spark, root, pins):
        for pin in pins:
            self.get(pin)
        frame = spark.read.option("basePath", f"s3a://{self.bucket}/{root.rstrip('/')}").parquet(
            *[f"s3a://{self.bucket}/{p['key']}" for p in pins])
        # The run is single-writer. Verify current versions again after materializing.
        frame = frame.cache(); frame.count()
        for pin in pins:
            require(self.s3.head_object(Bucket=self.bucket, Key=pin["key"]).get("VersionId") == pin["version"], "READ_DURING_OVERWRITE")
        return frame
