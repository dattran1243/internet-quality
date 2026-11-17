# Historical E2E — public preparation guide

This guide replaces an internal runbook containing historical runtime references.
Original runbook SHA256:
b855482b699302657268ff557a06f048312ac7aa8742291492a1226fba46670d
It is not an authorization or a ready-to-trigger replay recipe.

The original controller, guards, DAG and verification source are included.
Consumed manifests, approvals, resource attestations and resume checkpoints are
not exported. No original historical identity should be reused.

## Requirements before a new replay

1. Obtain the exact raw/probe archive inputs matching the immutable hashes in
   processing/e2e/contracts.py. Input data is not shipped.
2. Export VIQ_E2E_INPUT_DIR in the process environment for that input directory.
   The offline controller does not load .env automatically. Keep the inputs outside Git.
3. Choose a fresh run identity and resource namespace, with owner review.
4. Build and validate a new image against requirements-e2e.txt and its Dockerfile;
   pin the actual digest. Public source changes invalidate original source pins.
5. Prepare private metadata roles, Kafka topics, MinIO policy/versioning,
   ClickHouse users/schema and external networks. Use least privilege.
6. Review the legacy topology: guards and isolated Compose still bind exact
   service endpoints and expected networks. A new machine must prepare and
   validate a compatible topology or explicitly review changes and re-pin it.
7. Generate new source/deployment pins, verify resources/empty destinations,
   resource capacity and private file permissions, then obtain exact approval.

The prepare CLI requires an explicit --run-id. Its offline manifest builder
does not grant start/run permission. Original gate_b_only, source/input hash,
image, approval, namespace, mount and single-trigger guards remain in place.
The generated manifest path is local-only and ignored by Git.

The unit-test DEFAULT_RUN constant is synthetic and cannot be used by the
export CLI as an implicit runtime identity. No approved runnable manifest,
resume authority or admin provisioning evidence is included.
Historical recovery branches depend on their original private checkpoints;
shipping their source does not make those checkpoints portable.

See [MinIO preparation](VIQ_E2E_MINIO_OPERATOR_SETUP.md),
[deployment guide](../DEPLOYMENT.md), and
[historical acceptance summary](VIQ_E2E_FRESH_RECOVERY_FINAL_REPORT.md).
