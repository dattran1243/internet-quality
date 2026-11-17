# MinIO — public operator prerequisites

Public-safe replacement for the internal operator setup.
Original setup document SHA256:
1efe3319cb25cc89b7af994b970e9c6c3bfb3414e39bdf1f1eb8b00d40e9d8cf

For a fresh isolated replay, the owner must create a new dedicated bucket,
identity and least-privilege policy bound to the new manifest namespace.
Do not use admin/root credentials in workers or reuse historical resources.
The original implementation requires bucket versioning and validation of empty
objects, versions and delete markers before resource creation/ingestion.

Keep actual credentials, policies bound to a real deployment, approvals and
runtime control files outside public Git. The historical policy and consumed
manifest are not distributed as deployable defaults.
Retain private evidence and verify read-back with the reviewed controller.
No MinIO operation was performed during public export preparation.

The ordinary local-demo Compose uses separate lab resources and explicit
owner-supplied credentials. This is distinct from the strict historical E2E
resource/attestation workflow. See [public preparation guide](VIQ_E2E_RUNBOOK.md).
