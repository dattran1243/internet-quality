# New-machine deployment prerequisites

This export contains application source, tests, schemas and templates, not the
owner's running environment. No containers, migrations, ETL or E2E were executed
to validate this export. Compose validation is structural/offline.

## Local application development

Use Python 3.10, Java 17 for Spark 3.5.3, and Node matching the frontend lockfile
(Vite requires ^20.19.0 or >=22.12.0). Install the host requirements as described
in the [README](../README.md). Optional signed-registry/Parquet verification
branches additionally use [requirements-optional.txt](../requirements-optional.txt);
resolve and lock that environment independently. Airflow uses its own image.

Create .env from [.env.example](../.env.example), populate your own nonempty
MinIO/ClickHouse/Airflow secrets, and keep .env private. No reusable password is
provided. AIRFLOW_DATABASE_URL must be a complete postgresql+psycopg2 URL for
the airflow user/database on postgres:5432, with a URL-encoded password matching
AIRFLOW_POSTGRES_PASSWORD. Generate AIRFLOW_JWT_SECRET locally.
SIMPLE_AUTH_MANAGER_USERS maps a username to a role; it is not a password.

## Compose preparation

The root lab is named viq-public, Kafka viq-public-kafka and Airflow
viq-public-airflow. Fixed original container names are removed; root MinIO uses
a new project-managed volume. Review port collisions and choose your own
Compose project names when creating additional labs. No original volume is
imported by default.

Run these from the repository root to validate structure without services or
real secrets:

    docker compose --env-file .env.example config --no-interpolate --no-env-resolution --quiet
    docker compose --env-file .env.example -f infra/kafka/docker-compose.yml config --no-interpolate --no-env-resolution --quiet
    docker compose --env-file .env.example -f infra/airflow/docker-compose.yml config --no-interpolate --no-env-resolution --quiet

For deployment, use your populated .env with the same global --env-file option.
Blank required secrets intentionally prevent startup; offline structural PASS
does not prove authentication, connectivity, downloaded images or datasets.

Ports bind to loopback by default via VIQ_BIND_ADDRESS. Kafka is a single-broker
PLAINTEXT development service. host.docker.internal/host-gateway connectivity
must be configured for your Docker platform, especially when one Compose project
accesses another through host-published ports; loopback bindings may require a
reviewed shared-network setup instead. Do not expose these lab services publicly.
Cross-project networking has not been runtime-tested for this export.

Airflow logs use a managed volume, DAG source mounts are read-only, and new
metadata is separate from original resources. The Airflow image includes Java;
Spark S3 connectors must be supplied as required by the selected runner.
Dockerfile.spark supplies Hadoop AWS 3.3.4 and AWS SDK 1.12.262; its default
Silver command still requires explicit --start/--stop arguments for real work.

## Data and acceptance

Prepare empty destinations, schema, least-privilege users and scoped inputs
before any ingestion or load. No historical dataset is bundled beyond the
small sample JSON. Demo Explorer requires an independently loaded local
ClickHouse sandbox on port 18123; the root lab port 8123 is a different setup.
The HTML Explorer is served by Vite dev and is not a separate build entry yet.

Historical E2E requires new owner approval and the
[separate preparation guide](e2e/VIQ_E2E_RUNBOOK.md). It is not a one-command
deployment and is not authorized by publishing this source.
