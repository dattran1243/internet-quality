FROM apache/airflow:3.3.2-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "requests>=2.31,<3" \
    "boto3>=1.34,<2" \
    "confluent-kafka>=2.6,<3" \
    "pycountry==26.2.16" \
    "python-dotenv>=1.0,<2" \
    "pydantic-settings>=2,<3" \
    "pyspark==3.5.3" \
    "psycopg2-binary>=2.9,<3"