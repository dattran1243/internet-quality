"""INSERT-only sandbox loader transport. No credentials in SQL or URL."""
from __future__ import annotations

import json
import os
import re

from processing.e2e.contracts import GOLD_COLUMNS, canonical, digest, row_hash
from processing.e2e.guards import namespace, require
from processing.e2e.verification import ch_rows


def validate_sql(sql, database):
    require(bool(re.fullmatch(r"viq_e2e_e[a-f0-9]{16}", database)), "CH_DATABASE_NOT_E2E")
    require(not re.search(r"\b(DELETE|ALTER|DROP|TRUNCATE|UPDATE|SYSTEM|ATTACH|DETACH|RENAME)\b", sql, re.I), "SQL_MUTATION_FORBIDDEN")
    require(not re.search(r"\b(telecom|telecom_g1|viq_demo_24h|airflow)\b", sql, re.I), "SQL_PROTECTED_DATABASE")


def render_schema(m, template):
    r = m["resources"]; require(r == namespace(m["run_id"]), "DDL_RESOURCE_BINDING")
    sql = template.replace("{{database}}", r["database"]).replace("{{run_id}}", m["run_id"])
    require("{{" not in sql and "IF NOT EXISTS" not in sql.upper(), "DDL_UNRESOLVED_OR_REUSE")
    validate_sql(sql, r["database"])
    clean = re.sub(r"--[^\n]*", "", sql)
    statements = [s.strip() for s in clean.split(';') if s.strip()]
    require(len(statements) == 4 and statements[0] == "CREATE DATABASE " + r["database"], "DDL_STATEMENT_SET")
    targets = re.findall(r"CREATE (?:TABLE|VIEW) ([a-z0-9_]+)\.([a-z0-9_]+)", clean)
    require(set(targets) == {(r["database"], table) for table in
            ("hourly_network_quality_rows", "hourly_network_quality", "run_receipts")} and len(targets) == 3, "DDL_TARGET_SCOPE")
    require(all(db == r["database"] for db, table in re.findall(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b", clean)), "DDL_FOREIGN_DATABASE")
    return sql


class ClickHouse:
    def __init__(self, m):
        import requests
        self.m = m; self.db = m["resources"]["database"]
        require(self.db == namespace(m["run_id"])["database"], "CH_DATABASE_BINDING")
        self.url = f"http://{m['endpoints']['clickhouse_host']}:{m['endpoints']['clickhouse_port']}/"
        self.session = requests.Session(); self.session.trust_env = False
        self.auth = (m["resources"]["ch_loader_user"], os.environ["VIQ_CH_LOADER_PASSWORD"])

    def _request(self, sql, data=None):
        validate_sql(sql, self.db)
        # requests' default adapter has zero retries. An ambiguous INSERT is never retried.
        response = self.session.post(self.url, params={"database": self.db, "query": sql,
            "max_threads": 1, "max_insert_threads": 1, "max_memory_usage": 268435456,
            "output_format_json_quote_64bit_integers": 0},
            data=data, auth=self.auth, timeout=(5, 30))
        require(response.status_code == 200, "CH_REQUEST_FAILED_OR_INSERT_AMBIGUOUS")
        return response.text

    def select(self, sql):
        require(sql.lstrip().upper().startswith("SELECT ") and ";" not in sql, "SELECT_ONLY")
        return [json.loads(line) for line in self._request(sql + " FORMAT JSONEachRow").splitlines() if line.strip()]

    def insert(self, table, rows, columns):
        require(table in ("hourly_network_quality_rows", "run_receipts") and bool(rows), "INSERT_TARGET")
        require(all(set(row) == set(columns) for row in rows), "INSERT_COLUMN_BINDING")
        require(all(re.fullmatch(r"[a-z_][a-z0-9_]*", c) for c in columns), "INSERT_IDENTIFIER")
        sql = f"INSERT INTO {self.db}.{table} ({','.join(columns)}) FORMAT JSONEachRow"
        self._request(sql, b"\n".join(canonical(row) for row in rows) + b"\n")

    def empty(self):
        for table in ("hourly_network_quality_rows", "hourly_network_quality", "run_receipts"):
            require(self.select(f"SELECT count() AS n FROM {self.db}.{table}")[0]["n"] == 0, "CH_TARGET_NOT_EMPTY")


def gold_from_pins(evidence):
    import io
    import pyarrow.parquet as pq
    g = evidence.read("gold_full_hour")
    rows = []
    for pin in g["objects"]:
        rows.extend(pq.ParquetFile(io.BytesIO(evidence.get(pin))).read().to_pylist())
    require(len(rows) == 24, "GOLD_PARQUET_ROW_COUNT")
    normalized = ch_rows(rows)
    require(row_hash(normalized) == g["clickhouse_rows_sha256"], "GOLD_NORMALIZED_HASH")
    return normalized
