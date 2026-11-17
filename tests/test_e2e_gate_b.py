"""Gate B only: synthetic unit fixtures/mocks, no broker/storage/ETL/container execution."""
from __future__ import annotations

import ast
import builtins
import copy
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import runpy
import socket
import sys
import tempfile
import types
import unittest
from unittest.mock import patch, Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from processing.e2e.contracts import STAGES, RAW_SHA256, canonical, digest, row_hash
from processing.e2e.guards import (GuardError, actual_context, gate_c, load_manifest,
                                  metadata_env, namespace, runtime_env, safe_message)
from processing.e2e.clickhouse import ClickHouse, render_schema, validate_sql
from processing.e2e.verification import compare_rows
from scripts.e2e_control import DEFAULT_RUN, make_manifest, terminal_audit


class GateBTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cached_manifest = make_manifest(DEFAULT_RUN)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="viq_gate_b_unit_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.m = copy.deepcopy(self.cached_manifest)

    def save(self, m=None):
        path = self.root/"manifest.json"; path.write_bytes(canonical(m or self.m)); return path

    def test_manifest_valid(self):
        self.assertEqual(load_manifest(self.save())["resources"], namespace(DEFAULT_RUN))

    def test_namespace_mutations_rejected(self):
        for key, value in (("database", "viq_demo_24h"), ("metadata_database", "airflow"),
                           ("bucket", "telecom-data"), ("topic", "network.raw.ripe_atlas.ping.v1"),
                           ("group", "bronze-ripe-atlas-ping-v1"), ("dlq", "other")):
            m = copy.deepcopy(self.m); m["resources"][key] = value
            with self.subTest(key=key), self.assertRaises(GuardError): load_manifest(self.save(m))

    def test_missing_names_fail_closed(self):
        for key in self.m["resources"]:
            m = copy.deepcopy(self.m); del m["resources"][key]
            with self.subTest(key=key), self.assertRaises(GuardError): load_manifest(self.save(m))

    def test_source_path_escape_rejected(self):
        for name in ("../outside.py", "C:/outside.py", "..\\outside.py"):
            m = copy.deepcopy(self.m); m["source_sha256"] = {name: "0"*64}
            with self.subTest(name=name), self.assertRaises(GuardError): load_manifest(self.save(m))

    def test_endpoint_fallback_rejected(self):
        for key, value in (("kafka", "localhost:9092"), ("minio", "http://localhost:9000"),
                           ("clickhouse_host", "localhost"), ("clickhouse_port", 8124)):
            m = copy.deepcopy(self.m); m["endpoints"][key] = value
            with self.subTest(key=key), self.assertRaises(GuardError): load_manifest(self.save(m))

    def test_input_and_ram_cannot_be_relaxed(self):
        for name, value in (("raw_sha256", "0"*64), ("minimum_vm_bytes", 1),
                            ("minimum_available_bytes", 1), ("dashboard_accepted", True), ("live_ingestion", True)):
            m = copy.deepcopy(self.m); m[name] = value
            with self.subTest(name=name), self.assertRaises(GuardError): load_manifest(self.save(m))

    def test_run_id_cannot_inject_sql_or_path(self):
        for value in ("../../telecom", "manual__x'; DROP DATABASE telecom", "", "manual__viq_e2e_20261111T150000Z_aB0000000000"):
            with self.subTest(value=value), self.assertRaises(GuardError): namespace(value)

    def test_gate_c_absent_or_false_blocks(self):
        path = self.save(); approval = self.root/"approval.json"
        approval.write_bytes(canonical({"approved": False}))
        with self.assertRaises(GuardError): gate_c(self.m, path, approval, "run")

    def test_gate_b_even_with_mock_approval_cannot_run(self):
        from processing.e2e.guards import file_hash
        path = self.save(); a = {"approved": True, "manifest_sha256": file_hash(path), "run_id": DEFAULT_RUN,
            "actions": ["run"], "approved_by": "UNIT_FIXTURE", "approved_at_utc": "2026-11-11T00:00:00Z"}
        approval = self.root/"unit_approval.json"; approval.write_bytes(canonical(a))
        with self.assertRaises(GuardError): gate_c(self.m, path, approval, "run")

    def test_approval_exact_manifest_required(self):
        path = self.save(); approval = self.root/"approval.json"
        approval.write_bytes(canonical({"approved": True, "manifest_sha256": "0"*64}))
        with self.assertRaises(GuardError): gate_c(self.m, path, approval, "start")

    def test_metadata_production_and_wrong_host_rejected(self):
        r = self.m["resources"]
        good = f"postgresql+psycopg2://{r['metadata_user']}:UNIT_PASSWORD@internet-quality-airflow-postgres:5432/{r['metadata_database']}"
        with patch.dict(os.environ, {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": good}, clear=True): metadata_env(self.m)
        for bad in (good.replace(r["metadata_database"], "airflow"), good.replace("internet-quality-airflow-postgres", "localhost"), ""):
            with patch.dict(os.environ, {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": bad}, clear=True), self.assertRaises(GuardError): metadata_env(self.m)

    def test_worker_env_has_no_production_fallback(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(GuardError): runtime_env(self.m)

    def test_actual_context_bound_to_task(self):
        env = {"VIQ_ACTUAL_DAG_ID": self.m["dag_id"], "VIQ_ACTUAL_RUN_ID": DEFAULT_RUN,
               "VIQ_ACTUAL_TASK_ID": STAGES[0], "VIQ_ACTUAL_TRY_NUMBER": "1"}
        with patch.dict(os.environ, env, clear=True): actual_context(self.m, STAGES[0])
        with patch.dict(os.environ, {**env, "VIQ_ACTUAL_TRY_NUMBER": "2"}, clear=True), self.assertRaises(GuardError): actual_context(self.m, STAGES[0])

    def test_kafka_original_defaults_preserved(self):
        with patch.dict(os.environ, {}, clear=True): cfg = runpy.run_path(str(ROOT/"messaging/kafka_config.py"))
        self.assertEqual(cfg["RIPE_ATLAS_PING_TOPIC"], "network.raw.ripe_atlas.ping.v1")
        self.assertEqual(cfg["RIPE_ATLAS_PING_DLQ_TOPIC"], "network.dlq.ripe_atlas.ping.v1")
        self.assertEqual(cfg["BRONZE_RIPE_ATLAS_CONSUMER_GROUP"], "bronze-ripe-atlas-ping-v1")
        self.assertEqual(cfg["KAFKA_BOOTSTRAP_SERVERS"], "localhost:9092")

    def test_kafka_explicit_overrides(self):
        r = self.m["resources"]
        env = {"RIPE_ATLAS_PING_TOPIC": r["topic"], "RIPE_ATLAS_PING_DLQ_TOPIC": r["dlq"],
               "BRONZE_RIPE_ATLAS_CONSUMER_GROUP": r["group"], "KAFKA_BOOTSTRAP_SERVERS": self.m["endpoints"]["kafka"]}
        with patch.dict(os.environ, env, clear=True): cfg = runpy.run_path(str(ROOT/"messaging/kafka_config.py"))
        for key, value in env.items(): self.assertEqual(cfg[key], value)

    def test_original_kafka_contract_still_used(self):
        from messaging.contracts import build_event_id, build_window_id
        window = build_window_id(source="ripe_atlas", dataset="ping", country_code="DE", measurement_id="177401244",
                                 window_start_utc="2026-10-18T13:00:00Z", window_stop_utc="2026-10-18T13:05:00Z")
        self.assertEqual(build_event_id(window_id=window, chunk_index=0), build_event_id(window_id=window, chunk_index=0))
        self.assertNotEqual(build_event_id(window_id=window, chunk_index=0), build_event_id(window_id=window, chunk_index=1))

    def test_archive_adapter_preserves_values_unit_fixture(self):
        from processing.e2e.contracts import PROBES
        from processing.e2e.ripe_archive_adapter import adapt_archive
        # Synthetic metadata for adapter unit tests only, never pipeline input.
        probes = [{"id": i, "country_code": "DE", "status": 1, "status_name": "Connected",
                   "latitude": 50.0, "longitude": 8.0, "asn_v4": 123} for i in PROBES]
        result = adapt_archive({"objects": probes})
        self.assertEqual(result[0]["geometry"]["coordinates"], [8.0, 50.0])
        self.assertEqual(result[0]["asn_v4"], 123)
        self.assertNotIn("address_v4", result[0])
        probes[0]["country_code"] = "VN"
        with self.assertRaises(GuardError): adapt_archive({"objects": probes})

    def test_sql_protected_and_mutation_rejected(self):
        db = self.m["resources"]["database"]
        for sql in ("DELETE FROM x", "ALTER TABLE x DELETE WHERE 1", "DROP TABLE x", "TRUNCATE TABLE x", "SELECT * FROM viq_demo_24h.x"):
            with self.subTest(sql=sql), self.assertRaises(GuardError): validate_sql(sql, db)
        with self.assertRaises(GuardError): validate_sql("SELECT 1", "telecom_g1")

    def test_schema_is_new_database_and_bound_view(self):
        template = (ROOT/"infra/clickhouse/e2e_schema.sql").read_text()
        sql = render_schema(self.m, template)
        self.assertIn("CREATE DATABASE "+self.m["resources"]["database"], sql)
        self.assertIn("e2e_run_id = '"+DEFAULT_RUN+"'", sql)
        self.assertNotIn("IF NOT EXISTS", sql)
        self.assertNotIn("{{", sql)

    def test_schema_rejects_foreign_database_even_if_not_demo(self):
        template = (ROOT/"infra/clickhouse/e2e_schema.sql").read_text()
        template = template.replace("CREATE TABLE {{database}}.run_receipts", "CREATE TABLE unapproved.run_receipts")
        with self.assertRaises(GuardError): render_schema(self.m, template)

    def test_insert_not_retried_on_ambiguous_transport(self):
        session = Mock(); session.post.side_effect = TimeoutError("UNIT_TRANSPORT_AMBIGUOUS")
        requests = types.SimpleNamespace(Session=lambda: session)
        with patch.dict(sys.modules, {"requests": requests}), patch.dict(os.environ, {"VIQ_CH_LOADER_PASSWORD": "UNIT_PASSWORD"}):
            client = ClickHouse(self.m)
            with self.assertRaises(TimeoutError): client.insert("run_receipts", [{"status": "verified"}], ("status",))
            self.assertEqual(session.post.call_count, 1)

    def test_insert_rejects_arbitrary_table(self):
        session = Mock(); requests = types.SimpleNamespace(Session=lambda: session)
        with patch.dict(sys.modules, {"requests": requests}), patch.dict(os.environ, {"VIQ_CH_LOADER_PASSWORD": "UNIT_PASSWORD"}):
            with self.assertRaises(GuardError): ClickHouse(self.m).insert("telecom.x", [{"x": 1}], ("x",))
            session.post.assert_not_called()

    def test_insert_real_column_names_with_mock_transport(self):
        from processing.e2e.contracts import GOLD_COLUMNS
        columns = (*GOLD_COLUMNS, "e2e_run_id", "raw_sha256", "gold_manifest_sha256")
        session = Mock(); session.post.return_value.status_code = 200; session.post.return_value.text = ""
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace(Session=lambda: session)}), \
             patch.dict(os.environ, {"VIQ_CH_LOADER_PASSWORD": "UNIT_PASSWORD"}):
            ClickHouse(self.m).insert("hourly_network_quality_rows", [{c: 0 for c in columns}], columns)
        sql = session.post.call_args.kwargs["params"]["query"]
        self.assertIn("asn_v4", sql); self.assertIn("e2e_run_id", sql)
        self.assertTrue(sql.startswith("INSERT INTO "+self.m["resources"]["database"]+"."))
        self.assertNotIn("DELETE", sql)

    def test_full_row_comparison_detects_count_equal_corruption(self):
        a = [{"id": 1, "metric": 2.0, "nullable": None}]
        with self.assertRaises(GuardError): compare_rows(a, [{"id": 1, "metric": 3.0, "nullable": None}], ("id",))
        with self.assertRaises(GuardError): compare_rows(a, [{"id": 1, "metric": 2.0, "nullable": 0}], ("id",))
        compare_rows(a, [{"id": 1, "metric": 2.0+1e-10, "nullable": None}], ("id",))

    def test_duplicate_analytical_keys_rejected(self):
        with self.assertRaises(GuardError): compare_rows([{"id": 1}, {"id": 1}], [{"id": 1}], ("id",))

    def test_multiset_hash_is_order_independent_but_preserves_multiplicity(self):
        self.assertEqual(row_hash([{"x": 1}, {"x": 2}]), row_hash([{"x": 2}, {"x": 1}]))
        self.assertNotEqual(row_hash([{"x": 1}]), row_hash([{"x": 1}, {"x": 1}]))

    def test_quality_expected_is_average_across_probes_not_weighted_by_samples(self):
        from processing.e2e.verification import expected_quality
        rows = [{"probe_id": 1, "avg_rtt_ms": 100.0, "p95_rtt_ms": 110.0, "sample_count": 10,
                 "total_packets_sent": 30, "total_packets_received": 27},
                {"probe_id": 2, "avg_rtt_ms": 200.0, "p95_rtt_ms": 210.0, "sample_count": 100,
                 "total_packets_sent": 300, "total_packets_received": 300}]
        result = expected_quality(rows)[0]
        self.assertEqual(result["avg_rtt_ms"], 150.0)
        self.assertEqual(result["sample_count"], 110)
        self.assertAlmostEqual(result["packet_loss_pct"], 3*100/330)

    def test_object_version_drift_rejected_before_read(self):
        from processing.e2e.io import Evidence
        e = Evidence(self.m, self.save()); e._s3 = Mock()
        e._s3.head_object.return_value = {"VersionId": "another-version"}
        with self.assertRaises(GuardError): e.get({"key": "gold/a.parquet", "version": "pinned-version", "sha256": "0"*64, "bytes": 1})
        e._s3.get_object.assert_not_called()

    def test_bounded_timeout_kills_only_child_group_mock(self):
        from processing.e2e.runtime import bounded_process
        proc = Mock(); proc.pid = 123456; proc.stdout = iter([]); proc.poll.return_value = None; proc.wait.return_value = 0
        with patch("subprocess.Popen", return_value=proc), patch("time.monotonic", side_effect=[0, 100]), \
             patch("os.killpg", create=True) as kill, patch("subprocess.run"), self.assertRaises(TimeoutError):
            bounded_process(["UNIT_MOCK_PROCESS"], {}, self.root/"unit.log", 1)
        if os.name != "nt": self.assertEqual(kill.call_count, 1)

    def test_nested_consumer_stays_in_outer_process_group_mock(self):
        from processing.e2e.runtime import bounded_process
        proc = Mock(); proc.stdout = iter([]); proc.poll.return_value = None; proc.wait.return_value = 0
        with patch("subprocess.Popen", return_value=proc) as popen, patch("time.monotonic", side_effect=[0, 100]), \
             patch("os.killpg", create=True) as kill, patch("subprocess.run"), self.assertRaises(TimeoutError):
            bounded_process(["UNIT_MOCK_CONSUMER"], {}, self.root/"unit.log", 1, isolate=False)
        self.assertNotIn("start_new_session", popen.call_args.kwargs)
        kill.assert_not_called()

    def test_existing_process_log_blocks_spawn_before_any_work(self):
        from processing.e2e.runtime import bounded_process
        log = self.root/"already_attempted.log"; log.write_text("UNIT_FIXTURE")
        with patch("subprocess.Popen") as popen, self.assertRaises(FileExistsError):
            bounded_process(["UNIT_MOCK_PROCESS"], {}, log, 1)
        popen.assert_not_called()

    def test_non_finite_numbers_rejected(self):
        with self.assertRaises(ValueError): canonical({"x": float('nan')})

    def test_secrets_redacted(self):
        with patch.dict(os.environ, {"MINIO_SECRET_KEY": "UNIT_SECRET_ABC", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "UNIT_DSN_ABC"}):
            self.assertEqual(safe_message("UNIT_SECRET_ABC UNIT_DSN_ABC"), "[REDACTED] [REDACTED]")

    def test_encoded_dsn_password_fragments_redacted(self):
        with patch.dict(os.environ, {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://unit:UNIT%21PASSWORD@unit:5432/unit"}):
            self.assertEqual(safe_message("UNIT%21PASSWORD UNIT!PASSWORD"), "[REDACTED] [REDACTED]")

    def test_imports_have_no_service_io(self):
        modules = ["processing.e2e."+p.stem for p in (ROOT/"processing/e2e").glob("*.py") if p.stem != "__init__"]
        modules += ["scripts.e2e_stage", "scripts.e2e_control", "scripts.e2e_bootstrap", "api.e2e_isolated"]
        original_import = builtins.__import__
        def no_clients(name, *args, **kwargs):
            if name.split('.')[0] in {"requests", "boto3", "confluent_kafka", "pyspark", "psycopg2", "fastapi", "airflow"}:
                raise AssertionError("EAGER_CLIENT_IMPORT:" + name)
            return original_import(name, *args, **kwargs)
        with patch.object(socket, "create_connection", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch.object(socket.socket, "connect", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch("builtins.__import__", side_effect=no_clients), \
             patch("subprocess.Popen", side_effect=AssertionError("PROCESS_FORBIDDEN")):
            for name in modules:
                path = ROOT.joinpath(*name.split('.')).with_suffix('.py')
                runpy.run_path(str(path), run_name="gate_b_import_probe")

    def test_api_factory_without_manifest_refuses_before_production_import(self):
        from api.e2e_isolated import create_app
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(KeyError): create_app()

    def test_dag_import_and_edges_with_mock_sdk(self):
        captured = {}; nodes = []
        class Dag:
            def __init__(self, **kwargs): captured.update(kwargs)
            def __enter__(self): return self
            def __exit__(self, *args): pass
        class Task:
            def __init__(self, settings): self.settings = settings
            def override(self, **kwargs): return Task({**self.settings, **kwargs})
            def __call__(self, stage, upstream):
                node = {"stage": stage, "upstream": upstream, **self.settings}; nodes.append(node); return node
        def task(**settings): return lambda fn: Task(settings)
        sdk = types.ModuleType("airflow.sdk"); sdk.DAG = Dag; sdk.task = task; sdk.get_current_context = Mock()
        with patch.dict(sys.modules, {"airflow": types.ModuleType("airflow"), "airflow.sdk": sdk}):
            runpy.run_path(str(ROOT/"infra/airflow/e2e_dags/viq_e2e_isolated_once.py"))
        self.assertIsNone(captured["schedule"])
        self.assertTrue(captured["is_paused_upon_creation"])
        self.assertFalse(captured["catchup"])
        self.assertEqual(captured["max_active_runs"], 1); self.assertEqual(captured["max_active_tasks"], 1)
        self.assertEqual([x["task_id"] for x in nodes], list(STAGES))
        for i, node in enumerate(nodes):
            self.assertIs(node["upstream"], nodes[i-1] if i else None)
            self.assertEqual(node["retries"], 0); self.assertEqual(node["trigger_rule"], "all_success")
        sdk.get_current_context.assert_not_called()

    def test_terminal_not_success_cannot_pass(self):
        with self.assertRaises(GuardError): terminal_audit(self.m, self.root, {"dag_run_id": DEFAULT_RUN, "state": "running"}, [])

    def test_terminal_missing_task_cannot_pass(self):
        with self.assertRaises(GuardError): terminal_audit(self.m, self.root, {"dag_run_id": DEFAULT_RUN, "state": "success", "end_date": "2026-11-11T00:00:01Z"}, [])

    def test_terminal_failed_task_cannot_pass_even_if_dag_is_success(self):
        tasks = [{"task_id": stage, "state": "success", "try_number": 1,
                  "start_date": "2026-11-11T00:00:00Z", "end_date": "2026-11-11T00:00:01Z"} for stage in STAGES]
        tasks[3]["state"] = "failed"
        with self.assertRaises(GuardError):
            terminal_audit(self.m, self.root, {"dag_run_id": DEFAULT_RUN, "state": "success", "end_date": "2026-11-11T00:00:02Z"}, tasks)

    def test_terminal_audit_only_accepts_complete_chain_unit_fixture(self):
        from processing.e2e.guards import file_hash
        from datetime import timedelta
        # Synthetic receipts inside TemporaryDirectory, not application evidence or RIPE data.
        self.m["_manifest_sha256"] = file_hash(self.save())
        receipts = self.root/"receipts"; receipts.mkdir(); tasks = []; previous = None
        base = datetime(2026, 11, 11, tzinfo=timezone.utc)
        for index, stage in enumerate(STAGES):
            start = (base+timedelta(seconds=index*2)).isoformat()
            end = (base+timedelta(seconds=index*2+1)).isoformat()
            path = receipts/(stage+".json")
            path.write_bytes(canonical({"dag_id": self.m["dag_id"], "run_id": DEFAULT_RUN,
                "stage": stage, "status": "success", "manifest_sha256": self.m["_manifest_sha256"],
                "previous_receipt_sha256": previous, "started_at": start, "ended_at": end,
                "output": {"status": "DATA_CHECKS_PASSED"}}))
            previous = file_hash(path)
            tasks.append({"task_id": stage, "state": "success", "try_number": 1, "start_date": start, "end_date": end})
        result = terminal_audit(self.m, self.root, {"dag_run_id": DEFAULT_RUN, "state": "success", "end_date": tasks[-1]["end_date"]}, tasks)
        self.assertFalse(result["dashboard_accepted"])
        self.assertFalse((self.root/"FINAL_E2E_RESULT.json").exists())
        tasks[-1]["try_number"] = 2
        with self.assertRaises(GuardError): terminal_audit(self.m, self.root, result["dagrun"], tasks)

    def test_controller_without_approval_does_not_call_docker(self):
        from scripts.e2e_control import main
        manifest = self.save()
        with patch.object(sys, "argv", ["e2e_control.py", "start", "--manifest", str(manifest)]), \
             patch("scripts.e2e_control.docker", side_effect=AssertionError("DOCKER_FORBIDDEN")), self.assertRaises(GuardError):
            main()


if __name__ == "__main__":
    unittest.main(verbosity=2)
