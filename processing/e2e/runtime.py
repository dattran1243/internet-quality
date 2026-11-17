"""Bounded processes and Spark session. All expensive imports are lazy."""
from __future__ import annotations

import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time

from processing.e2e.guards import require, safe_message


def bounded_process(argv, env, log_path, timeout, idle_timeout=None, progress="Bronze object written", isolate=True):
    """No shell, no automatic retry; kill the whole child process group on timeout."""
    kwargs = ({"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}) if isolate else {}
    reserved_log = Path(log_path).open("x", encoding="utf-8")
    try:
        proc = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace", **kwargs)
    except BaseException:
        reserved_log.close()
        raise
    lines = queue.Queue()
    def read_lines():
        for line in proc.stdout:
            lines.put(line)
        lines.put(None)
    threading.Thread(target=read_lines, daemon=True).start()
    begin = last_progress = time.monotonic()
    handlers = {}
    if threading.current_thread() is threading.main_thread():
        def interrupted(signum, frame):
            raise InterruptedError("E2E_SUBPROCESS_INTERRUPTED")
        for sig in (signal.SIGTERM, signal.SIGINT):
            handlers[sig] = signal.signal(sig, interrupted)
    try:
        with reserved_log as log:
            finished = False
            while not finished:
                now = time.monotonic()
                if now-begin > timeout or (idle_timeout and now-last_progress > idle_timeout):
                    raise TimeoutError("E2E_SUBPROCESS_DEADLINE")
                try:
                    line = lines.get(timeout=0.25)
                except queue.Empty:
                    continue
                if line is None:
                    finished = True
                else:
                    if progress in line:
                        last_progress = now
                    log.write(safe_message(line)); log.flush()
            require(proc.wait(timeout=5) == 0, "E2E_SUBPROCESS_FAILED")
    finally:
        if proc.poll() is None:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True)
            elif isolate:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            proc.wait(timeout=10)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def spark_session():
    from pyspark.sql import SparkSession
    require(os.environ.get("SPARK_MASTER") == "local[1]", "SPARK_MASTER_UNSAFE")
    builder = SparkSession.builder.master("local[1]").appName("viq-e2e-isolated")
    conf = {
        "spark.sql.shuffle.partitions": "2", "spark.default.parallelism": "1",
        "spark.sql.session.timeZone": "UTC", "spark.ui.enabled": "false",
        "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.hadoop.fs.s3a.endpoint": os.environ["MINIO_ENDPOINT"],
        "spark.hadoop.fs.s3a.access.key": os.environ["MINIO_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["MINIO_SECRET_KEY"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.threads.max": "4", "spark.hadoop.fs.s3a.max.total.tasks": "4",
        "spark.hadoop.fs.s3a.connection.maximum": "16", "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.fast.upload.active.blocks": "1",
        "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
        "spark.hadoop.fs.s3a.buffer.dir": "/opt/e2e/scratch/s3a",
        "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|connection[.]?string",
    }
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        require(spark.version == "3.5.3" and spark.sparkContext.master == "local[1]", "SPARK_VERSION_MASTER")
        require(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion() == "3.3.4", "HADOOP_VERSION")
        heap = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
        require(900*1024**2 <= heap <= 1100*1024**2, "SPARK_HEAP_NOT_1G")
        args = str(spark.sparkContext._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments())
        require("ActiveProcessorCount=1" in args and "ParallelGCThreads=1" in args, "JVM_THREADS_NOT_BOUND")
        for key in ("spark.default.parallelism", "spark.sql.shuffle.partitions"):
            require(spark.conf.get(key) == conf[key], "SPARK_CONF_DRIFT")
    except BaseException:
        spark.stop()
        raise
    return spark
