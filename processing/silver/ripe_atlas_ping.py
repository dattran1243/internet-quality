"""RIPE Atlas ping Bronze-to-Silver transformation."""

from typing import Any


SILVER_COLUMNS = [
    "event_time_utc", "measurement_id", "probe_id", "target",
    "destination_address", "address_family", "protocol", "rtt_min_ms",
    "rtt_avg_ms", "rtt_max_ms", "packets_sent", "packets_received",
    "packet_loss_pct", "ingested_at_utc", "validation_status",
]


def packet_counts(record: dict[str, Any]) -> tuple[int | None, int | None]:
    """Use valid counters, otherwise infer exact counts from packet results."""
    sent, received = record.get("sent"), record.get("rcvd")
    if isinstance(sent, int) and not isinstance(sent, bool) and sent >= 0 and \
            isinstance(received, int) and not isinstance(received, bool) and \
            0 <= received <= sent:
        return sent, received
    results = record.get("result")
    if isinstance(results, list):
        packets = [item for item in results if isinstance(item, dict)]
        if len(packets) == len(results):
            return len(packets), sum(
                1 for item in packets
                if isinstance(item.get("rtt"), (int, float))
            )
    return None, None


def calculate_packet_loss(sent: int | None,
                          received: int | None) -> float | None:
    """Calculate bounded packet loss only from internally valid counters."""
    if sent is None or received is None or sent <= 0 or received < 0 or received > sent:
        return None
    return (sent - received) / sent * 100.0


def transform_record(record: dict[str, Any],
                     ingested_at_utc: str | None = None) -> dict[str, Any]:
    """Normalize one tolerant raw result for unit tests and documentation."""
    sent, received = packet_counts(record)
    protocol = record.get("proto")
    return {
        "event_time_utc": record.get("timestamp"),
        "measurement_id": record.get("msm_id"),
        "probe_id": record.get("prb_id"),
        "target": record.get("dst_name") or record.get("target"),
        "destination_address": record.get("dst_addr"),
        "address_family": record.get("af"),
        "protocol": str(protocol) if protocol is not None else None,
        "rtt_min_ms": record.get("min"), "rtt_avg_ms": record.get("avg"),
        "rtt_max_ms": record.get("max"), "packets_sent": sent,
        "packets_received": received,
        "packet_loss_pct": calculate_packet_loss(sent, received),
        "ingested_at_utc": ingested_at_utc,
    }


def silver_schema():
    """Return the explicit Spark schema (imported lazily for light unit tests)."""
    from pyspark.sql.types import (DoubleType, IntegerType, LongType,
                                   StringType, StructField, StructType,
                                   TimestampType)
    return StructType([
        StructField("event_time_utc", TimestampType(), True),
        StructField("measurement_id", LongType(), True),
        StructField("probe_id", LongType(), True),
        StructField("target", StringType(), True),
        StructField("destination_address", StringType(), True),
        StructField("address_family", IntegerType(), True),
        StructField("protocol", StringType(), True),
        StructField("rtt_min_ms", DoubleType(), True),
        StructField("rtt_avg_ms", DoubleType(), True),
        StructField("rtt_max_ms", DoubleType(), True),
        StructField("packets_sent", IntegerType(), True),
        StructField("packets_received", IntegerType(), True),
        StructField("packet_loss_pct", DoubleType(), True),
        StructField("ingested_at_utc", TimestampType(), True),
        StructField("validation_status", StringType(), False),
    ])


def transform_bronze(bronze_df):
    """Explode envelopes, normalize fields, and retain invalid rows with a flag."""
    from pyspark.sql import functions as f

    rows = bronze_df.select(f.explode_outer("records").alias("r"),
                            f.col("metadata.ingested_at_utc").alias("ingested"))
    record_fields = set(rows.schema["r"].dataType.fieldNames())
    if "dst_name" in record_fields and "target" in record_fields:
        target = f.coalesce(f.col("r.dst_name"), f.col("r.target"))
    elif "dst_name" in record_fields:
        target = f.col("r.dst_name")
    elif "target" in record_fields:
        target = f.col("r.target")
    else:
        target = f.lit(None)

    result_array = f.col("r.result")
    array_sent = f.when(result_array.isNotNull(), f.size(result_array))
    result_type = rows.schema["r"].dataType["result"].dataType
    result_fields = set(result_type.elementType.fieldNames())
    if "rtt" in result_fields:
        array_received = f.when(result_array.isNotNull(), f.aggregate(
            result_array, f.lit(0),
            lambda count, item:
            count + f.when(item["rtt"].isNotNull(), 1).otherwise(0)))
    else:
        array_received = f.when(result_array.isNotNull(), f.lit(0))

    raw_sent = (f.col("r.sent") if "sent" in record_fields
                else f.lit(None).cast("int"))
    raw_received = (f.col("r.rcvd") if "rcvd" in record_fields
                    else f.lit(None).cast("int"))
    counters_valid = (raw_sent >= 0) & (raw_received >= 0) & \
        (raw_received <= raw_sent)
    sent = f.when(counters_valid, raw_sent).otherwise(array_sent).cast("int")
    received = f.when(
        counters_valid, raw_received
    ).otherwise(array_received).cast("int")
    loss = f.when((sent > 0) & received.between(0, sent),
                  (sent - received) / sent * f.lit(100.0))
    no_successful_response = received == 0
    raw_rtt_min = f.col("r.min").cast("double")
    raw_rtt_avg = f.col("r.avg").cast("double")
    raw_rtt_max = f.col("r.max").cast("double")
    rtt_min = f.when(
        no_successful_response & (raw_rtt_min < 0), f.lit(None).cast("double")
    ).otherwise(raw_rtt_min)
    rtt_avg = f.when(
        no_successful_response & (raw_rtt_avg < 0), f.lit(None).cast("double")
    ).otherwise(raw_rtt_avg)
    rtt_max = f.when(
        no_successful_response & (raw_rtt_max < 0), f.lit(None).cast("double")
    ).otherwise(raw_rtt_max)
    df = rows.select(
        f.to_timestamp(f.from_unixtime(f.col("r.timestamp"))).alias("event_time_utc"),
        f.col("r.msm_id").cast("long").alias("measurement_id"),
        f.col("r.prb_id").cast("long").alias("probe_id"),
        target.cast("string").alias("target"),
        f.col("r.dst_addr").cast("string").alias("destination_address"),
        f.col("r.af").cast("int").alias("address_family"),
        f.col("r.proto").cast("string").alias("protocol"),
        rtt_min.alias("rtt_min_ms"),
        rtt_avg.alias("rtt_avg_ms"),
        rtt_max.alias("rtt_max_ms"), sent.alias("packets_sent"),
        received.alias("packets_received"), loss.alias("packet_loss_pct"),
        f.to_timestamp("ingested").alias("ingested_at_utc"),
    )
    valid = (f.col("event_time_utc").isNotNull() &
             (f.col("measurement_id") > 0) & (f.col("probe_id") > 0) &
             (f.col("rtt_min_ms").isNull() | (f.col("rtt_min_ms") >= 0)) &
             (f.col("rtt_avg_ms").isNull() | (f.col("rtt_avg_ms") >= 0)) &
             (f.col("rtt_max_ms").isNull() | (f.col("rtt_max_ms") >= 0)) &
             (f.col("packet_loss_pct").isNull() |
              f.col("packet_loss_pct").between(0, 100)) &
             (f.col("packets_sent").isNull() | (f.col("packets_sent") >= 0)) &
             (f.col("packets_received").isNull() | (f.col("packets_received") >= 0)) &
             (f.col("packets_sent").isNull() | f.col("packets_received").isNull() |
              (f.col("packets_received") <= f.col("packets_sent"))))
    return (df.withColumn("validation_status",
                          f.when(valid, "valid").otherwise("invalid"))
            .withColumn("year", f.date_format("event_time_utc", "yyyy"))
            .withColumn("month", f.date_format("event_time_utc", "MM"))
            .withColumn("day", f.date_format("event_time_utc", "dd"))
            .withColumn("hour", f.date_format("event_time_utc", "HH")))
