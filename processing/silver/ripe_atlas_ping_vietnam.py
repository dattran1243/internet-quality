"""Enrich selected Vietnam ping facts with the current probe snapshot."""

from dataclasses import dataclass
from typing import Iterable

from processing.silver.ripe_atlas_probe import validate_probe_dimension


PING_COLUMNS = [
    "event_time_utc", "measurement_id", "probe_id", "target",
    "destination_address", "address_family", "protocol", "rtt_min_ms",
    "rtt_avg_ms", "rtt_max_ms", "packets_sent", "packets_received",
    "packet_loss_pct", "ingested_at_utc", "validation_status",
    "year", "month", "day", "hour",
]

PROBE_ENRICHMENT_COLUMNS = [
    "country_code", "asn_v4", "asn_v6", "latitude", "longitude",
    "status_id", "status_name", "description", "is_anchor", "is_public",
    "prefix_v4", "prefix_v6", "probe_snapshot_at_utc",
]

ENRICHED_COLUMNS = PING_COLUMNS + PROBE_ENRICHMENT_COLUMNS


@dataclass(frozen=True)
class JoinMetrics:
    """Coverage and cardinality metrics calculated around the join."""

    selected_measurement_count: int
    input_ping_rows: int
    input_distinct_probes: int
    probe_dimension_rows: int
    matched_ping_rows: int
    unmatched_ping_rows: int
    matched_distinct_probes: int
    unmatched_distinct_probes: int
    output_rows: int
    unmatched_probe_ids: tuple[int, ...]


def normalize_measurement_ids(measurement_ids: Iterable[int]) -> tuple[int, ...]:
    """Return a deterministic non-empty set of positive measurement IDs."""
    normalized = tuple(sorted(set(measurement_ids)))
    if not normalized or any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in normalized):
        raise ValueError("measurement_ids must contain positive integers")
    return normalized


def filter_selected_measurements(ping_df, measurement_ids: Iterable[int]):
    """Filter facts before the join to the explicitly selected measurements."""
    from pyspark.sql import functions as f

    selected = normalize_measurement_ids(measurement_ids)
    return ping_df.filter(f.col("measurement_id").isin(list(selected)))


def enrich_ping_vietnam(
        ping_df, probes_df, measurement_ids: Iterable[int]):
    """Validate and broadcast-join selected facts to current VN probes."""
    from pyspark.sql import functions as f

    selected_ids = normalize_measurement_ids(measurement_ids)
    selected_ping = filter_selected_measurements(
        ping_df, selected_ids
    ).cache()
    probes = probes_df.cache()
    validate_probe_dimension(probes, "VN", "Connected")

    probe_keys = probes.select("probe_id")
    matched_ping = selected_ping.join(probe_keys, "probe_id", "left_semi")
    unmatched_ping = selected_ping.join(probe_keys, "probe_id", "left_anti")
    input_rows = selected_ping.count()
    matched_rows = matched_ping.count()
    unmatched_rows = unmatched_ping.count()
    input_distinct = selected_ping.select("probe_id").distinct().count()
    matched_distinct = matched_ping.select("probe_id").distinct().count()
    unmatched_ids = tuple(
        row.probe_id
        for row in unmatched_ping.select("probe_id").where(
            f.col("probe_id").isNotNull()
        ).distinct().orderBy("probe_id").collect()
    )

    ping = selected_ping.alias("ping")
    dimension = f.broadcast(probes).alias("probe")
    joined = ping.join(
        dimension,
        f.col("ping.probe_id") == f.col("probe.probe_id"),
        "inner",
    ).select(
        *[f.col(f"ping.{name}").alias(name) for name in PING_COLUMNS],
        f.col("probe.country_code"),
        f.col("probe.asn_v4"),
        f.col("probe.asn_v6"),
        f.col("probe.latitude"),
        f.col("probe.longitude"),
        f.col("probe.status_id"),
        f.col("probe.status_name"),
        f.col("probe.description"),
        f.col("probe.is_anchor"),
        f.col("probe.is_public"),
        f.col("probe.prefix_v4"),
        f.col("probe.prefix_v6"),
        f.col("probe.snapshot_at_utc").alias("probe_snapshot_at_utc"),
    ).cache()
    output_rows = joined.count()
    if output_rows != matched_rows:
        raise ValueError(
            "Probe join multiplied ping rows: "
            f"matched={matched_rows}, output={output_rows}"
        )
    if output_rows == 0:
        raise ValueError("Vietnam probe join is empty")
    metrics = JoinMetrics(
        selected_measurement_count=len(selected_ids),
        input_ping_rows=input_rows,
        input_distinct_probes=input_distinct,
        probe_dimension_rows=probes.count(),
        matched_ping_rows=matched_rows,
        unmatched_ping_rows=unmatched_rows,
        matched_distinct_probes=matched_distinct,
        unmatched_distinct_probes=len(unmatched_ids),
        output_rows=output_rows,
        unmatched_probe_ids=unmatched_ids,
    )
    return joined, metrics


def validate_enriched_ping(
        df, measurement_ids: Iterable[int], expected_rows: int | None = None
        ) -> None:
    """Fail on invalid enriched facts or a join cardinality violation."""
    from pyspark.sql import functions as f

    selected_ids = normalize_measurement_ids(measurement_ids)
    row_count = df.count()
    errors: list[str] = []
    if row_count == 0:
        errors.append("enriched ping is empty")
    if expected_rows is not None and row_count != expected_rows:
        errors.append(
            f"output rows {row_count} do not equal matched rows {expected_rows}"
        )
    rules = [
        (f.col("probe_id").isNull(), "probe_id must be non-null"),
        (f.col("measurement_id").isNull()
         | ~f.col("measurement_id").isin(list(selected_ids)),
         "measurement_id is outside the selected set"),
        (f.col("country_code").isNull() | (f.col("country_code") != "VN"),
         "country_code must be VN"),
        (f.col("status_name").isNull() | (f.col("status_name") != "Connected"),
         "status_name must be Connected"),
        (f.col("latitude").isNotNull()
         & ~f.col("latitude").between(-90, 90), "latitude is invalid"),
        (f.col("longitude").isNotNull()
         & ~f.col("longitude").between(-180, 180), "longitude is invalid"),
        (f.col("packet_loss_pct").isNotNull()
         & ~f.col("packet_loss_pct").between(0, 100),
         "packet_loss_pct is invalid"),
        (f.col("rtt_min_ms").isNotNull() & (f.col("rtt_min_ms") < 0),
         "rtt_min_ms is invalid"),
        (f.col("rtt_avg_ms").isNotNull() & (f.col("rtt_avg_ms") < 0),
         "rtt_avg_ms is invalid"),
        (f.col("rtt_max_ms").isNotNull() & (f.col("rtt_max_ms") < 0),
         "rtt_max_ms is invalid"),
        (f.col("event_time_utc").isNull(), "event_time_utc must be non-null"),
        (f.col("probe_snapshot_at_utc").isNull(),
         "probe_snapshot_at_utc must be non-null"),
    ]
    for condition, message in rules:
        if df.filter(condition).limit(1).count():
            errors.append(message)
    if errors:
        raise ValueError("Invalid Vietnam enriched ping: " + "; ".join(errors))


def enriched_summary(df) -> dict[str, object]:
    """Collect compact dimensions and missing-value metrics for logging."""
    from pyspark.sql import functions as f

    row = df.agg(
        f.countDistinct("measurement_id").alias("distinct_measurements"),
        f.countDistinct("probe_id").alias("distinct_output_probes"),
        f.countDistinct("asn_v4").alias("distinct_asn_v4"),
        f.sum((f.col("latitude").isNull() | f.col("longitude").isNull())
              .cast("int")).alias("missing_coordinates"),
        f.sum(f.col("asn_v4").isNull().cast("int")).alias("missing_asn_v4"),
    ).first()
    return {
        **{name: int(row[name] or 0) for name in row.asDict()},
        "measurement_ids": [
            item.measurement_id
            for item in df.select("measurement_id").distinct()
            .orderBy("measurement_id").collect()
        ],
        "country_distribution": [
            item.asDict() for item in df.groupBy("country_code").count().collect()
        ],
        "status_distribution": [
            item.asDict() for item in df.groupBy("status_name").count().collect()
        ],
    }
