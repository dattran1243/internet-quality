const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL ??
    "http://127.0.0.1:8000";


export const DEFAULT_COUNTRY_CODE = "VN";


export type HealthResponse = {
    status: string;
    api: string;
    clickhouse: string;
};


export type Measurement = {
    measurement_id: number;
    target: string;

    first_seen_utc: string;
    last_seen_utc: string;

    probe_count: number;
    hourly_probe_rows: number;
};


export type HourlyQualityPoint = {
    hour_start_utc: string;

    measurement_id: number;
    target: string;

    probe_count: number;
    sample_count: number;

    avg_rtt_ms: number | null;
    avg_probe_p95_rtt_ms: number | null;

    packet_loss_pct: number | null;

    total_packets_sent: number;
    total_packets_received: number;
};


export type IncidentRecord = {
    hour_start_utc: string;

    measurement_id: number;
    target: string;

    observed_probe_count: number;
    eligible_probe_count: number;

    warning_probe_count: number;
    critical_probe_count: number;
    affected_probe_count: number;

    observed_asn_count: number;
    eligible_asn_count: number;

    warning_asn_count: number;
    critical_asn_count: number;
    affected_asn_count: number;

    max_probe_latency_ratio: number | null;
    max_probe_latency_delta_ms: number | null;
    max_probe_packet_loss_pct: number | null;

    max_asn_affected_ratio: number | null;

    coverage_status: string | null;
    probe_coverage_ratio: number | null;

    incident_status: string;
    incident_candidate: boolean;

    severity_score: number | null;

    signal_types: string;
    incident_summary: string;
};


export type ProbeMapPoint = {
    hour_start_utc: string;

    measurement_id: number;
    probe_id: number;
    target: string;

    country_code: string | null;

    asn_v4: number | null;
    asn_v6: number | null;

    latitude: number;
    longitude: number;

    is_anchor: boolean | null;

    sample_count: number;

    avg_rtt_ms: number | null;
    p95_rtt_ms: number | null;
    packet_loss_pct: number | null;

    measurement_incident_status: string | null;
};


async function request<T>(
    path: string,
): Promise<T> {
    const response = await fetch(
        `${API_BASE_URL}${path}`,
    );

    if (!response.ok) {
        let detail = "";

        try {
            detail = await response.text();
        } catch {
            detail = "";
        }

        throw new Error(
            `API ${response.status}: ${detail || path
            }`,
        );
    }

    return response.json() as Promise<T>;
}


export function getHealth() {
    return request<HealthResponse>(
        "/health",
    );
}


export function getMeasurements(
    countryCode: string =
        DEFAULT_COUNTRY_CODE,
) {
    const params =
        new URLSearchParams({
            country_code:
                countryCode,
        });

    return request<Measurement[]>(
        `/measurements?${params.toString()}`,
    );
}


export function getHourlyQuality(
    measurementId: number,
    start: string,
    stop: string,
    countryCode: string =
        DEFAULT_COUNTRY_CODE,
) {
    const params =
        new URLSearchParams({
            measurement_id:
                String(measurementId),

            country_code:
                countryCode,

            start,
            stop,
        });

    return request<
        HourlyQualityPoint[]
    >(
        `/quality/hourly?${params.toString()}`,
    );
}


export function getIncidents(
    measurementId: number,
    start: string,
    stop: string,
    countryCode: string =
        DEFAULT_COUNTRY_CODE,
) {
    const params =
        new URLSearchParams({
            measurement_id:
                String(measurementId),

            country_code:
                countryCode,

            start,
            stop,

            only_candidates: "true",
            limit: "200",
        });

    return request<
        IncidentRecord[]
    >(
        `/incidents?${params.toString()}`,
    );
}


export function getProbeMap(
    measurementId: number,
    at?: string,
    countryCode: string =
        DEFAULT_COUNTRY_CODE,
) {
    const params =
        new URLSearchParams({
            measurement_id:
                String(measurementId),

            country_code:
                countryCode,
        });

    if (at) {
        params.set(
            "at",
            at,
        );
    }

    return request<
        ProbeMapPoint[]
    >(
        `/probes/map?${params.toString()}`,
    );
}