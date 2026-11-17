import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  DEFAULT_COUNTRY_CODE,
  getHealth,
  getHourlyQuality,
  getIncidents,
  getMeasurements,
  getProbeMap,

  type HealthResponse,
  type HourlyQualityPoint,
  type IncidentRecord,
  type Measurement,
  type ProbeMapPoint,
} from "./api";

import ProbeMap from "./components/ProbeMap";

import "./App.css";


const COUNTRY_OPTIONS = [
  { code: "AU", label: "Australia" },
  { code: "BR", label: "Brazil" },
  { code: "CA", label: "Canada" },
  { code: "DE", label: "Germany" },
  { code: "FR", label: "France" },
  { code: "GB", label: "United Kingdom" },
  { code: "IN", label: "India" },
  { code: "JP", label: "Japan" },
  { code: "NL", label: "Netherlands" },
  { code: "PL", label: "Poland" },
  { code: "SE", label: "Sweden" },
  { code: "SG", label: "Singapore" },
  { code: "US", label: "United States" },
  { code: "VN", label: "Vietnam" },
  { code: "ZA", label: "South Africa" },
] as const;


function parseUtcDate(
  value: string,
): Date {
  const hasTimezone =
    /(?:Z|[+-]\d{2}:\d{2})$/.test(
      value,
    );

  const normalized =
    hasTimezone
      ? value
      : `${value}Z`;

  return new Date(
    normalized,
  );
}


function toUtcInputValue(
  value: string,
): string {
  return parseUtcDate(
    value,
  )
    .toISOString()
    .slice(0, 16);
}


function utcInputToIso(
  value: string,
): string {
  if (!value) {
    throw new Error(
      "UTC datetime is required",
    );
  }

  const normalized =
    value.length === 16
      ? `${value}:00Z`
      : `${value}Z`;

  const date =
    new Date(
      normalized,
    );

  if (
    Number.isNaN(
      date.getTime(),
    )
  ) {
    throw new Error(
      `Invalid UTC datetime: ${value}`,
    );
  }

  return date.toISOString();
}


function addHours(
  value: string,
  hours: number,
): string {
  const date =
    parseUtcDate(
      value,
    );

  date.setUTCHours(
    date.getUTCHours()
    + hours,
  );

  return date.toISOString();
}


function formatHour(
  value: string,
): string {
  const date =
    parseUtcDate(
      value,
    );

  return date.toLocaleString(
    undefined,
    {
      timeZone: "UTC",

      month: "2-digit",
      day: "2-digit",

      hour: "2-digit",
      minute: "2-digit",

      hour12: false,
    },
  );
}


function numberOrDash(
  value: number | null | undefined,
  digits = 2,
): string {
  if (
    value === null
    || value === undefined
    || Number.isNaN(value)
  ) {
    return "—";
  }

  return value.toFixed(
    digits,
  );
}


function App() {
  const [
    health,
    setHealth,
  ] = useState<HealthResponse | null>(
    null,
  );

  const [
    selectedCountryCode,
    setSelectedCountryCode,
  ] = useState(
    DEFAULT_COUNTRY_CODE,
  );

  const [
    measurements,
    setMeasurements,
  ] = useState<Measurement[]>([]);

  const [
    selectedMeasurementId,
    setSelectedMeasurementId,
  ] = useState<number | null>(
    null,
  );

  const [
    startInput,
    setStartInput,
  ] = useState("");

  const [
    stopInput,
    setStopInput,
  ] = useState("");

  const [
    quality,
    setQuality,
  ] = useState<HourlyQualityPoint[]>([]);

  const [
    incidents,
    setIncidents,
  ] = useState<IncidentRecord[]>([]);

  const [
    probes,
    setProbes,
  ] = useState<ProbeMapPoint[]>([]);

  const [
    initialLoading,
    setInitialLoading,
  ] = useState(true);

  const [
    dataLoading,
    setDataLoading,
  ] = useState(false);

  const [
    error,
    setError,
  ] = useState<string | null>(
    null,
  );


  const selectedMeasurement =
    useMemo(
      () =>
        measurements.find(
          (item) =>
            item.measurement_id
            === selectedMeasurementId,
        ) ?? null,
      [
        measurements,
        selectedMeasurementId,
      ],
    );


  const loadDashboardData =
    useCallback(
      async (
        measurementId: number,
        startValue: string,
        stopValue: string,
        countryCode: string,
      ) => {
        if (
          !startValue
          || !stopValue
        ) {
          return;
        }

        setDataLoading(
          true,
        );

        setError(
          null,
        );

        try {
          const start =
            utcInputToIso(
              startValue,
            );

          const stop =
            utcInputToIso(
              stopValue,
            );

          if (
            new Date(start)
            >= new Date(stop)
          ) {
            throw new Error(
              "Start time must be before stop time.",
            );
          }

          const [
            qualityResult,
            incidentResult,
          ] = await Promise.all([
            getHourlyQuality(
              measurementId,
              start,
              stop,
              countryCode,
            ),

            getIncidents(
              measurementId,
              start,
              stop,
              countryCode,
            ),
          ]);

          setQuality(
            qualityResult,
          );

          setIncidents(
            incidentResult,
          );

          const latestPoint =
            qualityResult.length > 0
              ? qualityResult[
              qualityResult.length - 1
              ]
              : null;

          if (latestPoint) {
            const probeTime =
              parseUtcDate(
                latestPoint.hour_start_utc,
              ).toISOString();

            try {
              const probeResult =
                await getProbeMap(
                  measurementId,
                  probeTime,
                  countryCode,
                );

              setProbes(
                probeResult,
              );
            } catch (
            mapError
            ) {
              console.error(
                "Could not load probe map",
                mapError,
              );

              setProbes(
                [],
              );
            }
          } else {
            setProbes(
              [],
            );
          }
        } catch (err) {
          setError(
            err instanceof Error
              ? err.message
              : "Unknown dashboard error",
          );
        } finally {
          setDataLoading(
            false,
          );
        }
      },
      [],
    );


  useEffect(() => {
    async function initialize() {
      try {
        setDataLoading(
          true,
        );

        setError(
          null,
        );

        setMeasurements(
          [],
        );

        setSelectedMeasurementId(
          null,
        );

        setQuality(
          [],
        );

        setIncidents(
          [],
        );

        setProbes(
          [],
        );

        const [
          healthResult,
          measurementResult,
        ] = await Promise.all([
          getHealth(),

          getMeasurements(
            selectedCountryCode,
          ),
        ]);

        setHealth(
          healthResult,
        );

        setMeasurements(
          measurementResult,
        );

        if (
          measurementResult.length
          > 0
        ) {
          const first =
            measurementResult[0];

          setSelectedMeasurementId(
            first.measurement_id,
          );

          const start =
            toUtcInputValue(
              first.first_seen_utc,
            );

          const stop =
            toUtcInputValue(
              addHours(
                first.last_seen_utc,
                1,
              ),
            );

          setStartInput(
            start,
          );

          setStopInput(
            stop,
          );

          await loadDashboardData(
            first.measurement_id,
            start,
            stop,
            selectedCountryCode,
          );
        } else {
          setStartInput(
            "",
          );

          setStopInput(
            "",
          );
        }
      } catch (err) {
        setError(
          err instanceof Error
            ? err.message
            : "Could not initialize dashboard",
        );
      } finally {
        setInitialLoading(
          false,
        );

        setDataLoading(
          false,
        );
      }
    }

    void initialize();
  }, [
    loadDashboardData,
    selectedCountryCode,
  ]);


  function handleMeasurementChange(
    measurementId: number,
  ) {
    const measurement =
      measurements.find(
        (item) =>
          item.measurement_id
          === measurementId,
      );

    setSelectedMeasurementId(
      measurementId,
    );

    if (!measurement) {
      return;
    }

    const newStart =
      toUtcInputValue(
        measurement.first_seen_utc,
      );

    const newStop =
      toUtcInputValue(
        addHours(
          measurement.last_seen_utc,
          1,
        ),
      );

    setStartInput(
      newStart,
    );

    setStopInput(
      newStop,
    );

    void loadDashboardData(
      measurementId,
      newStart,
      newStop,
      selectedCountryCode,
    );
  }


  function handleRefresh() {
    if (
      selectedMeasurementId
      === null
    ) {
      return;
    }

    void loadDashboardData(
      selectedMeasurementId,
      startInput,
      stopInput,
      selectedCountryCode,
    );
  }


  const chartData =
    useMemo(
      () =>
        quality.map(
          (point) => ({
            ...point,

            timeLabel:
              formatHour(
                point.hour_start_utc,
              ),
          }),
        ),
      [
        quality,
      ],
    );


  const latest =
    quality.length > 0
      ? quality[
      quality.length - 1
      ]
      : null;


  const averageRtt =
    useMemo(
      () => {
        const values =
          quality
            .map(
              (point) =>
                point.avg_rtt_ms,
            )
            .filter(
              (
                value,
              ): value is number =>
                value !== null,
            );

        if (
          values.length === 0
        ) {
          return null;
        }

        return (
          values.reduce(
            (
              sum,
              value,
            ) =>
              sum + value,
            0,
          )
          / values.length
        );
      },
      [
        quality,
      ],
    );


  const averageLoss =
    useMemo(
      () => {
        const values =
          quality
            .map(
              (point) =>
                point.packet_loss_pct,
            )
            .filter(
              (
                value,
              ): value is number =>
                value !== null,
            );

        if (
          values.length === 0
        ) {
          return null;
        }

        return (
          values.reduce(
            (
              sum,
              value,
            ) =>
              sum + value,
            0,
          )
          / values.length
        );
      },
      [
        quality,
      ],
    );


  if (initialLoading) {
    return (
      <main className="center-screen">
        <div className="loader" />

        <p>
          Loading network telemetry…
        </p>
      </main>
    );
  }


  return (
    <main className="dashboard">
      <header className="topbar">
        <div>
          <p className="eyebrow">
            Network Observability Platform
          </p>

          <h1>
            Global Internet Quality
          </h1>

          <p className="subtitle">
            Internet performance,
            packet loss and network
            degradation monitoring
          </p>
        </div>


        <div
          className={
            health?.status === "ok"
              ? "system-status healthy"
              : "system-status unhealthy"
          }
        >
          <span className="status-dot" />

          {health?.status === "ok"
            ? "Systems Operational"
            : "System Degraded"}
        </div>
      </header>


      <section className="control-panel">
        <div className="control">
          <label>
            Country
          </label>

          <select
            value={
              selectedCountryCode
            }
            onChange={
              (event) =>
                setSelectedCountryCode(
                  event.target.value,
                )
            }
            disabled={
              dataLoading
            }
          >
            {COUNTRY_OPTIONS.map(
              (country) => (
                <option
                  key={
                    country.code
                  }
                  value={
                    country.code
                  }
                >
                  {country.label}
                  {" — "}
                  {country.code}
                </option>
              ),
            )}
          </select>
        </div>


        <div className="control">
          <label>
            Measurement
          </label>

          <select
            value={
              selectedMeasurementId
              ?? ""
            }
            onChange={
              (event) =>
                handleMeasurementChange(
                  Number(
                    event.target.value,
                  ),
                )
            }
            disabled={
              dataLoading
              || measurements.length === 0
            }
          >
            {measurements.length === 0 && (
              <option value="">
                No measurements available
              </option>
            )}

            {measurements.map(
              (measurement) => (
                <option
                  key={
                    measurement.measurement_id
                  }
                  value={
                    measurement.measurement_id
                  }
                >
                  {
                    measurement.target
                  }
                  {" — "}
                  {
                    measurement.measurement_id
                  }
                </option>
              ),
            )}
          </select>
        </div>


        <div className="control">
          <label>
            Start (UTC)
          </label>

          <input
            type="datetime-local"
            value={
              startInput
            }
            onChange={
              (event) =>
                setStartInput(
                  event.target.value,
                )
            }
            disabled={
              dataLoading
            }
          />
        </div>


        <div className="control">
          <label>
            Stop (UTC)
          </label>

          <input
            type="datetime-local"
            value={
              stopInput
            }
            onChange={
              (event) =>
                setStopInput(
                  event.target.value,
                )
            }
            disabled={
              dataLoading
            }
          />
        </div>


        <button
          className="refresh-button"
          onClick={
            handleRefresh
          }
          disabled={
            dataLoading
            || selectedMeasurementId === null
          }
        >
          {dataLoading
            ? "Loading…"
            : "Refresh"}
        </button>
      </section>


      {error && (
        <section className="error-banner">
          <strong>
            Dashboard error
          </strong>

          <span>
            {error}
          </span>
        </section>
      )}


      <section className="kpi-grid">
        <article className="kpi-card">
          <span className="kpi-label">
            Country
          </span>

          <strong className="kpi-value target-value">
            {selectedCountryCode}
          </strong>

          <small>
            selected monitoring scope
          </small>
        </article>


        <article className="kpi-card">
          <span className="kpi-label">
            Target
          </span>

          <strong className="kpi-value target-value">
            {
              selectedMeasurement
                ?.target
              ?? "—"
            }
          </strong>

          <small>
            RIPE Atlas measurement
          </small>
        </article>


        <article className="kpi-card">
          <span className="kpi-label">
            Active Probes
          </span>

          <strong className="kpi-value">
            {
              latest
                ?.probe_count
              ?? "—"
            }
          </strong>

          <small>
            latest observed hour
          </small>
        </article>


        <article className="kpi-card">
          <span className="kpi-label">
            Average RTT
          </span>

          <strong className="kpi-value">
            {numberOrDash(
              averageRtt,
            )}

            <span className="unit">
              {" "}
              ms
            </span>
          </strong>

          <small>
            selected UTC window
          </small>
        </article>


        <article className="kpi-card">
          <span className="kpi-label">
            Average Loss
          </span>

          <strong className="kpi-value">
            {numberOrDash(
              averageLoss,
              3,
            )}

            <span className="unit">
              {" "}
              %
            </span>
          </strong>

          <small>
            selected UTC window
          </small>
        </article>


        <article className="kpi-card incident-card">
          <span className="kpi-label">
            Incidents
          </span>

          <strong className="kpi-value">
            {
              incidents.length
            }
          </strong>

          <small>
            warning / critical
          </small>
        </article>
      </section>


      <section className="chart-grid">
        <article className="panel chart-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">
                Performance
              </p>

              <h2>
                Latency
              </h2>
            </div>

            <span className="panel-meta">
              {quality.length}
              {" "}
              hourly samples
            </span>
          </div>


          <div className="chart-container">
            <ResponsiveContainer
              width="100%"
              height="100%"
            >
              <LineChart
                data={
                  chartData
                }
                margin={{
                  top: 12,
                  right: 24,
                  left: 4,
                  bottom: 4,
                }}
              >
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="#1f2937"
                />

                <XAxis
                  dataKey="timeLabel"
                  stroke="#64748b"
                  tick={{
                    fontSize: 12,
                  }}
                  minTickGap={30}
                />

                <YAxis
                  stroke="#64748b"
                  tick={{
                    fontSize: 12,
                  }}
                  unit=" ms"
                />

                <Tooltip
                  contentStyle={{
                    background:
                      "#0f172a",

                    border:
                      "1px solid #334155",

                    borderRadius:
                      "10px",
                  }}
                />

                <Legend />

                <Line
                  type="monotone"
                  dataKey="avg_rtt_ms"
                  name="Average RTT"
                  stroke="#38bdf8"
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />

                <Line
                  type="monotone"
                  dataKey="avg_probe_p95_rtt_ms"
                  name="Average Probe P95"
                  stroke="#a78bfa"
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </article>


        <article className="panel chart-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">
                Reliability
              </p>

              <h2>
                Packet Loss & Coverage
              </h2>
            </div>
          </div>


          <div className="chart-container">
            <ResponsiveContainer
              width="100%"
              height="100%"
            >
              <LineChart
                data={
                  chartData
                }
                margin={{
                  top: 12,
                  right: 24,
                  left: 4,
                  bottom: 4,
                }}
              >
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="#1f2937"
                />

                <XAxis
                  dataKey="timeLabel"
                  stroke="#64748b"
                  tick={{
                    fontSize: 12,
                  }}
                  minTickGap={30}
                />

                <YAxis
                  yAxisId="loss"
                  stroke="#64748b"
                  tick={{
                    fontSize: 12,
                  }}
                  unit="%"
                />

                <YAxis
                  yAxisId="probes"
                  orientation="right"
                  stroke="#64748b"
                  tick={{
                    fontSize: 12,
                  }}
                />

                <Tooltip
                  contentStyle={{
                    background:
                      "#0f172a",

                    border:
                      "1px solid #334155",

                    borderRadius:
                      "10px",
                  }}
                />

                <Legend />

                <Line
                  yAxisId="loss"
                  type="monotone"
                  dataKey="packet_loss_pct"
                  name="Packet Loss"
                  stroke="#fb7185"
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />

                <Line
                  yAxisId="probes"
                  type="stepAfter"
                  dataKey="probe_count"
                  name="Probe Count"
                  stroke="#34d399"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </article>
      </section>


      <section className="panel map-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">
              Geography
            </p>

            <h2>
              Probe Network Map
            </h2>
          </div>

          <span className="panel-meta">
            {probes.length}
            {" "}
            probes
          </span>
        </div>


        <div className="map-legend">
          <span>
            <i className="legend-dot normal-dot" />

            No packet loss
          </span>

          <span>
            <i className="legend-dot loss-dot" />

            Packet loss observed
          </span>

          <span>
            Coordinates are approximate
          </span>
        </div>


        <ProbeMap
          probes={
            probes
          }
        />
      </section>


      <section className="panel incidents-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">
              Detection
            </p>

            <h2>
              Recent Incident Candidates
            </h2>
          </div>

          <span className="panel-meta">
            {incidents.length}
            {" "}
            detected
          </span>
        </div>


        {incidents.length === 0 ? (
          <div className="empty-state">
            <span className="empty-icon">
              ✓
            </span>

            <div>
              <strong>
                No incident candidates
              </strong>

              <p>
                No warning or critical
                network-level degradation
                was detected in this UTC window.
              </p>
            </div>
          </div>
        ) : (
          <div className="incident-table-wrapper">
            <table className="incident-table">
              <thead>
                <tr>
                  <th>
                    Time (UTC)
                  </th>

                  <th>
                    Status
                  </th>

                  <th>
                    Affected Probes
                  </th>

                  <th>
                    Affected ASNs
                  </th>

                  <th>
                    Coverage
                  </th>

                  <th>
                    Signal
                  </th>
                </tr>
              </thead>

              <tbody>
                {incidents.map(
                  (
                    incident,
                    index,
                  ) => (
                    <tr
                      key={
                        `${incident.hour_start_utc
                        }-${incident.measurement_id
                        }-${index}`
                      }
                    >
                      <td>
                        {formatHour(
                          incident.hour_start_utc,
                        )}
                      </td>

                      <td>
                        <span
                          className={
                            `status-badge ${incident.incident_status
                            }`
                          }
                        >
                          {
                            incident.incident_status
                          }
                        </span>
                      </td>

                      <td>
                        {
                          incident.affected_probe_count
                        }
                      </td>

                      <td>
                        {
                          incident.affected_asn_count
                        }
                      </td>

                      <td>
                        {
                          incident.coverage_status
                          ?? "—"
                        }
                      </td>

                      <td className="signal-cell">
                        {
                          incident.signal_types
                          || incident.incident_summary
                        }
                      </td>
                    </tr>
                  ),
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>


      <footer className="footer">
        <span>
          Data source: RIPE Atlas
        </span>

        <span>
          Country scope: {selectedCountryCode}
        </span>

        <span>
          All timestamps shown in UTC
        </span>

        <span>
          Probe locations are approximate
        </span>
      </footer>
    </main>
  );
}


export default App;