import {
    LngLatBounds,
    Map,
    Marker,
    NavigationControl,
    Popup,
} from "maplibre-gl";

import {
    useEffect,
    useRef,
} from "react";

import type {
    ProbeMapPoint,
} from "../api";

import "maplibre-gl/dist/maplibre-gl.css";
import "./ProbeMap.css";


type ProbeMapProps = {
    probes: ProbeMapPoint[];
};


function formatMetric(
    value: number | null,
    digits = 2,
): string {
    if (
        value === null
        || Number.isNaN(value)
    ) {
        return "N/A";
    }

    return value.toFixed(
        digits,
    );
}


function popupRow(
    label: string,
    value: string,
): HTMLDivElement {
    const row =
        document.createElement(
            "div",
        );

    const labelNode =
        document.createElement(
            "span",
        );

    labelNode.className =
        "probe-popup-label";

    labelNode.textContent =
        `${label}: `;

    const valueNode =
        document.createElement(
            "span",
        );

    valueNode.textContent =
        value;

    row.append(
        labelNode,
        valueNode,
    );

    return row;
}


function createPopupContent(
    probe: ProbeMapPoint,
): HTMLDivElement {
    const root =
        document.createElement(
            "div",
        );

    root.className =
        "probe-popup";


    const title =
        document.createElement(
            "strong",
        );

    title.textContent =
        `Probe ${probe.probe_id}`;

    root.appendChild(
        title,
    );


    root.appendChild(
        popupRow(
            "Target",
            probe.target,
        ),
    );


    root.appendChild(
        popupRow(
            "ASN",
            probe.asn_v4 !== null
                ? String(probe.asn_v4)
                : "N/A",
        ),
    );


    root.appendChild(
        popupRow(
            "RTT",
            `${formatMetric(
                probe.avg_rtt_ms,
            )
            } ms`,
        ),
    );


    root.appendChild(
        popupRow(
            "P95",
            `${formatMetric(
                probe.p95_rtt_ms,
            )
            } ms`,
        ),
    );


    root.appendChild(
        popupRow(
            "Packet loss",
            `${formatMetric(
                probe.packet_loss_pct,
                3,
            )
            }%`,
        ),
    );


    root.appendChild(
        popupRow(
            "Samples",
            String(
                probe.sample_count,
            ),
        ),
    );


    root.appendChild(
        popupRow(
            "Measurement status",
            probe.measurement_incident_status
            ?? "N/A",
        ),
    );


    const note =
        document.createElement(
            "small",
        );

    note.textContent =
        "Approximate probe location";

    root.appendChild(
        note,
    );


    return root;
}


function ProbeMap({
    probes,
}: ProbeMapProps) {
    const containerRef =
        useRef<HTMLDivElement | null>(
            null,
        );

    const mapRef =
        useRef<Map | null>(
            null,
        );


    useEffect(() => {
        if (
            !containerRef.current
            || mapRef.current
        ) {
            return;
        }


        const map =
            new Map({
                container:
                    containerRef.current,

                center: [
                    108.2,
                    16.0,
                ],

                zoom: 4.6,

                style: {
                    version: 8,

                    sources: {
                        osm: {
                            type: "raster",

                            tiles: [
                                "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
                            ],

                            tileSize: 256,

                            attribution:
                                "© OpenStreetMap contributors",
                        },
                    },

                    layers: [
                        {
                            id: "osm",

                            type: "raster",

                            source: "osm",
                        },
                    ],
                },
            });


        map.addControl(
            new NavigationControl(),
            "top-right",
        );


        mapRef.current =
            map;


        return () => {
            map.remove();

            mapRef.current =
                null;
        };
    }, []);


    useEffect(() => {
        const map =
            mapRef.current;

        if (!map) {
            return;
        }


        const markers:
            Marker[] = [];


        for (
            const probe
            of probes
        ) {
            const element =
                document.createElement(
                    "div",
                );

            element.className =
                "probe-marker";


            if (
                probe.packet_loss_pct
                !== null
                && probe.packet_loss_pct > 0
            ) {
                element.classList.add(
                    "probe-marker-loss",
                );
            }


            const popup =
                new Popup({
                    offset: 16,
                })
                    .setDOMContent(
                        createPopupContent(
                            probe,
                        ),
                    );


            const marker =
                new Marker({
                    element,
                })
                    .setLngLat([
                        probe.longitude,
                        probe.latitude,
                    ])
                    .setPopup(
                        popup,
                    )
                    .addTo(
                        map,
                    );


            markers.push(
                marker,
            );
        }


        if (
            probes.length > 0
        ) {
            const bounds =
                new LngLatBounds();


            for (
                const probe
                of probes
            ) {
                bounds.extend([
                    probe.longitude,
                    probe.latitude,
                ]);
            }


            map.fitBounds(
                bounds,
                {
                    padding: 60,
                    maxZoom: 8,
                    duration: 500,
                },
            );
        }


        return () => {
            for (
                const marker
                of markers
            ) {
                marker.remove();
            }
        };
    }, [
        probes,
    ]);


    return (
        <div
            ref={
                containerRef
            }
            className="probe-map"
        />
    );
}


export default ProbeMap;