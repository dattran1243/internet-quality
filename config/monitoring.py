"""Global monitoring configuration.

This module intentionally separates:

1. Measurement definitions
   - what target is being measured

2. Country scopes
   - where participating probes are located

This keeps the pipeline country-agnostic and allows the
same processing code to support VN, DE, US, JP, etc.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CountryScope:
    code: str
    name: str
    enabled: bool = True


@dataclass(frozen=True)
class MeasurementConfig:
    measurement_id: int
    target: str
    enabled: bool = True


# ------------------------------------------------------------------
# Countries
# ------------------------------------------------------------------

COUNTRIES: dict[str, CountryScope] = {
    "VN": CountryScope(
        code="VN",
        name="Vietnam",
        enabled=True,
    ),

    # These are prepared for the next phases.
    # They are NOT ingested yet.
    "DE": CountryScope(
        code="DE",
        name="Germany",
        enabled=False,
    ),

    "US": CountryScope(
        code="US",
        name="United States",
        enabled=False,
    ),

    "JP": CountryScope(
        code="JP",
        name="Japan",
        enabled=False,
    ),

    "SG": CountryScope(
        code="SG",
        name="Singapore",
        enabled=False,
    ),
}


DEFAULT_COUNTRY_CODE = "VN"


# ------------------------------------------------------------------
# Product measurements
# ------------------------------------------------------------------

MEASUREMENTS: dict[int, MeasurementConfig] = {
    86206926: MeasurementConfig(
        measurement_id=86206926,
        target="google.com",
    ),

    176906978: MeasurementConfig(
        measurement_id=176906978,
        target="cloudflare.com",
    ),

    177421171: MeasurementConfig(
        measurement_id=177421171,
        target="github.com",
    ),

    177401244: MeasurementConfig(
        measurement_id=177401244,
        target="fastly.com",
    ),

    177401210: MeasurementConfig(
        measurement_id=177401210,
        target="akamai.com",
    ),
}


# Historical/reference measurement.
#
# It remains valid generic RIPE Atlas data, but is deliberately
# excluded from product analytical measurement selection.
REFERENCE_MEASUREMENT_IDS: frozenset[int] = frozenset({
    1009,
})


def get_country(
    country_code: str,
) -> CountryScope:
    """Return a configured country."""

    normalized = (
        country_code
        .strip()
        .upper()
    )

    try:
        return COUNTRIES[normalized]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported country code: {normalized}"
        ) from exc


def get_enabled_countries() -> list[CountryScope]:
    """Return currently enabled country scopes."""

    return [
        country
        for country in COUNTRIES.values()
        if country.enabled
    ]


def get_enabled_measurements() -> list[MeasurementConfig]:
    """Return product measurements enabled for monitoring."""

    return [
        measurement
        for measurement in MEASUREMENTS.values()
        if measurement.enabled
    ]


def get_enabled_measurement_ids() -> list[int]:
    """Return enabled product measurement IDs."""

    return [
        measurement.measurement_id
        for measurement in get_enabled_measurements()
    ]


def is_product_measurement(
    measurement_id: int,
) -> bool:
    """Check whether a measurement belongs to product analytics."""

    measurement = MEASUREMENTS.get(
        measurement_id,
    )

    return bool(
        measurement
        and measurement.enabled
    )


def is_reference_measurement(
    measurement_id: int,
) -> bool:
    """Check whether a measurement is reference/regression data."""

    return (
        measurement_id
        in REFERENCE_MEASUREMENT_IDS
    )