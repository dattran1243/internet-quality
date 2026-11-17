"""Global monitoring configuration.

This module intentionally separates:

1. Measurement definitions
   - what target is being measured

2. Country scopes
   - where participating probes are located

This keeps the pipeline country-agnostic and allows the
same processing code to support global country scopes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pycountry
from dotenv import load_dotenv


# Load variables from the project .env file.
#
# This allows local runs such as:
#
# MONITORING_COUNTRIES=VN,DE,SG,JP,US
#
# or:
#
# MONITORING_COUNTRIES=ALL
#
load_dotenv()


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

# Countries already verified through the Kafka -> Bronze pipeline.
#
# This is also the fallback when MONITORING_COUNTRIES
# is not configured.
DEFAULT_ENABLED_COUNTRY_CODES: tuple[str, ...] = (
    "VN",
    "DE",
    "SG",
    "JP",
    "US",
)


def _all_iso_countries() -> dict[str, str]:
    """Return all ISO-3166 alpha-2 countries."""

    return {
        country.alpha_2.upper(): country.name
        for country in pycountry.countries
    }


SUPPORTED_COUNTRIES: dict[str, str] = (
    _all_iso_countries()
)


def _enabled_country_codes() -> set[str]:
    """Return country codes enabled for ingestion.

    Configuration examples:

        MONITORING_COUNTRIES=VN,DE,SG,JP,US

    Enable every supported ISO country:

        MONITORING_COUNTRIES=ALL

    If the variable is missing, the countries in
    DEFAULT_ENABLED_COUNTRY_CODES are enabled.
    """

    raw = os.getenv(
        "MONITORING_COUNTRIES",
        ",".join(
            DEFAULT_ENABLED_COUNTRY_CODES
        ),
    ).strip()

    if not raw:
        return set(
            DEFAULT_ENABLED_COUNTRY_CODES
        )

    if raw.upper() == "ALL":
        return set(
            SUPPORTED_COUNTRIES.keys()
        )

    codes = {
        item.strip().upper()
        for item in raw.split(",")
        if item.strip()
    }

    invalid = (
        codes
        - set(
            SUPPORTED_COUNTRIES.keys()
        )
    )

    if invalid:
        raise ValueError(
            "Unsupported ISO country codes in "
            "MONITORING_COUNTRIES: "
            f"{sorted(invalid)}"
        )

    return codes


ENABLED_COUNTRY_CODES: set[str] = (
    _enabled_country_codes()
)


COUNTRIES: dict[str, CountryScope] = {
    code: CountryScope(
        code=code,
        name=name,
        enabled=(
            code
            in ENABLED_COUNTRY_CODES
        ),
    )
    for code, name
    in SUPPORTED_COUNTRIES.items()
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


# ------------------------------------------------------------------
# Country helpers
# ------------------------------------------------------------------

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


def get_enabled_country_codes() -> list[str]:
    """Return currently enabled ISO country codes."""

    return [
        country.code
        for country in get_enabled_countries()
    ]


# ------------------------------------------------------------------
# Measurement helpers
# ------------------------------------------------------------------

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