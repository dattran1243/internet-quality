"""Resilient client for the official RIPE Atlas v2 results API."""

import logging
import time
from typing import Any

import requests

from ingestion.ripe_atlas.models import (
    TimeWindow,
    utc_text,
)


LOGGER = logging.getLogger(__name__)


# Keep result URLs comfortably below common URI-length limits.
#
# Countries such as Germany can have thousands of connected
# RIPE Atlas probes, so all probe IDs must not be placed in one
# GET request.
DEFAULT_PROBE_BATCH_SIZE = 200


class RipeAtlasClient:
    """Fetch public RIPE Atlas data with bounded retries."""

    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = (
            base_url.rstrip("/")
        )

        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

        self.session = (
            session
            or requests.Session()
        )

    def results_url(
        self,
        measurement_id: int,
    ) -> str:
        return (
            f"{self.base_url}"
            f"/measurements/"
            f"{measurement_id}"
            f"/results/"
        )

    def get_measurement_current_probe_ids(
        self,
        measurement_id: int,
    ) -> list[int]:
        """
        Return current participant probe IDs for one measurement.

        This is a current measurement snapshot, not a historical
        participant reconstruction.
        """

        if measurement_id <= 0:
            raise ValueError(
                "measurement_id must be positive"
            )

        url = (
            f"{self.base_url}"
            f"/measurements/"
            f"{measurement_id}/"
        )

        payload = self._get_json(
            url,
            params={
                "optional_fields":
                    "current_probes",
            },
        )

        if not isinstance(
            payload,
            dict,
        ):
            raise ValueError(
                "RIPE Atlas measurement response "
                "must be a JSON object"
            )

        raw_probe_ids = payload.get(
            "current_probes"
        )

        if not isinstance(
            raw_probe_ids,
            list,
        ):
            raise ValueError(
                "RIPE Atlas measurement "
                "current_probes is missing or malformed"
            )

        probe_ids: list[int] = []

        for probe_id in raw_probe_ids:
            if (
                not isinstance(
                    probe_id,
                    int,
                )
                or isinstance(
                    probe_id,
                    bool,
                )
                or probe_id <= 0
            ):
                raise ValueError(
                    "RIPE Atlas measurement "
                    "contains invalid current probe ID"
                )

            probe_ids.append(
                probe_id
            )

        normalized_ids = sorted(
            set(
                probe_ids
            )
        )

        LOGGER.info(
            "Measurement current participants "
            "measurement_id=%s probes=%s",
            measurement_id,
            len(normalized_ids),
        )

        return normalized_ids

    def _get_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        GET JSON with bounded retry for network
        and transient HTTP errors.
        """

        attempts = (
            self.max_retries + 1
        )

        for attempt in range(
            attempts
        ):
            try:
                response = (
                    self.session.get(
                        url,
                        params=params,
                        timeout=self.timeout,
                    )
                )

                LOGGER.info(
                    "API response status=%s",
                    response.status_code,
                )

                response.raise_for_status()

                return response.json()

            except (
                requests.Timeout,
                requests.ConnectionError,
                requests.exceptions.HTTPError,
            ) as exc:
                retryable = (
                    not isinstance(
                        exc,
                        requests.exceptions.HTTPError,
                    )
                    or (
                        exc.response is not None
                        and (
                            exc.response.status_code
                            == 429
                            or exc.response.status_code
                            >= 500
                        )
                    )
                )

                if (
                    not retryable
                    or attempt
                    == attempts - 1
                ):
                    raise

                delay = (
                    self.backoff_seconds
                    * (2 ** attempt)
                )

                LOGGER.warning(
                    "Transient API error; "
                    "retrying in %.1fs: %s",
                    delay,
                    exc,
                )

                time.sleep(
                    delay
                )

        raise RuntimeError(
            "unreachable"
        )

    @staticmethod
    def _result_records(
        payload: Any,
    ) -> list[dict[str, Any]]:
        """
        Validate a RIPE Atlas results payload and keep
        object records only.
        """

        if not isinstance(
            payload,
            list,
        ):
            raise ValueError(
                "RIPE Atlas results response "
                "must be a JSON array"
            )

        records = [
            item
            for item in payload
            if isinstance(
                item,
                dict,
            )
        ]

        malformed = (
            len(payload)
            - len(records)
        )

        if malformed:
            LOGGER.warning(
                "Ignored %s malformed "
                "non-object records",
                malformed,
            )

        return records

    def fetch_results(
        self,
        measurement_id: int,
        window: TimeWindow,
        probe_ids: list[int] | None = None,
        probe_batch_size: int =
            DEFAULT_PROBE_BATCH_SIZE,
    ) -> list[dict[str, Any]]:
        """
        Fetch measurement results for one bounded window.

        When probe_ids are supplied, requests are split into
        bounded batches so countries with thousands of probes
        do not exceed HTTP URI-length limits.
        """

        if measurement_id <= 0:
            raise ValueError(
                "measurement_id must be positive"
            )

        if probe_batch_size <= 0:
            raise ValueError(
                "probe_batch_size must be positive"
            )

        url = self.results_url(
            measurement_id
        )

        base_params = {
            "start": utc_text(
                window.start
            ),
            "stop": utc_text(
                window.stop
            ),
            "format": "json",
        }

        LOGGER.info(
            "API request start "
            "measurement_id=%s "
            "start=%s stop=%s",
            measurement_id,
            base_params["start"],
            base_params["stop"],
        )

        # -----------------------------------------------------
        # No probe filtering
        # -----------------------------------------------------

        if probe_ids is None:
            payload = self._get_json(
                url,
                params=base_params,
            )

            records = (
                self._result_records(
                    payload
                )
            )

            LOGGER.info(
                "records received=%s",
                len(records),
            )

            return records

        # -----------------------------------------------------
        # Probe-filtered request
        # -----------------------------------------------------

        normalized_ids = sorted(
            set(
                probe_ids
            )
        )

        if not normalized_ids:
            raise ValueError(
                "probe_ids must not be empty"
            )

        batches = [
            normalized_ids[
                index:
                index + probe_batch_size
            ]
            for index in range(
                0,
                len(normalized_ids),
                probe_batch_size,
            )
        ]

        LOGGER.info(
            "Fetching measurement=%s "
            "probe_count=%s "
            "batch_size=%s "
            "batches=%s",
            measurement_id,
            len(normalized_ids),
            probe_batch_size,
            len(batches),
        )

        all_records: list[
            dict[str, Any]
        ] = []

        total_batches = len(
            batches
        )

        for batch_index, batch in enumerate(
            batches,
            start=1,
        ):
            params = dict(
                base_params
            )

            params[
                "probe_ids"
            ] = ",".join(
                map(
                    str,
                    batch,
                )
            )

            LOGGER.info(
                "Fetching measurement=%s "
                "batch=%s/%s "
                "batch_probes=%s",
                measurement_id,
                batch_index,
                total_batches,
                len(batch),
            )

            payload = self._get_json(
                url,
                params=params,
            )

            batch_records = (
                self._result_records(
                    payload
                )
            )

            all_records.extend(
                batch_records
            )

            LOGGER.info(
                "Measurement=%s "
                "batch=%s/%s "
                "records=%s "
                "accumulated_records=%s",
                measurement_id,
                batch_index,
                total_batches,
                len(batch_records),
                len(all_records),
            )

        LOGGER.info(
            "records received=%s "
            "measurement_id=%s "
            "probe_batches=%s",
            len(all_records),
            measurement_id,
            total_batches,
        )

        return all_records

    def get_connected_probes(
        self,
        country_code: str = "VN",
        status_name: str = "Connected",
    ) -> list[dict[str, Any]]:
        """
        Return deterministic probe metadata,
        following all API pages.
        """

        country = (
            country_code
            .strip()
            .upper()
        )

        if len(country) != 2:
            raise ValueError(
                "country_code must be a "
                "two-letter ISO code"
            )

        status = (
            status_name.strip()
        )

        if not status:
            raise ValueError(
                "status_name must not be empty"
            )

        url: str | None = (
            f"{self.base_url}/probes/"
        )

        params: dict[
            str,
            Any,
        ] | None = {
            "country_code":
                country,

            "status_name":
                status,

            "page_size":
                500,
        }

        probes_by_id: dict[
            int,
            dict[str, Any],
        ] = {}

        while url:
            LOGGER.info(
                "Probe API request "
                "country_code=%s status=%s",
                country,
                status,
            )

            payload = self._get_json(
                url,
                params=params,
            )

            if (
                not isinstance(
                    payload,
                    dict,
                )
                or not isinstance(
                    payload.get(
                        "results"
                    ),
                    list,
                )
            ):
                raise ValueError(
                    "RIPE Atlas probes "
                    "response is malformed"
                )

            for probe in payload[
                "results"
            ]:
                if not isinstance(
                    probe,
                    dict,
                ):
                    continue

                probe_id = (
                    probe.get(
                        "id"
                    )
                )

                if (
                    isinstance(
                        probe_id,
                        int,
                    )
                    and probe_id > 0
                ):
                    previous = (
                        probes_by_id.get(
                            probe_id
                        )
                    )

                    if (
                        previous is not None
                        and previous != probe
                    ):
                        raise ValueError(
                            "Conflicting metadata "
                            "for duplicate probe "
                            f"{probe_id}"
                        )

                    probes_by_id[
                        probe_id
                    ] = probe

            next_url = (
                payload.get(
                    "next"
                )
            )

            if (
                next_url is not None
                and not isinstance(
                    next_url,
                    str,
                )
            ):
                raise ValueError(
                    "RIPE Atlas probes next "
                    "page URL is malformed"
                )

            url = next_url

            # The next-page URL already contains
            # pagination/query state.
            params = None

        probes = [
            probes_by_id[
                probe_id
            ]
            for probe_id in sorted(
                probes_by_id
            )
        ]

        LOGGER.info(
            "%s %s probes: %s",
            status,
            country,
            len(probes),
        )

        LOGGER.debug(
            "%s %s probe IDs: %s",
            status,
            country,
            [
                probe["id"]
                for probe in probes
            ],
        )

        return probes

    def get_connected_probe_ids(
        self,
        country_code: str = "VN",
        status_name: str = "Connected",
    ) -> list[int]:
        """
        Return sorted unique connected probe IDs.

        This ID-only helper intentionally deduplicates by
        probe ID even if mutable probe metadata differs
        between paginated RIPE Atlas API responses.

        Full metadata retrieval remains strict in
        get_connected_probes().
        """

        country = (
            country_code
            .strip()
            .upper()
        )

        if len(country) != 2:
            raise ValueError(
                "country_code must be a "
                "two-letter ISO code"
            )

        status = (
            status_name.strip()
        )

        if not status:
            raise ValueError(
                "status_name must not be empty"
            )

        url: str | None = (
            f"{self.base_url}/probes/"
        )

        params: dict[
            str,
            Any,
        ] | None = {
            "country_code":
                country,

            "status_name":
                status,

            "page_size":
                500,
        }

        probe_ids: set[int] = set()

        while url:
            LOGGER.info(
                "Probe API request "
                "country_code=%s status=%s",
                country,
                status,
            )

            payload = self._get_json(
                url,
                params=params,
            )

            if (
                not isinstance(
                    payload,
                    dict,
                )
                or not isinstance(
                    payload.get(
                        "results"
                    ),
                    list,
                )
            ):
                raise ValueError(
                    "RIPE Atlas probes "
                    "response is malformed"
                )

            for probe in payload[
                "results"
            ]:
                if not isinstance(
                    probe,
                    dict,
                ):
                    continue

                probe_id = (
                    probe.get(
                        "id"
                    )
                )

                if (
                    isinstance(
                        probe_id,
                        int,
                    )
                    and probe_id > 0
                ):
                    probe_ids.add(
                        probe_id
                    )

            next_url = (
                payload.get(
                    "next"
                )
            )

            if (
                next_url is not None
                and not isinstance(
                    next_url,
                    str,
                )
            ):
                raise ValueError(
                    "RIPE Atlas probes next "
                    "page URL is malformed"
                )

            url = next_url

            # next already contains pagination state
            params = None

        normalized = sorted(
            probe_ids
        )

        LOGGER.info(
            "Connected probe IDs "
            "country=%s status=%s probes=%s",
            country,
            status,
            len(normalized),
        )

        return normalized
