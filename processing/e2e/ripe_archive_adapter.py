"""Schema adapter only; metadata values remain from the pinned daily archive."""
from processing.e2e.contracts import PROBES
from processing.e2e.guards import require


def adapt_archive(archive):
    selected = [p for p in archive["objects"] if p["id"] in PROBES]
    require(len(selected) == 24 and {p["id"] for p in selected} == set(PROBES), "ARCHIVE_PROBE_SCOPE")
    result = []
    for p in selected:
        require(p.get("country_code") == "DE" and p.get("status") == 1 and
                p.get("status_name") == "Connected" and p.get("latitude") is not None and
                p.get("longitude") is not None, "ARCHIVE_COUNTRY_STATUS_COORDS")
        adapted = dict(p)
        adapted["status"] = {"id": p["status"], "name": p["status_name"]}
        adapted["geometry"] = {"coordinates": [float(p["longitude"]), float(p["latitude"])]}
        result.append(adapted)
    return result
