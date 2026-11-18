"""Same-day, multi-station temperature drift comparison.

The workflow is deliberately split into two phases so a package refresh that
happens while a comparison is being calculated can neither mix revisions into
the result nor be blocked by it:

1. **Acceptance.** For every participating station the completed, verified
   package is located, its bytes are read into memory *once* and the in-memory
   bytes are re-checked against the size and SHA-256 recorded when the package
   was published.  From this point on the calculation works only on that fixed
   byte snapshot; the digest returned to the caller describes exactly those
   bytes.  A refresh replaces the package via an atomic ``os.replace`` and
   updates the database, so it never mutates the snapshot and never has to wait
   for the comparison to finish.
2. **Parsing / calculation.** Each snapshot's ``observations.csv`` is parsed in
   full.  Any structural failure (unreadable archive, missing CSV, wrong
   station/date on a row, malformed time/columns) aborts the whole request —
   partial conclusions are never returned.

For every one of the 96 quarter-hour slots of the day, rows sharing a station
and a time are reduced to their median temperature.  A slot is comparable only
when the target station and at least two reference stations have a finite
value; the reference median is then the baseline.  An absolute difference
strictly greater than the threshold is reported as a suspected offset, and
every slot that could not be compared is listed explicitly.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import statistics
import tarfile
import zlib
from dataclasses import dataclass
from pathlib import Path

# 96 quarter-hour slots, 00:00 through 23:45.
DAY_SLOTS: list[str] = [
    f"{hour:02d}:{minute:02d}" for hour in range(24) for minute in (0, 15, 30, 45)
]
TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
REQUIRED_COLUMNS = frozenset({"station", "date", "time", "temperature_c"})
MAX_CSV_BYTES = 256 * 1024 * 1024
CSV_MEMBER_NAME = "observations.csv"


class PackageUnavailableError(Exception):
    """A precondition failed: no completed/verified package, or it changed at acceptance."""


class ComparisonParseError(Exception):
    """A snapshot cannot be parsed into a trustworthy comparison (no partial result)."""


@dataclass(frozen=True)
class PackageSnapshot:
    """The exact package revision fixed at request acceptance."""

    station: str
    date: str
    task_id: str
    source_revision: str | None
    package_path: str
    size: int
    sha256: str
    data: bytes


@dataclass(frozen=True)
class Observation:
    station: str
    date: str
    time: str
    temperature_c: float


def _snapshot_from_row(row) -> PackageSnapshot:
    station, date = row["station"], row["date"]
    label = f"station {station} date {date}"
    if row["state"] != "completed":
        raise PackageUnavailableError(
            f"task for {label} is '{row['state']}'; only completed packages participate"
        )
    if not row["package_path"] or not Path(row["package_path"]).is_file():
        raise PackageUnavailableError(f"verified package file is missing for {label}")
    if (
        row["verified_size"] != row["expected_size"]
        or row["expected_size"] is None
        or row["verified_sha256"] != row["expected_sha256"]
        or not row["verified_sha256"]
    ):
        raise PackageUnavailableError(f"package for {label} is not fully verified")

    path = Path(row["package_path"])
    try:
        data = path.read_bytes()
    except OSError as exc:
        # A refresh/unlink landing between the existence check and the read.
        raise PackageUnavailableError(
            f"package for {label} could not be read at acceptance: {exc}"
        ) from exc

    digest = hashlib.sha256(data).hexdigest()
    if digest != row["verified_sha256"] or len(data) != row["verified_size"]:
        # The bytes no longer match the digest the task published.  Refuse rather
        # than analyse an unidentified revision; the client may retry.
        raise PackageUnavailableError(
            f"package for {label} changed on disk before the comparison was accepted"
        )

    return PackageSnapshot(
        station=station,
        date=date,
        task_id=row["id"],
        source_revision=row["source_revision"],
        package_path=str(path),
        size=len(data),
        sha256=digest,
        data=data,
    )


def _find_csv_member(archive: tarfile.TarFile) -> tarfile.TarInfo | None:
    members = archive.getmembers()
    for member in members:
        if member.name == CSV_MEMBER_NAME:
            return member
    # Tolerate a nested directory (e.g. "./observations.csv") but never pick an
    # unrelated file that merely shares an extension.
    fallback = [
        member
        for member in members
        if member.isfile() and Path(member.name).name == CSV_MEMBER_NAME
    ]
    return fallback[0] if len(fallback) == 1 else None


def _read_observations_csv(snapshot: PackageSnapshot) -> str:
    try:
        with tarfile.open(fileobj=io.BytesIO(snapshot.data), mode="r:gz") as archive:
            member = _find_csv_member(archive)
            if member is None:
                raise ComparisonParseError(
                    f"{CSV_MEMBER_NAME} not found in package for station "
                    f"{snapshot.station} date {snapshot.date}"
                )
            if not member.isfile():
                raise ComparisonParseError(
                    f"{CSV_MEMBER_NAME} in package for station {snapshot.station} "
                    f"date {snapshot.date} is not a regular file"
                )
            if member.size > MAX_CSV_BYTES:
                raise ComparisonParseError(
                    f"{CSV_MEMBER_NAME} for station {snapshot.station} date "
                    f"{snapshot.date} exceeds the {MAX_CSV_BYTES} byte safety limit"
                )
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ComparisonParseError(
                    f"{CSV_MEMBER_NAME} for station {snapshot.station} date "
                    f"{snapshot.date} cannot be extracted"
                )
            raw = extracted.read()
    except ComparisonParseError:
        raise
    except (tarfile.TarError, OSError, EOFError, zlib.error) as exc:
        raise ComparisonParseError(
            f"package for station {snapshot.station} date {snapshot.date} is not a "
            f"readable gzip tar archive: {type(exc).__name__}: {exc}"
        ) from exc

    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ComparisonParseError(
            f"{CSV_MEMBER_NAME} for station {snapshot.station} date {snapshot.date} "
            f"is not valid UTF-8: {exc}"
        ) from exc


def parse_observations(snapshot: PackageSnapshot) -> list[Observation]:
    """Parse and validate every row of one fixed package snapshot."""
    text = _read_observations_csv(snapshot)
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None or not REQUIRED_COLUMNS.issubset(reader.fieldnames):
        raise ComparisonParseError(
            f"{CSV_MEMBER_NAME} for station {snapshot.station} date {snapshot.date} "
            f"is missing required columns {sorted(REQUIRED_COLUMNS)}"
        )

    observations: list[Observation] = []
    for line_number, row in enumerate(reader, start=2):  # header is line 1
        location = f"line {line_number} of {CSV_MEMBER_NAME} (sha256={snapshot.sha256[:12]})"
        station = (row.get("station") or "").strip()
        date_value = (row.get("date") or "").strip()
        time_value = (row.get("time") or "").strip()

        if station != snapshot.station:
            raise ComparisonParseError(
                f"row station {station!r} does not match package station "
                f"{snapshot.station!r} at {location}"
            )
        if date_value != snapshot.date:
            raise ComparisonParseError(
                f"row date {date_value!r} does not match package date "
                f"{snapshot.date!r} at {location}"
            )
        match = TIME_RE.fullmatch(time_value)
        if match is None or int(match.group(2)) % 15 != 0:
            raise ComparisonParseError(
                f"time {time_value!r} at {location} is not one of the 96 "
                f"quarter-hour slots (HH:MM, minutes a multiple of 15)"
            )

        raw_temperature = (row.get("temperature_c") or "").strip()
        try:
            temperature = float(raw_temperature)
        except (TypeError, ValueError):
            # Non-numeric / blank readings carry no finite value; they simply make
            # the slot unavailable rather than corrupting the medians.
            continue
        if not _is_finite(temperature):
            continue
        observations.append(
            Observation(
                station=station,
                date=date_value,
                time=time_value,
                temperature_c=temperature,
            )
        )
    return observations


def _is_finite(value: float) -> bool:
    return value == value and value not in (float("inf"), float("-inf"))


def _station_medians(observations: list[Observation]) -> dict[str, float]:
    """Reduce same-station, same-time rows to their median temperature."""
    grouped: dict[str, list[float]] = {}
    for observation in observations:
        grouped.setdefault(observation.time, []).append(observation.temperature_c)
    return {time: statistics.median(values) for time, values in grouped.items()}


def _package_digest(snapshot: PackageSnapshot) -> dict:
    return {
        "station": snapshot.station,
        "date": snapshot.date,
        "task_id": snapshot.task_id,
        "source_revision": snapshot.source_revision,
        "package_path": snapshot.package_path,
        "size": snapshot.size,
        "sha256": snapshot.sha256,
    }


def run_comparison(store, target_station: str, reference_stations: list[str], date: str, threshold_c: float) -> dict:
    """Fix every participating package, then compute the full-day comparison.

    Raises :class:`PackageUnavailableError` when a participant has no usable
    completed package and :class:`ComparisonParseError` when any fixed snapshot
    cannot be parsed.  In both cases nothing partial is returned.
    """
    stations = [target_station, *reference_stations]

    # Phase 1: accept and fix the exact bytes used for the whole calculation.
    snapshots: list[PackageSnapshot] = []
    for station in stations:
        row = store.find(station, date)
        if row is None:
            raise PackageUnavailableError(
                f"no collection task exists for station {station} date {date}"
            )
        snapshots.append(_snapshot_from_row(row))

    # Phase 2: parse everything first so one bad package fails the whole request
    # before any conclusion is produced.
    medians_by_station: dict[str, dict[str, float]] = {}
    for snapshot in snapshots:
        medians_by_station[snapshot.station] = _station_medians(parse_observations(snapshot))

    target_series = medians_by_station[target_station]
    reference_series = [medians_by_station[station] for station in reference_stations]

    comparable_times: list[str] = []
    non_comparable_times: list[str] = []
    suspected_drifts: list[dict] = []

    for slot in DAY_SLOTS:
        target_value = target_series.get(slot)
        reference_values = [
            series[slot] for series in reference_series if slot in series
        ]
        if target_value is None or len(reference_values) < 2:
            non_comparable_times.append(slot)
            continue

        reference_median = statistics.median(reference_values)
        difference = target_value - reference_median
        comparable_times.append(slot)
        if abs(difference) > threshold_c:
            suspected_drifts.append(
                {
                    "time": slot,
                    "target_temperature_c": target_value,
                    "reference_median_c": reference_median,
                    "difference_c": difference,
                    "threshold_c": threshold_c,
                }
            )

    return {
        "target_station": target_station,
        "reference_stations": list(reference_stations),
        "date": date,
        "threshold_c": threshold_c,
        "slot_count": len(DAY_SLOTS),
        "comparable_times": comparable_times,
        "non_comparable_times": non_comparable_times,
        "suspected_drifts": suspected_drifts,
        "packages": [_package_digest(snapshot) for snapshot in snapshots],
    }
