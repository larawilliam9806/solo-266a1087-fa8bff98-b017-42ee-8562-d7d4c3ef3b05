from __future__ import annotations

import asyncio
import gzip
import hashlib
import io
import tarfile
import threading
import time
import uuid
from pathlib import Path

import httpx
import pytest

from app import comparison as comp_mod
from app.collector import Collector
from app.comparison import DAY_SLOTS
from app.config import Settings
from app.main import create_app
from app.store import utcnow
from mock_source.app import app as source_app, state as source_state
from tests.conftest import free_port, run_server

HEADER = "station,date,time,temperature_c,pressure_hpa,wind_mps"
DATE = "2026-11-16"


def _csv_line(station: str, date: str, slot: str, temp: str) -> str:
    return f"{station},{date},{slot},{temp},1000.0,5.0"


def build_package(
    station: str,
    date: str,
    rows: list[tuple[str, str]] | None = None,
    *,
    row_station: str | None = None,
    row_date: str | None = None,
    include_csv: bool = True,
    extra_members: dict[str, bytes] | None = None,
    corrupt: bool = False,
    not_a_gzip: bool = False,
    header_line: str = HEADER,
) -> bytes:
    """Craft a deterministic tar.gz with fully controlled observations.csv."""
    lines = [header_line]
    rs, rd = row_station or station, row_date or date
    for slot, temp in rows or []:
        lines.append(_csv_line(rs, rd, slot, temp))
    csv_bytes = ("\n".join(lines) + "\n").encode()

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        def add(name: str, data: bytes) -> None:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = "observation"
            archive.addfile(info, io.BytesIO(data))

        add("README.txt", f"station={station}\ndate={date}\n".encode())
        if include_csv:
            add("observations.csv", csv_bytes)
        for name, data in (extra_members or {}).items():
            add(name, data)

    if not_a_gzip:
        body = b"this is definitely not a gzip stream" + buffer.getvalue()[:64]
    else:
        body = gzip.compress(buffer.getvalue(), mtime=0)
        if corrupt:
            # Flip a payload byte in the middle; the gzip CRC then rejects it.
            tampered = bytearray(body)
            tampered[len(tampered) // 2] ^= 0xFF
            body = bytes(tampered)
    return body


def register_completed(
    collector: Collector,
    settings: Settings,
    station: str,
    date: str,
    body: bytes,
    *,
    revision: str = "v1",
) -> tuple[str, str, Path]:
    digest = hashlib.sha256(body).hexdigest()
    path = settings.packages_dir / f"{station}-{date}-{digest[:12]}.tar.gz"
    path.write_bytes(body)
    task_id = uuid.uuid4().hex
    collector.create_task(task_id, station, date)
    collector.store.update(
        task_id,
        state="completed",
        source_url=f"http://source/packages/{station}/{date}.tar.gz",
        source_revision=revision,
        source_etag=f'"{digest}"',
        expected_size=len(body),
        expected_sha256=digest,
        downloaded_bytes=len(body),
        package_path=str(path),
        verified_size=len(body),
        verified_sha256=digest,
        completed_at=utcnow(),
    )
    return task_id, digest, path


def full_rows(station: str, date: str, temp: str) -> list[tuple[str, str]]:
    return [(slot, temp) for slot in DAY_SLOTS]


@pytest.fixture
def local(tmp_path):
    settings = Settings(
        source_base_url="http://127.0.0.1:1",
        data_dir=tmp_path / "data",
    )
    collector = Collector(settings)
    return collector, settings


async def _wait(client: httpx.AsyncClient, api: str, task_id: str, states: set[str], timeout: float = 60):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] in states:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout; last={view}")


@pytest.fixture
async def stack(tmp_path):
    source_state.revisions.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.0
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.no_range.clear()
    source_state.requests.clear()

    source_port = free_port()
    api_port = free_port()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data",
        http_timeout=15,
        chunk_size=8 * 1024,
        max_source_resyncs=3,
    )
    collector = Collector(settings)
    api_app = create_app(settings, collector)
    async with run_server(source_app, source_port):
        async with run_server(api_app, api_port):
            async with httpx.AsyncClient(timeout=30) as client:
                yield (
                    f"http://127.0.0.1:{source_port}",
                    f"http://127.0.0.1:{api_port}",
                ), client, settings, collector


# --------------------------------------------------------------------------- #
# Core calculation
# --------------------------------------------------------------------------- #
def test_drift_detection_boundaries_and_package_digests(local):
    collector, settings = local
    threshold = 2.0
    target_rows = full_rows("T", DATE, "20.00")
    # 12:00 is 5C above the references; 06:00 is exactly 2C (not strictly over).
    target_rows = [(s, "25.00" if s == "12:00" else "22.00" if s == "06:00" else t)
                   for s, t in target_rows]
    target_body = build_package("T", DATE, target_rows)
    r1_body = build_package("R1", DATE, full_rows("R1", DATE, "20.00"))
    r2_body = build_package("R2", DATE, full_rows("R2", DATE, "20.00"))
    _, t_digest, t_path = register_completed(collector, settings, "T", DATE, target_body)
    _, r1_digest, _ = register_completed(collector, settings, "R1", DATE, r1_body)
    _, r2_digest, _ = register_completed(collector, settings, "R2", DATE, r2_body)

    result = comp_mod.run_comparison(
        collector.store, "T", ["R1", "R2"], DATE, threshold
    )

    assert result["slot_count"] == 96
    assert len(result["comparable_times"]) == 96
    assert result["non_comparable_times"] == []
    drifts = result["suspected_drifts"]
    assert len(drifts) == 1
    drift = drifts[0]
    assert drift["time"] == "12:00"
    assert drift["target_temperature_c"] == 25.0
    assert drift["reference_median_c"] == 20.0
    assert drift["difference_c"] == 5.0
    assert drift["threshold_c"] == threshold

    digests = {p["station"]: p for p in result["packages"]}
    assert set(digests) == {"T", "R1", "R2"}
    assert digests["T"]["sha256"] == t_digest
    assert digests["T"]["package_path"] == str(t_path)
    assert digests["T"]["size"] == len(target_body)
    assert digests["R1"]["sha256"] == r1_digest
    assert digests["R2"]["sha256"] == r2_digest
    assert all(p["date"] == DATE for p in result["packages"])


def test_same_station_same_time_rows_reduce_to_median(local):
    collector, settings = local
    # Three readings at 00:00 for the target -> median 20.00.
    target_rows = full_rows("T", DATE, "20.00")
    target_rows.extend([("00:00", "18.00"), ("00:00", "22.00")])
    # Even-count median: R1 00:00 readings 18 and 22 -> 20.
    r1_rows = [row for row in full_rows("R1", DATE, "20.00") if row[0] != "00:00"]
    r1_rows.extend([("00:00", "18.00"), ("00:00", "22.00")])
    r2_rows = [row for row in full_rows("R2", DATE, "20.00") if row[0] != "00:00"]
    r2_rows.extend([("00:00", "19.00"), ("00:00", "21.00")])  # also median 20

    register_completed(collector, settings, "T", DATE, build_package("T", DATE, target_rows))
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, r1_rows))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, r2_rows))

    result = comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 0.5)
    assert result["non_comparable_times"] == []
    assert result["suspected_drifts"] == []


def test_non_comparable_slots_are_listed_in_day_order(local):
    collector, settings = local
    target_rows = [(s, t) for s, t in full_rows("T", DATE, "20.00")
                   if s not in {"00:00", "00:15"}]
    # R1 misses 01:00 and 02:00; R2 misses 01:00.
    r1_rows = [(s, t) for s, t in full_rows("R1", DATE, "20.00") if s not in {"01:00", "02:00"}]
    r2_rows = [(s, t) for s, t in full_rows("R2", DATE, "20.00") if s != "01:00"]

    register_completed(collector, settings, "T", DATE, build_package("T", DATE, target_rows))
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, r1_rows))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, r2_rows))

    result = comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)

    # 00:00/00:15 target missing; 01:00 only zero references; 02:00 one reference.
    assert result["non_comparable_times"] == ["00:00", "00:15", "01:00", "02:00"]
    assert len(result["comparable_times"]) == 92
    assert set(result["comparable_times"]).isdisjoint(result["non_comparable_times"])


def test_non_finite_temperature_rows_are_ignored_not_crashing(local):
    collector, settings = local
    target_rows = full_rows("T", DATE, "20.00")

    # 03:00 carries only non-finite / blank values -> the station has no value there.
    r1_rows = [row for row in full_rows("R1", DATE, "20.00") if row[0] != "03:00"]
    r1_rows.append(("03:00", "nan"))
    r2_rows = [row for row in full_rows("R2", DATE, "20.00") if row[0] != "03:00"]
    r2_rows.append(("03:00", ""))  # blank likewise

    # At 04:00 an extra infinite row is ignored; the finite reading still counts.
    r2_rows.append(("04:00", "inf"))

    register_completed(collector, settings, "T", DATE, build_package("T", DATE, target_rows))
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, r1_rows))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, r2_rows))

    result = comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)
    assert "03:00" in result["non_comparable_times"]
    # 04:00 still has R1's finite value plus R2's original finite row.
    assert "04:00" in result["comparable_times"]


# --------------------------------------------------------------------------- #
# Whole-request failures (never partial)
# --------------------------------------------------------------------------- #
def test_row_station_mismatch_fails_entire_request(local):
    collector, settings = local
    bad = build_package("T", DATE, full_rows("X", DATE, "20.00"), row_station="X")
    register_completed(collector, settings, "T", DATE, bad)
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))
    with pytest.raises(comp_mod.ComparisonParseError, match="station"):
        comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)


def test_row_date_mismatch_fails_entire_request(local):
    collector, settings = local
    bad = build_package("T", DATE, full_rows("T", "2020-01-01", "20.00"), row_date="2020-01-01")
    register_completed(collector, settings, "T", DATE, bad)
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))
    with pytest.raises(comp_mod.ComparisonParseError, match="date"):
        comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)


def test_missing_csv_member_fails(local):
    collector, settings = local
    body = build_package("T", DATE, include_csv=False)
    register_completed(collector, settings, "T", DATE, body)
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))
    with pytest.raises(comp_mod.ComparisonParseError, match="observations.csv"):
        comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)


def test_corrupt_archive_fails(local):
    collector, settings = local
    register_completed(collector, settings, "T", DATE, build_package("T", DATE, corrupt=True))
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))
    with pytest.raises(comp_mod.ComparisonParseError):
        comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)

    # A completely non-gzip body is rejected the same way.
    collector2 = Collector(Settings(data_dir=settings.data_dir))
    register_completed(collector2, settings, "T2", DATE, build_package("T2", DATE, not_a_gzip=True))
    with pytest.raises(comp_mod.ComparisonParseError):
        comp_mod.run_comparison(collector2.store, "T2", ["R1", "R2"], DATE, 1.0)


def test_bad_time_fails(local):
    collector, settings = local
    bad_rows = full_rows("T", DATE, "20.00")
    bad_rows.append(("00:07", "20.00"))  # not a quarter-hour slot
    register_completed(collector, settings, "T", DATE, build_package("T", DATE, bad_rows))
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))
    with pytest.raises(comp_mod.ComparisonParseError, match="96"):
        comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)


def test_missing_required_column_fails(local):
    collector, settings = local
    bad_header = "station,date,time,pressure_hpa,wind_mps"
    body = build_package("T", DATE, full_rows("T", DATE, "20.00"), header_line=bad_header)
    register_completed(collector, settings, "T", DATE, body)
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))
    with pytest.raises(comp_mod.ComparisonParseError, match="required columns"):
        comp_mod.run_comparison(collector.store, "T", ["R1", "R2"], DATE, 1.0)


def test_missing_or_unfinished_or_changed_package_is_409(local):
    collector, settings = local
    register_completed(collector, settings, "R1", DATE, build_package("R1", DATE, full_rows("R1", DATE, "20.00")))
    register_completed(collector, settings, "R2", DATE, build_package("R2", DATE, full_rows("R2", DATE, "20.00")))

    # No task at all for the target.
    with pytest.raises(comp_mod.PackageUnavailableError, match="no collection task"):
        comp_mod.run_comparison(collector.store, "GHOST", ["R1", "R2"], DATE, 1.0)

    # Queued (not completed) task.
    queued_id = uuid.uuid4().hex
    collector.create_task(queued_id, "Q", DATE)
    with pytest.raises(comp_mod.PackageUnavailableError, match="completed"):
        comp_mod.run_comparison(collector.store, "Q", ["R1", "R2"], DATE, 1.0)

    # Completed row whose package file disappeared.
    _, digest, path = register_completed(
        collector, settings, "GONE", DATE, build_package("GONE", DATE, full_rows("GONE", DATE, "20.00"))
    )
    path.unlink()
    with pytest.raises(comp_mod.PackageUnavailableError, match="missing"):
        comp_mod.run_comparison(collector.store, "GONE", ["R1", "R2"], DATE, 1.0)

    # File on disk no longer matches the recorded digest.
    _, _, path2 = register_completed(
        collector, settings, "SWAP", DATE, build_package("SWAP", DATE, full_rows("SWAP", DATE, "20.00"))
    )
    path2.write_bytes(b"unrelated new bytes")
    with pytest.raises(comp_mod.PackageUnavailableError, match="changed on disk"):
        comp_mod.run_comparison(collector.store, "SWAP", ["R1", "R2"], DATE, 1.0)


# --------------------------------------------------------------------------- #
# HTTP layer
# --------------------------------------------------------------------------- #
async def test_http_comparison_happy_path(local):
    collector, settings = local
    app = create_app(Settings(data_dir=settings.data_dir), collector)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        for station in ("T", "R1", "R2"):
            register_completed(
                collector, settings, station, DATE,
                build_package(station, DATE, full_rows(station, DATE, "20.00")),
            )
        resp = await client.post(
            "/comparisons",
            json={
                "target_station": "T",
                "reference_stations": ["R1", "R2"],
                "date": DATE,
                "threshold_c": 1.5,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["slot_count"] == 96
        assert len(body["packages"]) == 3
        assert body["non_comparable_times"] == []
        assert body["suspected_drifts"] == []
        assert {p["station"] for p in body["packages"]} == {"T", "R1", "R2"}
        assert all(len(p["sha256"]) == 64 for p in body["packages"])


async def test_http_409_and_422_mapping(local):
    collector, settings = local
    app = create_app(Settings(data_dir=settings.data_dir), collector)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        register_completed(
            collector, settings, "R1", DATE,
            build_package("R1", DATE, full_rows("R1", DATE, "20.00")),
        )
        register_completed(
            collector, settings, "R2", DATE,
            build_package("R2", DATE, full_rows("R2", DATE, "20.00")),
        )

        missing = await client.post(
            "/comparisons",
            json={"target_station": "NOPE", "reference_stations": ["R1", "R2"],
                  "date": DATE, "threshold_c": 1.0},
        )
        assert missing.status_code == 409
        assert "NOPE" in missing.json()["detail"]

        bad = build_package("T", DATE, include_csv=False)
        register_completed(collector, settings, "T", DATE, bad)
        parse_fail = await client.post(
            "/comparisons",
            json={"target_station": "T", "reference_stations": ["R1", "R2"],
                  "date": DATE, "threshold_c": 1.0},
        )
        assert parse_fail.status_code == 422
        assert "observations.csv" in parse_fail.json()["detail"]


async def test_http_request_validation(local):
    collector, settings = local
    app = create_app(Settings(data_dir=settings.data_dir), collector)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        base = {"target_station": "T", "date": DATE, "threshold_c": 1.0}

        async def status(payload):
            return (await client.post("/comparisons", json=payload)).status_code

        assert await status({**base, "reference_stations": ["R1"]}) == 422
        assert await status({**base, "reference_stations": ["R1", "R1"]}) == 422
        assert await status({**base, "reference_stations": ["T", "R1"]}) == 422
        assert await status({**base, "reference_stations": ["R1", "R2"], "threshold_c": 0}) == 422
        assert await status({**base, "reference_stations": ["R1", "R2"], "threshold_c": -3}) == 422
        # Non-finite values sent as strings (JSON itself cannot encode NaN).
        assert await status({**base, "reference_stations": ["R1", "R2"], "threshold_c": "nan"}) == 422
        assert await status({**base, "reference_stations": ["R1", "R2"], "threshold_c": "inf"}) == 422
        assert await status({**base, "reference_stations": ["R1", "R2"], "date": "17-11-2026"}) == 422
        assert await status({**base, "reference_stations": ["../x", "R2"]}) == 422


# --------------------------------------------------------------------------- #
# Snapshot semantics against a real concurrent refresh
# --------------------------------------------------------------------------- #
async def test_refresh_during_parsing_keeps_snapshot_and_is_not_blocked(stack, monkeypatch):
    (source, api), client, settings, collector = stack
    stations = ["CT", "CR1", "CR2"]
    task_ids: dict[str, str] = {}
    for station in stations:
        task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": DATE})).json()["id"]
        await client.post(f"{api}/tasks/{task_id}/start")
        done = await _wait(client, api, task_id, {"completed", "failed"})
        assert done["state"] == "completed", done
        task_ids[station] = task_id

    old_digest = collector.store.find("CT", DATE)["verified_sha256"]

    entered = threading.Event()
    release = threading.Event()
    real_parse = comp_mod.parse_observations

    def gated_parse(snapshot):
        if snapshot.station == "CT":
            entered.set()
            assert release.wait(timeout=20), "comparison gate was never released"
        return real_parse(snapshot)

    monkeypatch.setattr(comp_mod, "parse_observations", gated_parse)

    comparison_task = asyncio.create_task(
        client.post(
            f"{api}/comparisons",
            json={
                "target_station": "CT",
                "reference_stations": ["CR1", "CR2"],
                "date": DATE,
                "threshold_c": 1000.0,
            },
        )
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, entered.wait)

    # While the comparison is mid-parse on a worker thread, the source moves to a
    # new revision and a refresh runs to completion.  It must not block, and the
    # in-flight comparison must keep analysing the bytes it accepted.
    await client.post(
        f"{source}/admin/revision",
        json={"station": "CT", "date": DATE, "revision": "v2"},
    )
    refresh = await client.post(f"{api}/tasks/{task_ids['CT']}/refresh")
    assert refresh.status_code == 200
    refreshed = await _wait(client, api, task_ids["CT"], {"completed", "failed"})
    assert refreshed["state"] == "completed", refreshed
    assert refreshed["verified_sha256"] != old_digest

    release.set()
    response = await comparison_task
    assert response.status_code == 200, response.text
    body = response.json()
    target_package = next(p for p in body["packages"] if p["station"] == "CT")
    assert target_package["sha256"] == old_digest
    assert target_package["sha256"] != refreshed["verified_sha256"]
    assert body["slot_count"] == 96
    assert len(body["comparable_times"]) == 96


async def test_end_to_end_collected_packages_comparison(stack):
    (_source, api), client, _settings, collector = stack
    for station in ("E1", "E2", "E3"):
        task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": DATE})).json()["id"]
        await client.post(f"{api}/tasks/{task_id}/start")
        done = await _wait(client, api, task_id, {"completed", "failed"})
        assert done["state"] == "completed", done

    response = await client.post(
        f"{api}/comparisons",
        json={
            "target_station": "E1",
            "reference_stations": ["E2", "E3"],
            "date": DATE,
            "threshold_c": 0.0001,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["slot_count"] == 96
    assert len(body["comparable_times"]) + len(body["non_comparable_times"]) == 96
    for drift in body["suspected_drifts"]:
        assert abs(drift["difference_c"]) == abs(
            drift["target_temperature_c"] - drift["reference_median_c"]
        )
        assert abs(drift["difference_c"]) > 0.0001
    recorded = {
        row["station"]: row["verified_sha256"]
        for row in collector.store.list("completed")
        if row["date"] == DATE
    }
    for package in body["packages"]:
        assert package["sha256"] == recorded[package["station"]]
