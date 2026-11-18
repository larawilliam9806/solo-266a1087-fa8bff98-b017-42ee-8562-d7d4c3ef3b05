"""A deliberately small, controllable observation-data source.

The source exposes a directory per station/date, strong ETags, byte ranges and
an admin API that lets a demo switch the current revision or add response delay.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import io
import random
import re
import tarfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Iterator

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

STATION_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
STREAM_CHUNK = 32 * 1024


@dataclass
class SourceState:
    revisions: dict[tuple[str, str], str] = field(default_factory=dict)
    delay_seconds: float = 0.0
    chunk_delay_seconds: float = 0.0
    failing: set[tuple[str, str]] = field(default_factory=set)
    corrupting: set[tuple[str, str]] = field(default_factory=set)
    no_range: set[tuple[str, str]] = field(default_factory=set)
    requests: list[dict] = field(default_factory=list)

    def record(self, method: str, path: str, headers: dict[str, str]) -> None:
        self.requests.append(
            {
                "method": method,
                "path": path,
                "range": headers.get("range"),
                "if_range": headers.get("if-range"),
            }
        )
        if len(self.requests) > 500:
            del self.requests[:-500]

    def revision(self, station: str, date: str) -> str:
        return self.revisions.get((station, date), "v1")


state = SourceState()
app = FastAPI(title="Observation Source (mock)", version="1.0.0")


class RevisionRequest(BaseModel):
    station: str
    date: str
    revision: str = Field(..., min_length=1, max_length=64)

    @field_validator("station")
    @classmethod
    def valid_station(cls, value: str) -> str:
        if not STATION_RE.fullmatch(value):
            raise ValueError("station must match [A-Za-z0-9_-]{1,32}")
        return value

    @field_validator("date")
    @classmethod
    def valid_date(cls, value: str) -> str:
        if not DATE_RE.fullmatch(value):
            raise ValueError("date must be YYYY-MM-DD")
        return value

    @field_validator("revision")
    @classmethod
    def safe_revision(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            raise ValueError("revision must match [A-Za-z0-9_.-]+")
        return value


class DelayRequest(BaseModel):
    seconds: float = Field(0, ge=0, le=60, description="Delay before response headers")
    chunk_seconds: float = Field(0, ge=0, le=5, description="Delay between body chunks")


class FaultRequest(BaseModel):
    station: str
    date: str
    mode: str = Field(..., pattern="^(fail|corrupt|no_range|clear)$")
    @field_validator("station")
    @classmethod
    def valid_station(cls, value: str) -> str:
        if not STATION_RE.fullmatch(value):
            raise ValueError("station must match [A-Za-z0-9_-]{1,32}")
        return value

    @field_validator("date")
    @classmethod
    def valid_date(cls, value: str) -> str:
        if not DATE_RE.fullmatch(value):
            raise ValueError("date must be YYYY-MM-DD")
        return value


def _validate(station: str, date: str) -> None:
    if not STATION_RE.fullmatch(station):
        raise HTTPException(400, "invalid station")
    if not DATE_RE.fullmatch(date):
        raise HTTPException(400, "invalid date")


@lru_cache(maxsize=64)
def build_package(station: str, date: str, revision: str) -> bytes:
    """Build a deterministic tar.gz for (station, date, revision).

    The payload is mostly incompressible so progress and Range behaviour are
    observable, and switching revisions changes both bytes and digest.
    """
    seed = hashlib.sha256(f"{station}|{date}|{revision}".encode()).digest()
    rng = random.Random(seed)
    blob = rng.randbytes(1024 * 1024)
    rows = [
        "station,date,time,temperature_c,pressure_hpa,wind_mps",
        *[
            f"{station},{date},{(i // 4) % 24:02d}:{(i * 15) % 60:02d},"
            f"{rng.uniform(-35, 45):.2f},{rng.uniform(850, 1080):.2f},{rng.uniform(0, 30):.2f}"
            for i in range(240)
        ],
    ]
    csv_bytes = ("\n".join(rows) + "\n").encode()

    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as archive:
        def add(name: str, data: bytes) -> None:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = "observation"
            archive.addfile(info, io.BytesIO(data))

        add("README.txt", f"station={station}\ndate={date}\nrevision={revision}\n".encode())
        add("observations.csv", csv_bytes)
        add("payload.bin", blob)

    return gzip.compress(tar_buffer.getvalue(), mtime=0)


def _meta(station: str, date: str) -> dict:
    revision = state.revision(station, date)
    body = build_package(station, date, revision)
    digest = hashlib.sha256(body).hexdigest()
    return {
        "name": f"{station}-{date}.tar.gz",
        "url": f"/packages/{station}/{date}.tar.gz",
        "size": len(body),
        "sha256": digest,
        "etag": f'"{digest}"',
        "revision": revision,
        "content_type": "application/gzip",
    }


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.get("/directory/{station}/{date}")
async def directory(station: str, date: str) -> dict:
    _validate(station, date)
    entry = _meta(station, date)
    return {
        "station": station,
        "date": date,
        "revision": entry["revision"],
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "entries": [entry],
    }


def _parse_range(header: str, size: int) -> tuple[int, int] | None:
    match = RANGE_RE.fullmatch(header.strip())
    if match is None:
        return None
    first, last = match.groups()
    if first == "" and last == "":
        return None
    if first == "":
        suffix = int(last)
        if suffix == 0:
            return None
        start = max(0, size - suffix)
        end = size - 1
    else:
        start = int(first)
        end = int(last) if last else size - 1
    if start >= size or start > end:
        return (-1, -1)
    return start, min(end, size - 1)


async def _stream(body: bytes, start: int, end: int, chunk_delay: float) -> Iterator[bytes]:
    for pos in range(start, end + 1, STREAM_CHUNK):
        yield body[pos : min(pos + STREAM_CHUNK, end + 1)]
        if chunk_delay:
            await asyncio.sleep(chunk_delay)


@app.api_route("/packages/{station}/{date}.tar.gz", methods=["GET", "HEAD"])
async def package(
    station: str,
    date: str,
    request: Request,
    range_header: str | None = Header(default=None, alias="Range"),
    if_range: str | None = Header(default=None, alias="If-Range"),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
):
    _validate(station, date)
    state.record(
        request.method,
        request.url.path,
        {"range": range_header or "", "if-range": if_range or ""},
    )
    if (station, date) in state.failing:
        raise HTTPException(503, "simulated source outage")
    entry = _meta(station, date)
    body = build_package(station, date, entry["revision"])
    etag = entry["etag"]
    if (station, date) in state.corrupting:
        # Advertise the honest digest but serve a body with a flipped byte.
        tampered = bytearray(body)
        tampered[len(tampered) // 2] ^= 0xFF
        body = bytes(tampered)

    if if_none_match and if_none_match == etag:
        return Response(status_code=304, headers={"ETag": etag, "Accept-Ranges": "bytes"})

    # Delay is captured per request, so changing it affects requests started later.
    delay = state.delay_seconds
    chunk_delay = state.chunk_delay_seconds
    if delay:
        await asyncio.sleep(delay)

    headers = {
        "ETag": etag,
        "Accept-Ranges": "bytes",
        "Content-Type": "application/gzip",
        "Cache-Control": "no-cache",
        "Content-Disposition": f'attachment; filename="{entry["name"]}"',
    }

    use_range = (
        range_header is not None
        and (station, date) not in state.no_range
        and (if_range is None or if_range == etag)
    )
    if use_range:
        parsed = _parse_range(range_header, len(body))
        if parsed == (-1, -1):
            return Response(
                status_code=416,
                headers={**headers, "Content-Range": f"bytes */{len(body)}"},
            )
        if parsed is not None:
            start, end = parsed
            headers["Content-Range"] = f"bytes {start}-{end}/{len(body)}"
            headers["Content-Length"] = str(end - start + 1)
            if request.method == "HEAD":
                return Response(status_code=206, headers=headers)
            return StreamingResponse(
                _stream(body, start, end, chunk_delay),
                status_code=206,
                headers=headers,
                media_type="application/gzip",
            )

    headers["Content-Length"] = str(len(body))
    if request.method == "HEAD":
        return Response(status_code=200, headers=headers)
    return StreamingResponse(
        _stream(body, 0, len(body) - 1, chunk_delay),
        status_code=200,
        headers=headers,
        media_type="application/gzip",
    )


# -- admin / demo controls ---------------------------------------------------
@app.get("/admin/state")
async def admin_state() -> dict:
    return {
        "revisions": [
            {"station": station, "date": date, "revision": revision}
            for (station, date), revision in sorted(state.revisions.items())
        ],
        "failing": sorted(f"{s}/{d}" for s, d in state.failing),
        "corrupting": sorted(f"{s}/{d}" for s, d in state.corrupting),
        "no_range": sorted(f"{s}/{d}" for s, d in state.no_range),
        "delay_seconds": state.delay_seconds,
        "chunk_delay_seconds": state.chunk_delay_seconds,
        "recent_requests": state.requests[-50:],
    }


@app.post("/admin/revision")
async def set_revision(body: RevisionRequest) -> dict:
    state.revisions[(body.station, body.date)] = body.revision
    return {"station": body.station, "date": body.date, "revision": body.revision, "entry": _meta(body.station, body.date)}


@app.post("/admin/fault")
async def set_fault(body: FaultRequest) -> dict:
    key = (body.station, body.date)
    state.failing.discard(key)
    state.corrupting.discard(key)
    state.no_range.discard(key)
    if body.mode == "fail":
        state.failing.add(key)
    elif body.mode == "corrupt":
        state.corrupting.add(key)
    elif body.mode == "no_range":
        state.no_range.add(key)
    return {"station": body.station, "date": body.date, "mode": body.mode}


@app.post("/admin/delay")
async def set_delay(body: DelayRequest) -> dict:
    state.delay_seconds = body.seconds
    state.chunk_delay_seconds = body.chunk_seconds
    return {"delay_seconds": state.delay_seconds, "chunk_delay_seconds": state.chunk_delay_seconds}


@app.post("/admin/reset")
async def reset() -> dict:
    state.revisions.clear()
    state.delay_seconds = 0.0
    state.chunk_delay_seconds = 0.0
    state.failing.clear()
    state.corrupting.clear()
    state.no_range.clear()
    state.requests.clear()
    return {"ok": True}
