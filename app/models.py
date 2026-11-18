from __future__ import annotations

import hashlib
import re
from datetime import date as date_type
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

STATION_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def valid_date(value: str) -> str:
    try:
        date_type.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError("date must be a real calendar date in YYYY-MM-DD form")
    if len(value) != 10:
        raise ValueError("date must be YYYY-MM-DD")
    return value


class TaskState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class StartTaskRequest(BaseModel):
    station: str = Field(..., examples=["54511"])
    date: str = Field(..., examples=["2026-11-17"])

    @field_validator("station")
    @classmethod
    def valid_station(cls, value: str) -> str:
        if not STATION_RE.fullmatch(value):
            raise ValueError("station must match [A-Za-z0-9_-]{1,32}")
        return value

    @field_validator("date")
    @classmethod
    def valid_date_value(cls, value: str) -> str:
        return valid_date(value)


class CatalogEntry(BaseModel):
    """The source directory entry is the sole authority for size and digest."""

    station: str
    date: str
    url: str
    size: int = Field(..., ge=0)
    sha256: str
    etag: str
    revision: str | None = None
    updated_at: str | None = None

    @field_validator("sha256")
    @classmethod
    def valid_sha256(cls, value: str) -> str:
        if not SHA256_RE.fullmatch(value):
            raise ValueError("sha256 must be 64 hexadecimal characters")
        return value.lower()

    @field_validator("etag")
    @classmethod
    def strong_etag(cls, value: str) -> str:
        if not value or value.startswith("W/") or not value.startswith('"') or not value.endswith('"'):
            raise ValueError("source must provide a strong ETag")
        return value


class TaskView(BaseModel):
    id: str
    station: str
    date: str
    state: TaskState
    source_url: str | None = None
    source_revision: str | None = None
    source_etag: str | None = None
    expected_size: int | None = None
    expected_sha256: str | None = None
    downloaded_bytes: int = 0
    total_bytes: int | None = None
    percent: float = 0.0
    verified_size: int | None = None
    verified_sha256: str | None = None
    error: str | None = None
    attempts: int = 0
    resyncs: int = 0
    generation: int = 0
    created_at: str
    updated_at: str
    completed_at: str | None = None
    download_url: str | None = None


class ErrorResponse(BaseModel):
    detail: str
    state: str | None = None


def sha256_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
