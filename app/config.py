from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    source_base_url: str = "http://localhost:9000"
    data_dir: Path = Path("./data")
    http_timeout: float = 10.0
    chunk_size: int = 64 * 1024
    max_source_resyncs: int = 3
    auto_resume: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            source_base_url=os.getenv("SOURCE_BASE_URL", "http://localhost:9000").rstrip("/"),
            data_dir=Path(os.getenv("COLLECTOR_DATA_DIR", "./data")),
            http_timeout=float(os.getenv("SOURCE_HTTP_TIMEOUT", "10")),
            chunk_size=int(os.getenv("COLLECTOR_CHUNK_SIZE", str(64 * 1024))),
            max_source_resyncs=int(os.getenv("MAX_SOURCE_RESYNCS", "3")),
            auto_resume=os.getenv("AUTO_RESUME", "false").lower() in {"1", "true", "yes"},
        )

    @property
    def parts_dir(self) -> Path:
        return self.data_dir / "parts"

    @property
    def packages_dir(self) -> Path:
        return self.data_dir / "packages"

    @property
    def database_path(self) -> Path:
        return self.data_dir / "collector.sqlite3"
