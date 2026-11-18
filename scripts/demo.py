#!/usr/bin/env python3
"""End-to-end demo against the Compose stack (or any base URLs).

Usage:
    python scripts/demo.py [--source http://localhost:9000] [--api http://localhost:8000]

The demo collects one archive, pauses/resumes it, switches the source revision,
refreshes and verifies the downloadable package's size and SHA-256.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time

import httpx


def wait(client: httpx.Client, api: str, task_id: str, states: set[str], timeout: float = 60) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        view = client.get(f"{api}/tasks/{task_id}").json()
        if view["state"] in states:
            return view
        time.sleep(0.2)
    raise SystemExit(f"timeout waiting for {states}; last={view}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="http://localhost:9000")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--station", default="54511")
    parser.add_argument("--date", default="2026-11-17")
    args = parser.parse_args()

    source, api = args.source.rstrip("/"), args.api.rstrip("/")
    with httpx.Client(timeout=30) as client:
        print("source:", client.get(f"{source}/healthz").json())
        print("api:   ", client.get(f"{api}/healthz").json())
        client.post(f"{source}/admin/reset")
        client.post(f"{source}/admin/delay", json={"seconds": 0.3, "chunk_seconds": 0.01})

        task = client.post(
            f"{api}/tasks", json={"station": args.station, "date": args.date}
        ).json()
        task_id = task["id"]
        print("task:", task_id, task["state"])

        started = client.post(f"{api}/tasks/{task_id}/start").json()
        print("start:", started["state"])

        # Let some bytes land, then pause.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            view = client.get(f"{api}/tasks/{task_id}").json()
            if view["downloaded_bytes"] > 0:
                break
            time.sleep(0.05)
        paused = client.post(f"{api}/tasks/{task_id}/pause").json()
        print("pause:", paused["state"], "bytes:", paused["downloaded_bytes"])
        time.sleep(0.5)
        after = client.get(f"{api}/tasks/{task_id}").json()
        assert after["state"] == "paused", after
        assert after["downloaded_bytes"] >= paused["downloaded_bytes"]
        print("stable while paused:", after["state"], after["downloaded_bytes"])

        resumed = client.post(f"{api}/tasks/{task_id}/resume").json()
        print("resume:", resumed["state"])
        done = wait(client, api, task_id, {"completed", "failed"}, timeout=90)
        print("finished:", done["state"], done["percent"], done["verified_sha256"])
        if done["state"] != "completed":
            print("error:", done["error"])
            return 1

        package = client.get(f"{api}/tasks/{task_id}/package")
        package.raise_for_status()
        digest = hashlib.sha256(package.content).hexdigest()
        assert digest == done["verified_sha256"], (digest, done["verified_sha256"])
        assert len(package.content) == done["verified_size"]
        print(f"package downloaded: {len(package.content)} bytes, sha256 verified")

        # Switch the source revision and refresh: old package must be replaced.
        client.post(
            f"{source}/admin/revision",
            json={"station": args.station, "date": args.date, "revision": "v2"},
        )
        client.post(f"{api}/tasks/{task_id}/refresh")
        done2 = wait(client, api, task_id, {"completed", "failed"}, timeout=90)
        assert done2["state"] == "completed", done2
        assert done2["source_revision"] == "v2", done2
        assert done2["verified_sha256"] != done["verified_sha256"]
        package2 = client.get(f"{api}/tasks/{task_id}/package")
        package2.raise_for_status()
        assert hashlib.sha256(package2.content).hexdigest() == done2["verified_sha256"]
        print("revision switch verified:", done2["source_revision"], done2["verified_sha256"][:12])
        print("events:", [e["event"] for e in client.get(f"{api}/tasks/{task_id}/events").json()])
    return 0


if __name__ == "__main__":
    sys.exit(main())
