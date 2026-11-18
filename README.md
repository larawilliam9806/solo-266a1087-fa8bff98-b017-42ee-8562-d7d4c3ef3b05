# Observation Archive Collector

A small FastAPI service that collects per-station/per-day observation archives
from a directory-driven source, with pause/resume, restart-safe progress and
verify-before-download semantics.

## Components

| Service | Port | Purpose |
| --- | --- | --- |
| `collector` | 8000 | Collection API + OpenAPI docs at `/docs` |
| `source` | 9000 | Local mock observation source with strong ETags, Range, revisions, latency and fault injection |

## Run with Docker Compose

```bash
docker compose up --build
```

- API docs / demo entry point: <http://localhost:8000/docs>
- Root descriptor: <http://localhost:8000/> lists all endpoints
- Source admin state: <http://localhost:9000/admin/state>

Docker is not required for the tests; they start both ASGI apps on loopback
ports in-process.

A scripted end-to-end demonstration (collect → pause → resume → revision switch
→ verified download):

```bash
python scripts/demo.py
```

Local (non-Docker) run:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn mock_source.app:app --port 9000 &
SOURCE_BASE_URL=http://127.0.0.1:9000 COLLECTOR_DATA_DIR=./data \
  uvicorn app.main:app --port 8000
python scripts/demo.py
```

`POST /tasks` returns HTTP 201 for new tasks; an existing station+date returns
the current task.

## API

| Method & path | Meaning |
| --- | --- |
| `POST /tasks` `{station,date}` | Create (or return the existing) task; one task per station+date |
| `POST /tasks/{id}/start` | Start a queued/paused/failed task |
| `POST /tasks/{id}/pause` | Pause; returns only after in-flight writes can no longer land |
| `POST /tasks/{id}/resume` | Resume from durable progress |
| `POST /tasks/{id}/refresh` | Re-read the directory for a completed task and re-collect if it moved |
| `GET /tasks`, `GET /tasks/{id}` | List / inspect tasks (state, bytes, percent, expected vs verified size & SHA-256, error) |
| `GET /tasks/{id}/events` | Append-only event log for the task |
| `GET /tasks/{id}/package` | Download the archive; `409` unless completed and verified |
| `POST /comparisons` | Same-day multi-station temperature drift comparison over fixed, verified packages |

`GET /tasks` accepts `?state=running|paused|completed|failed|queued`.

## Same-day multi-station comparison (`POST /comparisons`)

Finds the times of day at which a target station's temperature may have drifted
from its peers, so the instrument can be re-checked.

Request body:

```json
{
  "target_station": "54511",
  "reference_stations": ["54512", "54513"],
  "date": "2026-11-17",
  "threshold_c": 2.0
}
```

Rules:

- At least two distinct reference stations are required, and the target must not
  appear among them; `threshold_c` must be a finite positive number.
- Only **completed and verified** packages participate (state `completed`, file
  present, size and SHA-256 matching the directory entry). A missing task, an
  unfinished task, a missing file or a digest mismatch fails the whole request
  with `409`.
- At acceptance the exact bytes of every participating package are read once
  and their SHA-256 is re-computed; the response lists each fixed package
  (`task_id`, source revision, path, size, SHA-256). All parsing afterwards uses
  that in-memory snapshot, so a package refresh that succeeds **or** fails while
  the comparison runs can neither mix a new revision into the answer nor be
  blocked by it. If the acceptance read itself races a refresh, the request
  fails fast with `409` and may be retried — it never analyses unknown bytes.
- Each package's `observations.csv` is parsed from the gzip tar archive. A
  station/date on any row that does not match its package, a malformed time
  (the day has exactly 96 quarter-hour slots `HH:00/15/30/45`), a missing
  `observations.csv` or required column, or an unreadable archive fails the
  whole request with `422` — partial conclusions are never returned. Blank,
  non-numeric or non-finite (`NaN`/`inf`) temperatures carry no value and make
  the affected slot unavailable rather than corrupting the medians.
- Rows for the same station and time are reduced to their **median**
  temperature. For each of the 96 slots the slot is comparable only when the
  target and **at least two** reference stations have a finite value; the
  baseline is the median of the available reference values. An absolute
  target-minus-baseline difference **strictly greater** than `threshold_c` is
  reported in `suspected_drifts`. Every slot that could not be compared is
  listed in `non_comparable_times`, in day order.

Response excerpt:

```json
{
  "target_station": "54511",
  "reference_stations": ["54512", "54513"],
  "date": "2026-11-17",
  "threshold_c": 2.0,
  "slot_count": 96,
  "comparable_times": ["00:00", "..."],
  "non_comparable_times": ["03:00"],
  "suspected_drifts": [
    {
      "time": "12:00",
      "target_temperature_c": 25.0,
      "reference_median_c": 20.0,
      "difference_c": 5.0,
      "threshold_c": 2.0
    }
  ],
  "packages": [
    {"station": "54511", "date": "2026-11-17", "task_id": "...",
     "source_revision": "v1", "package_path": "/srv/data/packages/54511-....tar.gz",
     "size": 1053044, "sha256": "..."}
  ]
}
```

The comparison is read-only: collection, pause/resume, refresh and package
download behave exactly as before.

## Correctness guarantees

1. **Directory is authoritative.** Each attempt reads size and SHA-256 from the
   source directory; nothing is trusted from an earlier attempt unless the whole
   identity (URL + strong ETag + size + digest) still matches.
2. **Resume only within one version.** The part file name embeds a hash of the
   source identity. If the directory reports a changed revision, partial bytes
   and metadata are discarded before a fresh transfer starts, so bytes from two
   versions are never concatenated.
3. **Range + If-Range.** Resumes send `Range: bytes=N-` and `If-Range: <strong
   ETag>`. A mismatched ETag, a wrong `Content-Range`, or a 412/416 causes a
   re-read instead of an append. A server that ignores Range but returns the
   same ETag is handled by truncating and restarting that version from zero
   (never appending a full body to partial data).
4. **Pause is a generation fence.** Pause bumps `generation` transactionally
   before responding; every worker write is a compare-and-set on
   `(state='running', generation)`. Late chunks/responses therefore cannot
   advance progress, mark completion, or publish a package after the pause
   response.
5. **Restart-safe.** Running tasks are flipped to `paused` at startup; the part
   file and byte count survive. With `AUTO_RESUME=true` only tasks interrupted by
   the restart (and queued tasks) resume automatically — explicit user pauses are
   left alone.
6. **Verify before exposure.** Completion requires the downloaded file's length
   and SHA-256 to equal the directory entry's. The file is written to a temp
   name and atomically renamed; the task becomes `completed` only after the
   rename. Until then — and for failed tasks — `GET .../package` returns 409.
   The directory is re-read once more just before publishing so a mid-transfer
   revision swap is discarded, not published.
7. **Failure keeps the reason.** Failed tasks retain `error`; transient failures
   (timeouts, 5xx, network) keep their partial prefix and can be retried via
   `start`; terminal failures (bad metadata, repeated verification/source
   mismatch) delete partial data.

## Source mock controls

```bash
# switch the revision served for a station/date (changes body, size, digest)
curl -X POST localhost:9000/admin/revision \
  -H 'content-type: application/json' \
  -d '{"station":"54511","date":"2026-11-17","revision":"v2"}'

# add 300 ms header delay and 10 ms per-chunk delay
curl -X POST localhost:9000/admin/delay \
  -H 'content-type: application/json' -d '{"seconds":0.3,"chunk_seconds":0.01}'

# simulate outage (503) or a corrupted body (honest digest, tampered bytes)
curl -X POST localhost:9000/admin/fault \
  -H 'content-type: application/json' -d '{"station":"54511","date":"2026-11-17","mode":"fail"}'
```

`mode` accepts `fail`, `corrupt` or `clear`. `GET /admin/state` also shows the
recent package requests including their `Range`/`If-Range` headers.

## Tests

```bash
pytest -q
```

The suite runs the real API and the real mock source on loopback ports and
covers: full verified flow, duplicate task creation, pause with a late response
(progress/completion must not move), restart recovery with a real `Range`
resume, revision change mid-pause (old bytes discarded), corruption → failure
with no downloadable package, outage → failure with reason → successful retry, refresh replacing a package on
revision change, a source that ignores Range (clean restart, no append), a pause
issued before a delayed response arrives, and input validation.

The comparison suite (`tests/test_comparison.py`) additionally covers: drift
boundaries (difference equal to the threshold is not flagged), median
aggregation of same-station/same-time rows (odd and even counts), the full 96
slot accounting with non-comparable slots listed in day order, non-finite
temperature handling, whole-request failures for row station/date mismatch,
missing `observations.csv`, corrupt archives, bad slot times and missing
columns (`422`), precondition failures for missing/unfinished/replaced packages
(`409`), request validation, and a real concurrent refresh whose new revision is
excluded from the in-flight snapshot while the refresh itself completes
unblocked.
