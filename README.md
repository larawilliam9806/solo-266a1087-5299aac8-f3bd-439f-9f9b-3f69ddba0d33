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
| `GET /budget` | Space-budget snapshot: cap, used, reserved, available, holds and blocked reasons |
| `PUT /budget` `{limit_bytes}` | Set the total byte cap (`null` clears it); lowering below committed bytes returns `409` |

`GET /tasks` accepts `?state=running|paused|completed|failed|queued`.

## Space budget

```bash
# Cap the collection disk at 2 GiB (counted files only; SQLite is excluded).
curl -X PUT localhost:8000/budget -H 'content-type: application/json' \
  -d '{"limit_bytes": 2147483648}'

# Inspect usage, unwritten reservations and any tasks blocked on the cap.
curl localhost:8000/budget
# {
#   "limit_bytes": 2147483648,
#   "used_bytes": 1053044,        # parts + packages + publish temporaries
#   "reserved_bytes": 1053044,    # admitted but not yet written
#   "available_bytes": 2145377560,
#   "holds": [{"task_id": "...", "generation": 3, "bytes": 1053044}],
#   "blocked": [{"task_id": "...", "required_bytes": 1053044,
#                "available_bytes": 0, "reason": "space budget: ..."}]
# }

# Remove the cap entirely.
curl -X PUT localhost:8000/budget -H 'content-type: application/json' -d '{"limit_bytes": null}'
```

A cap can also be seeded for a fresh deployment with the
`SPACE_BUDGET_BYTES` environment variable; a limit set through the API is
persisted in the data volume and takes precedence on the next start.

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
8. **Space budget is reserved before bytes arrive.** After the source directory
   is read and before any package body is received, the worker reserves the
   directory size the task may reach. Counted bytes are the raw part files
   (including fragments kept across pause/timeout), the published packages and
   the publish temporaries — the SQLite database is excluded. At every instant
   `used_bytes + reserved_bytes <= limit_bytes`; a reservation converts into
   real usage chunk by chunk as the part grows.
9. **Admission failure stops only the blocked task.** If a task cannot be
   admitted, it fails with a `space budget:` reason (visible in
   `GET /budget` under `blocked`), keeps any retained fragment, deletes no other
   task's data and can be retried with `start` once space frees up or the cap
   is raised. An empty disk whose cap fits one package always completes.
10. **Revisions and refreshes are re-admitted.** A changed source revision is
    re-checked against its new size. During a refresh the old package is kept
    until the new version is fully verified and atomically renamed into place;
    the old package, the growing new part and the publish temporary all count
    while they coexist. Lowering the cap below the bytes already committed
    (used + reserved) is rejected with `409`.
11. **Restart reconciles the budget.** The cap persists in SQLite; on startup
    the collector recomputes real usage, deletes un-published publish
    temporaries and drops the in-memory reservations of the dead process.
    Reservations are keyed by task generation, so a late worker from an old
    round can neither write nor release the new round's budget.

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
issued before a delayed response arrives, input validation, and the space
budget (exact-fit completion, blocking with reasons, parallel-task admission,
pause releasing the reservation while retaining usage, refresh keeping the old
package plus the new part within budget, re-admission on revision, restart
purge of temporaries and hold reconciliation, and cap-lowering rejection).
