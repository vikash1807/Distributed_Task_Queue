# Development guide

This guide covers setup, configuration, API usage, testing, and development
practices. See [architecture](architecture.md) for internal behavior and invariants,
and [contributing](../CONTRIBUTING.md) for the contribution and review workflow.

## Requirements

For the full local stack, install Docker with Compose and BuildKit support.
For native development, also install Python 3.14+ and uv. Redis is the only running
data service required by the current application.

Run commands from the repository root after cloning as shown in the
[README](../README.md#quick-start). Default API examples use port 8080.

## Docker workflow

### Start and inspect

```bash
docker compose up --build -d --wait
docker compose ps
curl http://localhost:8080/api/health
docker compose logs -f api worker
```

Defaults work without `.env`. For customization, copy `.env.example` to `.env`
only if you do not already have one, then edit supported settings. Compose reads
that file for interpolation; exported shell variables take precedence.

Only `127.0.0.1:8080` is published by default. Redis is available to containers
as `redis:6379`, with no host port or password in this local stack. Workers publish
no ports and can make outbound requests for `http_fetch` tasks.

### Configure and scale

Set `DOCKER_API_PORT=8090` in `.env` to publish the API at localhost:8090. Compose
always passes `SERVER_PORT=8080`, `REDIS_ADDR=redis:6379`, and an empty
`REDIS_PASSWORD` to the application, overriding those host settings. It forwards
only the environment settings listed in `compose.yaml`.

```bash
docker compose up -d --scale worker=3
curl http://localhost:8080/api/nodes
curl http://localhost:8080/api/workers
```

With `WORKER_COUNT=5`, three worker replicas provide 15 async executor slots.
Keep the desired `--scale worker=3` argument on subsequent `up` commands when
retaining that replica count. Apply environment changes with `docker compose up -d`.
Worker readiness is checked through registration and task execution, rather than
a worker HTTP health endpoint.

### Rebuild after edits

Source is copied into the image; source mounts and automatic reload are not enabled.
After changing source or dependencies:

```bash
docker compose up --build -d
```

Pinned Python, uv, and Redis image references must be updated deliberately when
upgrading. The dependency build uses the lockfile and excludes development tools.
See [container internals](architecture.md#startup-shutdown-and-containers).

### Stop and preserve data

```bash
docker compose down
```

This removes containers and retains Redis data in the project's named volume.
Keep the same Compose project name to reuse that volume. Increase
`DOCKER_STOP_GRACE_PERIOD` above expected drain and cleanup time when increasing
handler or polling timeouts. Redis failures can extend cleanup beyond normal timing.

**The following command intentionally deletes all persisted queue data:**

```bash
docker compose down -v
```

The restart policy handles process exits unless explicitly stopped. An unhealthy
health-check status alone does not restart a container. After a Redis outage,
inspect application logs and restart API/worker services if recovery requires it:

```bash
docker compose restart api worker
```

Public deployments require a separate plan for access control, TLS, persistence,
backups, and Redis availability. The Compose defaults are for local use.

## Native setup

Install the locked dependencies, including pytest and Ruff from the default dev group:

```bash
uv sync --locked
```

Start a dedicated development Redis if you do not already have one:

```bash
docker run --detach --name taskqueue-dev-redis \
  --publish 127.0.0.1:6379:6379 redis:8.2
docker exec taskqueue-dev-redis redis-cli ping
```

Copy `.env.example` to `.env` if needed and set native configuration, for example
`REDIS_ADDR=localhost:6379`. Copying the file alone does not configure Python:
the application reads real process environment variables. Either export settings
or explicitly ask uv to load the file.

In one terminal:

```bash
uv run --env-file .env python -m app.main
```

In a second terminal:

```bash
uv run --env-file .env python -m app.run_worker
```

Without a file, `uv run python -m app.main` and `uv run python -m app.run_worker`
use exported settings and built-in defaults. Start additional worker processes in
additional terminals as needed. Stop native processes with Ctrl+C; stop the
separate development Redis with `docker stop taskqueue-dev-redis` and start it
again with `docker start taskqueue-dev-redis`.

## Configuration reference

Settings are defined in `app/core/config.py` and illustrated in
[.env.example](../.env.example). Application durations below use milliseconds
unless otherwise stated.

| Setting | Default | Purpose |
| --- | --- | --- |
| `REDIS_ADDR` | `localhost:6379` | Native Redis host and port; the application selects DB 0 |
| `REDIS_PASSWORD` | empty | Native Redis password, if required |
| `SERVER_PORT` | `8080` | Native API listening port |
| `METRICS_PORT` | `9100` | Reserved setting; there is no standalone worker metrics listener |
| `WORKER_COUNT` | `5` | Async executors per worker node |
| `POLL_INTERVAL_MS` | `500` | Worker backoff after a dequeue/execution-loop error |
| `DRAIN_TIMEOUT_MS` | `5000` | Per-handler execution limit, which bounds normal in-flight drain |
| `SIGNAL_BLOCK_MS` | `1000` | Blocking wake-up wait and fallback/shutdown-check interval |
| `SIGNAL_CAP` | `1024` | Maximum retained ready wake-up tokens |
| `VISIBILITY_TIMEOUT_MS` | `30000` | Processing lease duration |
| `REAPER_INTERVAL_MS` | `5000` | Leader recovery scan interval |
| `HEARTBEAT_INTERVAL_MS` | `3000` | Worker heartbeat refresh interval |
| `HEARTBEAT_TTL_MS` | `10000` | Heartbeat lifetime; must exceed its refresh interval |
| `DEAD_NODE_GRACE_MS` | `30000` | Dead-node visibility before pruning |
| `LEADER_ELIGIBLE` | `true` | Whether an API competes to run singleton services |
| `LEADER_TTL_MS` | `10000` | Leader lease TTL; minimum 300, renewed at TTL/3 cadence |
| `CRON_TICK_MS` | `1000` | Positive leader-owned cron scan interval |
| `DOCKER_API_PORT` | `8080` | Compose-only host port, bound to loopback |
| `DOCKER_STOP_GRACE_PERIOD` | `30s` | Compose-only shutdown allowance before forced termination |

Keep handler timeouts below visibility timeouts with margin for ACK and Redis
operations. The current worker does not automatically extend task leases during
execution. Positive execution timings and worker counts are validated; the
heartbeat TTL must exceed its interval.

Commented placeholders such as `RUN_WORKERS`, backpressure, and HTTP timeout
settings in `.env.example` are not implemented configuration options. Setting them
does not enable those capabilities. Leader eligibility affects API supervision,
not worker execution.

## API usage

Open [Swagger UI](http://localhost:8080/docs) or
[OpenAPI JSON](http://localhost:8080/openapi.json) when the API is running. The
following table summarizes registered routes; see the DLQ routing caveat below.

| Method | Path | Operation |
| --- | --- | --- |
| POST | `/api/tasks` | Submit a task |
| GET | `/api/tasks/{task_id}` | Read canonical task state |
| GET | `/api/tasks/failed` | Intended DLQ listing; currently shadowed by the task-ID route |
| GET | `/api/tasks/failed/redrive` | Redrive all retained failed tasks; changes state |
| GET | `/api/metrics` | Read processing, failure, retry, and queue counters |
| GET | `/api/metrics/enhanced` | Include submissions, delayed depth, DLQ depth, and success rate |
| GET | `/api/events` | Read recent task events |
| GET | `/api/events/cluster` | Read recent node/recovery lifecycle events |
| GET | `/api/workers` | Read executor state |
| GET | `/api/nodes` | Read node descriptors, liveness, capacity, and in-flight count |
| GET | `/api/health` | Check Redis readiness and supervisor failure state |
| GET | `/api/leader` | Inspect current API leadership |
| POST | `/api/cron` | Create a cron definition |
| GET | `/api/cron` | List definitions in ID order |
| PATCH | `/api/cron/{id}` | Pause or resume a definition |
| DELETE | `/api/cron/{id}` | Delete future scheduling |

### Submit immediate or delayed tasks

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"hash","payload":{"input":"hello","rounds":1000},"priority":5,"delay":0,"max_retries":3}'
```

Use `delay: 20` for execution eligibility after 20 seconds. Keep the returned task
ID and query it with `GET /api/tasks/{task_id}`. A task can be pending, processing,
completed, or failed. `max_retries=3` permits up to three additional attempts after
the initial one.

A supplied `id` makes submissions deduplicated by task ID. Reusing an existing ID
returns 409; successful creation returns 201. Task schema failures normally return
422, and unexpected task-service failures return 500. This differs from the cron
API's explicit 400/503 contract.

### Built-in handlers

| Type | Payload | Behavior |
| --- | --- | --- |
| `hash` | `{"input":"hello","rounds":1000}` | Repeated SHA-256 hashing; yields periodically during larger workloads |
| `sleep` | `{"duration_ms":800,"fail_rate":0.3}` | Sleep with simulated random failures |
| `http_fetch` | `{"url":"https://example.com"}` | HTTP GET; non-2xx responses fail the task |

Use a small `hash` workload for deterministic smoke tests. The sleep handler uses
random 200–800 ms duration when no positive duration is supplied. Its current
fallback also treats `fail_rate: 0` as the default 0.3, so zero does not disable
simulated failures. HTTP fetch has its own 10-second timeout and also remains
subject to the worker's configured handler limit.

### Inspect failures and monitoring

```bash
curl http://localhost:8080/api/metrics/enhanced
curl 'http://localhost:8080/api/events?limit=50'
curl http://localhost:8080/api/events/cluster
curl http://localhost:8080/api/nodes
curl http://localhost:8080/api/workers
```

The event limit defaults to 50 and is capped at 200. These endpoints are operational
views rather than a durable result/audit system.

**Current routing caveat:** `/api/tasks/failed` is registered after the dynamic
single-task route and currently performs a lookup for task ID `failed`. Do not
rely on it as a working DLQ listing endpoint until route ordering is fixed. See
[architecture limits](architecture.md#current-limits).

The registered `GET /api/tasks/failed/redrive` endpoint changes state by draining
and re-enqueuing retained failures. Invoke it only when intentionally retrying
that data. Redrive is currently a sequence of separate Redis operations.

### Inspect readiness and leadership

A ready API returns HTTP 200 with:

```json
{"status":"healthy","redis":"connected"}
```

Redis unavailability returns 503. Unexpected supervisor failure also returns 503,
with `background_services: "failed"`. A healthy follower can still report ready.

`GET /api/leader` returns the Redis owner's identity and whether the queried API
holds a matching valid local lease:

```json
{"leader_id":"hostname-random","is_self":false}
```

With no owner, the response is `{"leader_id":null,"is_self":false}`. An unavailable
leader query returns 503. If every API has `LEADER_ELIGIBLE=false`, delayed work,
recovery, and cron materialization pause even though HTTP and ready-task execution
can remain available.

### Create and manage cron jobs

```bash
curl -X POST http://localhost:8080/api/cron \
  -H 'Content-Type: application/json' \
  -d '{"id":"daily-hash","schedule":"0 2 * * *","type":"hash","payload":{"input":"cron","rounds":1000},"priority":5,"max_retries":3,"enabled":true}'
curl http://localhost:8080/api/cron
```

`id` is optional; supplied IDs use 1–128 letters, digits, `_`, or `-`. `schedule`
and nonblank `type` are required. Defaults are null payload, priority 0, retry
limit 0, and enabled true. Unknown fields and incorrectly typed values are rejected.

Schedules have five fields and use UTC. Day-of-month and day-of-week restrictions
use standard OR semantics. Seconds fields, aliases, random/hashed expressions,
and unreachable dates are rejected; search is bounded to eight years.

Job responses include creation time, `eligible_after_unix`, and nullable
`last_materialized_slot_unix`. Internal revision tokens are not exposed.

```bash
curl -X PATCH http://localhost:8080/api/cron/daily-hash \
  -H 'Content-Type: application/json' -d '{"enabled":false}'
curl -X PATCH http://localhost:8080/api/cron/daily-hash \
  -H 'Content-Type: application/json' -d '{"enabled":true}'
curl -X DELETE http://localhost:8080/api/cron/daily-hash
```

PATCH accepts only `enabled`. Resuming starts with future slots and repeating
`enabled=true` does not reset an already enabled job. Pausing/deletion leave
already submitted tasks intact. Following downtime, only the latest due slot is
materialized; older missed executions are skipped.

Create returns 201, list/update return 200, and successful delete returns 204.
Invalid cron input, including malformed JSON, returns 400; duplicate creation
returns 409; missing update/delete targets return 404; unavailable operations
return 503. Management requests can go to followers.

## Tests and checks

### Unit tests

```bash
uv run pytest -q
```

Without dedicated Redis test URLs, Redis integration tests skip. If those URLs
are already set in your shell and you want only the tests that do not use Redis:

```bash
env -u REAPER_TEST_REDIS_URL -u WORKER_TEST_REDIS_URL \
  -u LEADER_CRON_TEST_REDIS_URL uv run pytest -q
```

### Redis integration tests

**Fixtures call `FLUSHDB` before and after cases. Use a separate disposable Redis
instance, never a shared development, application, or production instance.**

Start a temporary test container on a different host port:

```bash
docker run --rm --detach --name taskqueue-test-redis \
  --publish 127.0.0.1:6389:6379 redis:8.2
docker exec taskqueue-test-redis redis-cli ping
```

Then run the suites with isolated databases:

```bash
REAPER_TEST_REDIS_URL=redis://127.0.0.1:6389/15 \
WORKER_TEST_REDIS_URL=redis://127.0.0.1:6389/14 \
LEADER_CRON_TEST_REDIS_URL=redis://127.0.0.1:6389/13 \
uv run pytest -q
```

| Test area | Files |
| --- | --- |
| Readiness and HTTP contracts | `tests/test_health.py`, `tests/test_cron_api.py` |
| Execution failures and lifecycle | `tests/test_executor_failure.py`, `tests/test_worker_lifecycle.py` |
| Task recovery | `tests/test_reaper.py`, `tests/test_worker_failure.py` |
| Election, cron races, and supervision | `tests/test_leader_cron_unit.py`, `tests/test_leader_cron_redis.py`, `tests/test_leader_lifecycle.py` |
| Two real APIs, worker execution, and failover | `tests/test_leader_cron_process.py` |

The process test overrides the test subprocesses' Redis factory to use the supplied
isolated URL; it does not change production DB selection. Run these suites
sequentially against a given test database because fixtures clear shared test keys.

Stop the temporary instance after testing:

```bash
docker stop taskqueue-test-redis
```

### Lint and formatting

Use actual changed paths in place of the examples:

```bash
uv run ruff check app/path/to/file.py tests/test_feature.py
uv run ruff format --check app/path/to/file.py tests/test_feature.py
git diff --check
```

For deliberate formatting changes, run `uv run ruff format` on those paths. Keep
fixes focused; existing unrelated lint findings do not justify reformatting the
whole repository.

## Development practices

### Dependencies

Use `uv add PACKAGE` for runtime dependencies and `uv add --dev PACKAGE` for tools
used only during development. Commit both `pyproject.toml` and `uv.lock` when
dependencies change. The Docker build installs with the following command, so
dependency changes require an image rebuild:

```bash
uv sync --locked --no-dev --no-install-project
```

### Extend behavior within existing boundaries

- Add handlers under `app/handler/` and register them in `create_registry()`.
  Handlers are async callables receiving a `Task` and returning a `Result`.
- Define/validate HTTP input, delegate to an application service, and wire shared
  dependencies in `app/container.py` where appropriate.
- Keep Redis persistence in stores and queue/lease transitions in their owning
  components. Consult the [architecture guide](architecture.md) before changes.
- Use async I/O. CPU work must yield or be moved to an appropriate executor to
  avoid starving heartbeats, renewal, and cancellation.
- Preserve `asyncio.CancelledError` through cleanup. Keep Redis open until its
  dependent tasks have finished releasing or recording recoverable state.
- Document classes/functions with concise docstrings and explain important
  decisions with inline comments. For Lua, describe every key, argument, return
  outcome, and race guard.
- Test competing transitions, retries/exhaustion, Redis errors, cancellation,
  shutdown, and lost responses where relevant.

### Documentation placement

Keep README short: product capabilities, a simple quick start, and links.
Use CONTRIBUTING for proposing changes, branches, review, and submission checks.
Put architecture, internal behavior, operational detail, and development
instructions under `docs/`. Keep `.env.example` aligned with real settings and
cross-link related documents when changing behavior.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| API cannot connect to Redis | Check `REDIS_ADDR`, Redis health, and whether settings were explicitly loaded; Compose always uses `redis:6379` |
| Host port is already allocated | Change `DOCKER_API_PORT` for Compose, or the native Redis/API host ports and settings |
| Source edits are not visible | Rebuild the application image; the default stack does not mount source |
| Delayed or cron tasks never run | Check `/api/leader`, eligibility, and supervisor logs; at least one API must own leadership |
| Handlers time out or work is reclaimed unexpectedly | Compare handler execution time, `DRAIN_TIMEOUT_MS`, and `VISIBILITY_TIMEOUT_MS` |
| A stopped worker still appears as dead | Allow heartbeat expiry, a leader recovery sweep, and the configured dead-node grace period |
| Redis integration tests skip | Set the dedicated test URLs only after starting a disposable test instance |
| `/api/tasks/failed` returns task-not-found | This is the current route-order limitation described above |
