Read `README.md` for product behavior and `CONTRIBUTING.md` for the contributor workflow. This file records the implementation boundaries and invariants that are easy to miss when changing one part of the queue.

## agent workflow and communication Rules
- At final step, always provide a clear summary of finished task.
- If you make any code changes then:
    - Final Response must have a detailed explaination of implementation approach, design decisions, files/components changed and why, and how it works.

## Project context

- This is a Python 3.14+ distributed task queue. FastAPI serves the API; Redis stores task state and coordinates workers. Use `uv` with `pyproject.toml` and `uv.lock` for dependencies and commands.
- `app/main.py` starts the API, delayed-task scheduler, and reaper in one process. `app/run_worker.py` starts a separate worker node and executor pool. Keep these entrypoints separate unless a task explicitly changes the deployment model.
- `app/api/` handles HTTP, `app/service/` holds application operations, `app/broker/` owns leases and task claims, `app/queue/` owns ready and delayed queues, `app/reaper/` recovers abandoned work, `app/store/` owns Redis persistence, `app/worker/` runs tasks, and `app/handler/` contains handlers. Wire shared dependencies in `app/container.py` where appropriate.
- Configuration is defined and validated in `app/core/config.py`. It reads process environment variables; `.env.example` documents settings. Do not assume copying `.env.example` makes the application load `.env` automatically.

## Queue and Redis invariants

- Treat Redis task records and queue indexes as one logical state. The main keys and helpers live in `app/store/redis.py`. Ready and delayed tasks are sorted sets; processing is a sorted set of lease deadlines; worker-owned task IDs are sets; node heartbeats are TTL keys.
- Ready scores are negative priorities, delayed scores are Unix **seconds**, and processing lease scores are Unix **milliseconds**. Preserve those units when changing Python or Lua code.
- Use the existing Redis/Lua pattern for transitions that modify several keys. A task must not disappear between releasing a lease and entering ready, delayed, or the DLQ. Guard competing lease transitions with ownership checks and removal from `taskqueue:processing`.
- Lua scripts should document every `KEYS` and `ARGV` value, return codes, and the race guard. Keep scripts focused, and make retries safe if a client loses a response after Redis has executed a script.
- Keep canonical task records, DLQ entries, metrics, and events consistent. `FailedTask` fields must use their model names (`error`, `owner`, `failed_at`, and `reason`). Add or update tests whenever a Redis transition changes.
- The reaper sweeps dead nodes before expired leases. It uses a `SET NX` tombstone for one-time dead-node reporting, reclaims with a `ZREM` guard, and leaves dead nodes visible through `DEAD_NODE_GRACE_MS` before pruning. Preserve idempotence when multiple API instances or retries run a sweep.

## Lifecycle and failure handling

- Worker nodes register and heartbeat while running. On SIGINT/SIGTERM, stop taking new work, let in-flight work drain, then remove worker state and deregister. Keep the heartbeat active through the drain; if draining fails, leave the node discoverable so the reaper can recover its leases.
- Propagate `asyncio.CancelledError` through cleanup. Do not silently swallow registration, startup, or unexpected worker-exit errors. Ancillary event and monitoring failures may be logged without reclassifying a completed task as failed.
- `DRAIN_TIMEOUT_MS` currently limits each handler execution and therefore bounds a normal in-flight drain. Keep configuration comments and shutdown behavior aligned if this changes.
- Close Redis clients and cancel background tasks during shutdown. Avoid blocking I/O in async code and avoid broad `except` blocks that hide task loss.

## Checks and safe test data

- Run unit tests with `uv run pytest -q`. The Redis integration suites skip unless their dedicated URLs are set: `REAPER_TEST_REDIS_URL` and `WORKER_TEST_REDIS_URL`.
- Integration fixtures call `FLUSHDB`. Point each URL at an isolated, disposable Redis database or instance; never use a shared development or production database. For example, use DB 15 for reaper tests and DB 14 for worker-failure tests on a temporary Redis container.
- Run `uv run ruff check <changed paths>` and `git diff --check` for code changes. Keep fixes focused; do not reformat unrelated files just to clear existing lint findings.
- Add tests for meaningful behavior and failure cases, especially lease races, retry exhaustion, Redis errors, cancellation, and shutdown. Update `README.md` and `.env.example` when public behavior or configuration changes.
- Preserve unrelated work already in the working tree. Inspect `git status` before editing, and do not overwrite user changes.
