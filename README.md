# Distributed Task Queue

A Python task queue for running background work across multiple workers, with
Redis-backed task storage and a FastAPI HTTP API.

## Features

- Immediate, priority-based, and delayed task execution.
- Retries with exponential backoff and dead-letter handling.
- UTC cron schedules with pause and resume.
- Worker recovery, graceful shutdown, and automatic API leader election.
- Task status, events, metrics, and worker/node inspection.

## Quick start

Install Docker with Compose and BuildKit support, then run:

```bash
git clone https://github.com/vikash1807/Distributed_Task_Queue.git
cd Distributed_Task_Queue
docker compose up --build -d --wait
```

The API is available at `http://localhost:8080`. Open the
[interactive API documentation](http://localhost:8080/docs), or check readiness:

```bash
curl http://localhost:8080/api/health
```

Submit a task:

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"hash","payload":{"input":"hello","rounds":1000},"max_retries":1}'
```

Use the returned task ID with `GET /api/tasks/{id}` to check its status.
Stop the local stack with `docker compose down`.

## Documentation

- [Development guide](docs/develoment.md): native setup, Docker commands,
  configuration, API examples, testing, and troubleshooting.
- [Architecture](docs/architecture.md): components, task lifecycle, Redis data,
  leader election, cron materialization, failure handling, and current limits.
- [Contributing](CONTRIBUTING.md): issues, branches, code review, and pull requests.
- [Environment example](.env.example): available configuration settings.

The Compose setup is intended for local use. See the development guide for
configuration and deployment considerations.
