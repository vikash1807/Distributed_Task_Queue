# Distributed Task Queue

A Redis-backed distributed task queue built with **Python, FastAPI, asyncio, and Redis**.

## What it supports

* Task submission and persistent task state
* Priority-based task queue
* Delayed task execution
* Async worker pool
* Redis/Lua atomic operations
* Visibility timeouts and task leases
* Lease extension and ownership checks
* Retries with exponential backoff
* Dead-letter queue (DLQ)
* Failed-task inspection and redrive
* Task and cluster events
* Processing and queue metrics
* Worker/node state APIs
* Node registration and TTL-based heartbeats
* Automatic recovery of expired leases and dead worker nodes
* Graceful worker shutdown

## Architecture

```text
                 FastAPI
                    |
          +---------+---------+
          |                   |
      Task / Metrics      Events / Nodes
          |                   |
          +---------+---------+
                    |
                  Redis
                    |
       +------------+------------+
       |            |            |
    Ready        Delayed      Processing
    Queue         Queue          Leases
       |                         |
       +-----------+-------------+
                   |
              Worker Nodes
           +-------+-------+
           |               |
        Executor        Executor
           |               |
           +-------+-------+
                   |
                Handlers
```

Redis is the central coordination and state store. Lua scripts are used for operations where multiple Redis updates need to happen atomically.

## Tech Stack

* Python 3.14+
* FastAPI
* Redis
* asyncio
* Uvicorn
* Pydantic Settings
* httpx
* uv
* Ruff

## Project Structure

```text
app/
├── api/
│   ├── routes/
│   │   ├── task.py
│   │   ├── metrics.py
│   │   ├── events.py
│   │   └── worker_nodes.py
│   ├── dependencies.py
│   ├── middleware.py
│   └── router.py
├── broker/
│   ├── broker.py
│   └── scripts/
├── core/
│   ├── config.py
│   └── logging.py
├── handler/
│   ├── builtins.py
│   └── registry.py
├── model/
├── queue/
│   ├── queue.py
│   ├── delayed.py
│   └── scripts/
├── reaper/
│   ├── reaper.py
│   └── scripts/
├── service/
├── store/
├── worker/
│   ├── executor.py
│   ├── pool.py
│   └── node.py
├── container.py
├── main.py
└── run_worker.py
```

## Getting Started

### 1. Clone

```bash
git clone https://github.com/vikash1807/Distributed_Task_Queue.git
cd Distributed_Task_Queue
```

### 2. Install dependencies

The project uses Python 3.14+ and `uv`.

```bash
uv sync
```

If needed, install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Start Redis

Redis is required.

Using Docker:

```bash
docker run --name taskqueue-redis -p 6379:6379 -d redis
```

Check it:

```bash
docker exec -it taskqueue-redis redis-cli ping
```

Expected output:

```text
PONG
```

### 4. Configure environment

Copy the example configuration:

```bash
cp .env.example .env
```

The default setup uses:

```text
REDIS_ADDR=localhost:6379
SERVER_PORT=8080
WORKER_COUNT=5
VISIBILITY_TIMEOUT_MS=30000
```

The main configuration options are documented in `.env.example`.

## Run the Application

Start the API:

```bash
uv run python -m app.main
```

The API is available at:

```text
http://localhost:8080
```

Start a worker in another terminal:

```bash
uv run python -m app.run_worker
```

You can start multiple worker processes to simulate multiple nodes in the cluster.

## Example: Submit a Task

### Sleep Task

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "type": "sleep",
    "payload": {
      "duration_ms": 800
    },
    "priority": 5,
    "delay": 0,
    "max_retries": 3
  }'
```

### Delayed Task

Set `delay` to the number of seconds before the task becomes ready:

```json
{
  "type": "sleep",
  "payload": {
    "duration_ms": 800
  },
  "priority": 5,
  "delay": 20,
  "max_retries": 3
}
```

## Built-in Handlers

### `sleep`

Useful for testing worker execution and failures.

```json
{
  "type": "sleep",
  "payload": {
    "duration_ms": 800,
    "fail_rate": 0.3
  }
}
```

### `http_fetch`

Performs an HTTP GET request.

```json
{
  "type": "http_fetch",
  "payload": {
    "url": "https://example.com"
  }
}
```

### `hash`

Performs repeated SHA-256 hashing and can be used for CPU-heavy task testing.

```json
{
  "type": "hash",
  "payload": {
    "input": "hello",
    "rounds": 100000
  }
}
```

## API Endpoints

### Tasks

```text
POST /api/tasks
GET  /api/tasks/{task_id}
GET  /api/tasks/failed
GET  /api/tasks/failed/redrive
```

### Metrics

```text
GET /api/metrics
GET /api/metrics/enhanced
```

### Health

`GET /api/health` checks Redis. It returns HTTP 200 with
`{"status":"healthy","redis":"connected"}` when ready, or HTTP 503 with
`{"status":"unhealthy","redis":"unavailable"}` when Redis is unavailable.

Worker processes handle SIGINT and SIGTERM by finishing in-flight tasks,
removing their worker state, and deregistering their node. `DRAIN_TIMEOUT_MS`
limits each task handler's execution time and therefore bounds normal drain time.

### Events

```text
GET /api/events
GET /api/events/cluster
```

### Workers and Nodes

```text
GET /api/workers
GET /api/nodes
```

## How Task Processing Works

A normal task follows this flow:

```text
Submit
  |
  v
Task record
  |
  +---- delayed ----> Delayed Queue ----+
  |                                     |
  +-------------------------------------+
                    |
                    v
                Ready Queue
                    |
                    v
                Processing
                    |
          +---------+---------+
          |                   |
       Success             Failure
          |                   |
          v                   v
      Completed             Retry
                              |
                     retries remaining?
                       /           \
                     yes           no
                      |             |
                      v             v
                   Queue          DLQ
```

When a worker claims a task, Redis stores a lease for it. The lease prevents a task from being considered permanently owned by a worker that may have stopped responding.

The API runs a background reaper every `REAPER_INTERVAL_MS` (default 5000). It first
reclaims tasks from nodes whose heartbeat has expired, then scans expired leases.
Reclaimed tasks consume a retry and return to the ready queue; tasks with no
retries left move to the DLQ. Dead nodes remain visible as `alive=false` for
`DEAD_NODE_GRACE_MS` (default 30000) before they are removed from the node list.

## Retries and Dead Letters

A failed task can be retried up to `max_retries`.

Retry delays use exponential backoff and are capped by the queue implementation.

When retries are exhausted, the task is moved to the dead-letter queue.

Failed tasks can be inspected and redriven through:

```text
GET /api/tasks/failed
GET /api/tasks/failed/redrive
```

## Cluster and Worker State

Each worker process registers as a node and maintains a Redis TTL-based heartbeat.

The cluster exposes:

* Node identity
* Hostname
* Role
* Capacity
* Liveness
* In-flight task count
* Individual worker state

Useful endpoints:

```text
GET /api/nodes
GET /api/workers
GET /api/events/cluster
```

## Redis Data Model

Some of the main Redis structures are:

```text
taskqueue:task:{id}       Task record
taskqueue:ready           Ready-task ZSET
taskqueue:delayed         Delayed-task ZSET
taskqueue:processing      Processing/lease ZSET
taskqueue:deadletter      Dead-letter LIST
taskqueue:metrics         Metrics HASH
taskqueue:events          Event LIST
taskqueue:events:cluster  Cluster event LIST
taskqueue:nodes           Node SET
taskqueue:workers         Worker state HASH
```

Lua scripts are used for critical atomic queue and lease operations.

## Development

Run Ruff:

```bash
uv run ruff check .
```
