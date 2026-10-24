"""Two real API processes and a worker recover after the API leader crashes."""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time

import httpx
import pytest
from redis.asyncio import Redis

from app.model import Task, TaskStatus
from app.service.cron import latest_slot
from app.store.redis import KEY_EVENTS, KEY_PROCESSING, key_cron, key_task
from app.store.task import TaskStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    """Run async tests on asyncio, matching the application runtime."""
    return "asyncio"


def unused_port():
    """Ask the OS for a free loopback port for a child API process."""
    with socket.socket() as connection:
        connection.bind(("127.0.0.1", 0))
        return connection.getsockname()[1]


async def test_api_process_failover_and_worker_execution(tmp_path):
    """Crash a real API leader and verify its successor can schedule and recover work."""
    url = os.getenv("LEADER_CRON_TEST_REDIS_URL")
    if not url:
        pytest.skip("requires LEADER_CRON_TEST_REDIS_URL for a disposable Redis DB")
    redis = Redis.from_url(url, decode_responses=True)
    processes = []
    logs = []
    ports = [unused_port(), unused_port()]
    # Inject the dedicated test DB without changing production's Redis DB 0.
    script = (
        "import os; from redis.asyncio import Redis; import {module} as entry; "
        "entry.new_redis = lambda **kwargs: Redis.from_url("
        "os.environ['LEADER_CRON_TEST_REDIS_URL'], decode_responses=True); entry.main()"
    )
    settings = dict(
        os.environ,
        LEADER_ELIGIBLE="true",
        LEADER_TTL_MS="600",
        CRON_TICK_MS="50",
        REAPER_INTERVAL_MS="50",
        WORKER_COUNT="1",
        DRAIN_TIMEOUT_MS="2000",
        HEARTBEAT_INTERVAL_MS="500",
        HEARTBEAT_TTL_MS="3000",
        SIGNAL_BLOCK_MS="100",
    )
    try:
        await redis.flushdb()
        for index, module in enumerate(("app.main", "app.main", "app.run_worker")):
            log = (tmp_path / f"process-{index}.log").open("w+")
            logs.append(log)
            env = dict(settings, SERVER_PORT=str(ports[index]) if index < 2 else "8080")
            processes.append(
                subprocess.Popen(
                    [sys.executable, "-c", script.format(module=module)],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            )

        async with httpx.AsyncClient(timeout=1) as client:

            async def request(index, method, path, **kwargs):
                """Send an API request to the selected child process."""
                return await client.request(
                    method,
                    f"http://127.0.0.1:{ports[index]}/api{path}",
                    **kwargs,
                )

            async def wait(predicate, timeout=10):
                """Poll until success, tolerating connection errors while processes start."""
                async with asyncio.timeout(timeout):
                    while True:
                        try:
                            result = await predicate()
                            if result:
                                return result
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.02)

            for index in (0, 1):

                async def ready(index=index):
                    """Check that this child API is ready to accept work."""
                    return (await request(index, "GET", "/health")).status_code == 200

                await wait(ready)

            async def has_leader():
                """Wait for the API cluster to report a nonempty leader identity."""
                status = (await request(0, "GET", "/leader")).json()
                return status if status["leader_id"] else None

            status = await wait(has_leader)
            winner = 0 if status["is_self"] else 1
            follower = 1 - winner
            follower_status = (await request(follower, "GET", "/leader")).json()
            assert not follower_status["is_self"]
            assert follower_status["leader_id"] == status["leader_id"]

            async def completed(task_id):
                """Check task completion through the surviving API process."""
                response = await request(follower, "GET", f"/tasks/{task_id}")
                return response.status_code == 200 and response.json()["status"] == "completed"

            response = await request(
                follower,
                "POST",
                "/cron",
                json={
                    "id": "before",
                    "schedule": "* * * * *",
                    "type": "hash",
                    "payload": {"rounds": 100},
                },
            )
            assert response.status_code == 201
            # Simulate a job that missed ten minutes during prior downtime.
            now = int((await redis.time())[0])
            await redis.hset(key_cron("before"), "eligible_after_unix", now - 600)
            before_id = f"before-{latest_slot('* * * * *', now)}"
            await wait(lambda: completed(before_id))
            assert (
                await request(
                    follower,
                    "PATCH",
                    "/cron/before",
                    json={
                        "enabled": False,
                    },
                )
            ).status_code == 200

            # Crash rather than release: the successor must wait for expiry.
            processes[winner].kill()
            await asyncio.to_thread(processes[winner].wait)

            async def took_over():
                """Require the survivor to report itself as the new leader."""
                new = (await request(follower, "GET", "/leader")).json()
                return new["is_self"] and new["leader_id"] != status["leader_id"]

            await wait(took_over)

            response = await request(
                follower,
                "POST",
                "/cron",
                json={
                    "id": "after",
                    "schedule": "* * * * *",
                    "type": "hash",
                    "payload": {"rounds": 100},
                },
            )
            assert response.status_code == 201
            now = int((await redis.time())[0])
            await redis.hset(key_cron("after"), "eligible_after_unix", now - 600)
            after_id = f"after-{latest_slot('* * * * *', now)}"
            await wait(lambda: completed(after_id))

            assert (
                await request(
                    follower,
                    "POST",
                    "/tasks",
                    json={
                        "id": "delayed",
                        "type": "hash",
                        "delay": 1,
                        "payload": {"rounds": 100},
                    },
                )
            ).status_code == 201
            await wait(lambda: completed("delayed"))
            await TaskStore(redis).save(
                Task(
                    id="expired",
                    type="hash",
                    max_retries=1,
                    status=TaskStatus.PROCESSING,
                )
            )
            await redis.zadd(KEY_PROCESSING, {"expired": time.time() * 1000 - 1})
            await wait(lambda: completed("expired"))
            assert await redis.hget(key_task("expired"), "retries") == "1"
            events = [json.loads(value) for value in await redis.lrange(KEY_EVENTS, 0, -1)]
            for task_id in (before_id, after_id):
                assert (
                    sum(
                        event["type"] == "submitted" and event["task_id"] == task_id
                        for event in events
                    )
                    == 1
                )
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                await asyncio.to_thread(process.wait, timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)
        for log in logs:
            log.seek(0)
            print(log.read())
            log.close()
        await redis.flushdb()
        await redis.aclose()
