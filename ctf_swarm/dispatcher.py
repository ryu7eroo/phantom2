from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import UUID

from redis.asyncio import Redis

from .persistence import (
    claim_task,
    create_assignment,
    get_task,
    list_eligible_workers,
    list_queued_tasks,
)


@dataclass(frozen=True)
class Assignment:
    assignment_id: str
    task_id: str
    worker_id: str
    round: str


class Dispatcher:
    """Redis-stream driven dispatcher with atomic task claiming and queue recovery."""

    def __init__(self, redis: Redis, database_url: str) -> None:
        self.redis = redis
        self.database_url = database_url
        self.stream = "ctf:events"
        self.group = "ctf-dispatchers"
        self.consumer = f"dispatcher-{int(time.time() * 1000)}"
        self._running = False

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def dispatch_task(self, task_id: str) -> list[Assignment]:
        task = await get_task(self.database_url, UUID(task_id))
        if task is None or task["status"] != "queued":
            return []

        workers = await list_eligible_workers(self.database_url, task["category"])
        if not workers:
            return []

        claimed = await claim_task(self.database_url, UUID(task_id), "dispatched")
        if not claimed:
            return []

        return await self._create_assignments(task, workers)

    async def dispatch_queued(self, category: str | None = None) -> int:
        tasks = await list_queued_tasks(self.database_url, category)
        dispatched = 0
        for task in tasks:
            assignments = await self.dispatch_task(task["id"])
            if assignments:
                dispatched += 1
        return dispatched

    async def _create_assignments(
        self,
        task: dict[str, object],
        workers: list[dict[str, object]],
    ) -> list[Assignment]:
        task_id = str(task["id"])
        assignments: list[Assignment] = []
        for worker in workers:
            assignment = await create_assignment(
                self.database_url,
                UUID(task_id),
                str(worker["id"]),
                str(task["round"]),
            )
            typed_assignment = Assignment(**assignment)
            assignments.append(typed_assignment)
            await self.redis.xadd(
                f"ctf:worker:{worker['id']}",
                {
                    "type": "assignment_created",
                    "assignment_id": assignment["assignment_id"],
                    "task_id": task_id,
                    "round": str(task["round"]),
                    "category": str(task["category"]),
                    "title": str(task["title"]),
                    "points": str(task["points"]),
                },
                maxlen=1000,
                approximate=True,
            )

        await self.redis.xadd(
            self.stream,
            {
                "type": "task_dispatched",
                "task_id": task_id,
                "assignment_count": str(len(assignments)),
            },
            maxlen=10000,
            approximate=True,
        )
        return assignments

    async def emit_cancel(self, task_id: str, reason: str = "global_solved") -> None:
        await self.redis.xadd(
            self.stream,
            {
                "type": "task_cancel",
                "task_id": task_id,
                "reason": reason,
            },
            maxlen=10000,
            approximate=True,
        )

    async def consume_once(self) -> int:
        await self.ensure_group()
        entries = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: ">"},
            count=50,
            block=1000,
        )
        handled = 0
        for _stream, messages in entries:
            for message_id, fields in messages:
                event = {str(k): str(v) for k, v in fields.items()}
                event_type = event.get("type")
                if event_type == "task_created":
                    await self.dispatch_task(event["task_id"])
                elif event_type == "worker_registered":
                    await self.dispatch_queued()
                elif event_type == "verified":
                    await self.emit_cancel(event["task_id"], "verified")
                await self.redis.xack(self.stream, self.group, message_id)
                handled += 1

        # Recover queued tasks after a dispatcher restart even when the original
        # task_created event was already acknowledged before a worker was ready.
        await self.dispatch_queued()
        return handled

    async def run_forever(self) -> None:
        self._running = True
        await self.ensure_group()
        while self._running:
            await self.consume_once()

    def stop(self) -> None:
        self._running = False
