from __future__ import annotations

import json
from dataclasses import asdict

from redis.asyncio import Redis

from .models import Evidence, Round


class DistributedBlackboard:
    """Redis-backed evidence, history, and round barrier for distributed workers."""

    def __init__(self, redis: Redis, task_id: str, round_name: str) -> None:
        self.redis = redis
        self.task_id = task_id
        self.round = round_name
        self.prefix = f"ctf:bb:{task_id}:{round_name}"
        self.pending_key = f"{self.prefix}:pending"
        self.evidence_key = f"{self.prefix}:evidence"
        self.dead_key = f"{self.prefix}:dead"
        self.open_key = f"{self.prefix}:open"
        self.history_key = f"ctf:bb:{task_id}:history"

    async def register_workers(self, worker_ids: list[str]) -> None:
        if worker_ids:
            await self.redis.sadd(self.pending_key, *worker_ids)

    async def record_evidence(self, evidence: Evidence) -> bool:
        payload = asdict(evidence)
        payload["round"] = evidence.round.value
        encoded = json.dumps(payload)
        await self.redis.hset(self.evidence_key, evidence.agent_id, encoded)
        await self.redis.rpush(self.history_key, encoded)
        if evidence.failed_paths:
            await self.redis.sadd(self.dead_key, *evidence.failed_paths)
        if evidence.next_hypotheses:
            await self.redis.sadd(self.open_key, *evidence.next_hypotheses)
        await self.redis.srem(self.pending_key, evidence.agent_id)
        return await self.redis.scard(self.pending_key) == 0

    async def snapshot(self) -> dict[str, object]:
        raw_history = await self.redis.lrange(self.history_key, 0, -1)
        history = [json.loads(value) for value in raw_history]
        current_raw = await self.redis.hgetall(self.evidence_key)
        current_round_evidence = [json.loads(value) for value in current_raw.values()]
        return {
            "task_id": self.task_id,
            "round": self.round,
            "evidence": current_round_evidence,
            "history": history,
            "dead_ends": sorted(await self.redis.smembers(self.dead_key)),
            "open_hypotheses": sorted(await self.redis.smembers(self.open_key)),
            "pending_workers": sorted(await self.redis.smembers(self.pending_key)),
        }

    async def clear(self) -> None:
        # Round-local barrier state can be cleared; task-wide history is retained.
        await self.redis.delete(self.pending_key, self.evidence_key, self.dead_key, self.open_key)


ROUND_SEQUENCE = [Round.INDEPENDENT, Round.COLLABORATIVE, Round.DIVERGENT]


def next_round(round_name: str) -> str | None:
    try:
        current = Round(round_name)
    except ValueError:
        return None
    try:
        index = ROUND_SEQUENCE.index(current)
    except ValueError:
        return None
    if index + 1 >= len(ROUND_SEQUENCE):
        return None
    return ROUND_SEQUENCE[index + 1].value
