from __future__ import annotations

import json
from dataclasses import asdict

from redis.asyncio import Redis

from .models import Evidence, Round


class DistributedBlackboard:
    """Redis-backed evidence and round barrier for distributed workers."""

    def __init__(self, redis: Redis, task_id: str, round_name: str) -> None:
        self.redis = redis
        self.task_id = task_id
        self.round = round_name
        self.prefix = f"ctf:bb:{task_id}:{round_name}"
        self.pending_key = f"{self.prefix}:pending"
        self.evidence_key = f"{self.prefix}:evidence"
        self.dead_key = f"{self.prefix}:dead"
        self.open_key = f"{self.prefix}:open"

    async def register_workers(self, worker_ids: list[str]) -> None:
        if worker_ids:
            await self.redis.sadd(self.pending_key, *worker_ids)

    async def record_evidence(self, evidence: Evidence) -> bool:
        payload = asdict(evidence)
        payload["round"] = evidence.round.value
        await self.redis.hset(self.evidence_key, evidence.agent_id, json.dumps(payload))
        if evidence.failed_paths:
            await self.redis.sadd(self.dead_key, *evidence.failed_paths)
        if evidence.next_hypotheses:
            await self.redis.sadd(self.open_key, *evidence.next_hypotheses)
        await self.redis.srem(self.pending_key, evidence.agent_id)
        return await self.redis.scard(self.pending_key) == 0

    async def snapshot(self) -> dict[str, object]:
        raw = await self.redis.hgetall(self.evidence_key)
        evidence: list[dict[str, object]] = []
        for value in raw.values():
            evidence.append(json.loads(value))
        return {
            "task_id": self.task_id,
            "round": self.round,
            "evidence": evidence,
            "dead_ends": sorted(await self.redis.smembers(self.dead_key)),
            "open_hypotheses": sorted(await self.redis.smembers(self.open_key)),
            "pending_workers": sorted(await self.redis.smembers(self.pending_key)),
        }

    async def clear(self) -> None:
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
