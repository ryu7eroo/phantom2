import asyncio
import json

from ctf_swarm.distributed_blackboard import DistributedBlackboard, next_round
from ctf_swarm.models import Evidence, Round


class FakeRedis:
    def __init__(self):
        self.sets = {}
        self.hashes = {}

    async def sadd(self, key, *values):
        bucket = self.sets.setdefault(key, set())
        before = len(bucket)
        bucket.update(values)
        return len(bucket) - before

    async def srem(self, key, *values):
        bucket = self.sets.setdefault(key, set())
        removed = 0
        for value in values:
            if value in bucket:
                bucket.remove(value)
                removed += 1
        return removed

    async def scard(self, key):
        return len(self.sets.get(key, set()))

    async def smembers(self, key):
        return self.sets.get(key, set())

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    async def hgetall(self, key):
        return self.hashes.get(key, {})

    async def delete(self, *keys):
        for key in keys:
            self.sets.pop(key, None)
            self.hashes.pop(key, None)
        return len(keys)


def test_next_round_sequence():
    assert next_round(Round.INDEPENDENT.value) == Round.COLLABORATIVE.value
    assert next_round(Round.COLLABORATIVE.value) == Round.DIVERGENT.value
    assert next_round(Round.DIVERGENT.value) is None


def test_blackboard_opens_next_round_only_after_all_workers_report():
    async def run():
        redis = FakeRedis()
        board = DistributedBlackboard(redis, "task-1", Round.INDEPENDENT.value)
        await board.register_workers(["a", "b", "c"])

        evidence = lambda worker: Evidence(
            worker,
            "task-1",
            "tested path",
            tested_paths=(worker,),
            failed_paths=(f"dead-{worker}",),
            next_hypotheses=(f"open-{worker}",),
            confidence=0.5,
            round=Round.INDEPENDENT,
        )

        assert await board.record_evidence(evidence("a")) is False
        assert await board.record_evidence(evidence("b")) is False
        assert await board.record_evidence(evidence("c")) is True

        snapshot = await board.snapshot()
        assert snapshot["pending_workers"] == []
        assert sorted(snapshot["dead_ends"]) == ["dead-a", "dead-b", "dead-c"]
        assert sorted(snapshot["open_hypotheses"]) == ["open-a", "open-b", "open-c"]
        assert len(snapshot["evidence"]) == 3
        json.dumps(snapshot)

    asyncio.run(run())
