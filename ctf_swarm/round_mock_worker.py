from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(frozen=True)
class WorkerPolicy:
    worker_id: str
    mode: str
    delay: float


async def get_blackboard(redis: Redis, task_id: str, round_name: str) -> dict[str, object]:
    key = f"ctf:bb:{task_id}:{round_name}"
    history_key = f"ctf:bb:{task_id}:history"
    evidence_raw = await redis.hgetall(f"{key}:evidence")
    history_raw = await redis.lrange(history_key, 0, -1)
    dead = sorted(await redis.smembers(f"ctf:bb:{task_id}:dead"))
    open_hypotheses = sorted(await redis.smembers(f"ctf:bb:{task_id}:open"))
    return {
        "evidence": [json.loads(v) for v in evidence_raw.values()],
        "history": [json.loads(v) for v in history_raw],
        "dead_ends": dead,
        "open_hypotheses": open_hypotheses,
    }


async def publish_evidence(redis: Redis, task_id: str, worker: WorkerPolicy, round_name: str) -> None:
    strategies = {
        "A": "static-analysis",
        "B": "protocol-abuse",
        "C": "data-flow",
    }
    strategy = strategies.get(worker.mode, "independent-analysis")
    failed = [f"{strategy}:round1"]
    hypotheses = [f"{worker.mode.lower()}-new-path"]
    if round_name == "collaborative":
        failed = [f"{strategy}:dead-end"]
        hypotheses = ["heap-or-type-confusion", f"{worker.mode.lower()}-cross-check"]
    payload = {
        "type": "evidence",
        "task_id": task_id,
        "worker_id": worker.worker_id,
        "round": round_name,
        "claim": f"{worker.mode} explored {strategy}",
        "evidence": json.dumps([f"{strategy}:observed"]),
        "tested_paths": json.dumps([strategy]),
        "failed_paths": json.dumps(failed),
        "next_hypotheses": json.dumps(hypotheses),
        "confidence": "0.55",
    }
    await redis.xadd("ctf:events", payload, maxlen=10000, approximate=True)


async def run(policy: WorkerPolicy) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = Redis.from_url(redis_url, decode_responses=True)
    stream = f"ctf:worker:{policy.worker_id}"
    last_id = "0-0"
    try:
        print(f"[round-worker {policy.worker_id}] listening mode={policy.mode}", flush=True)
        while True:
            entries = await redis.xread({stream: last_id}, count=1, block=1000)
            if not entries:
                continue
            for _, messages in entries:
                for message_id, fields in messages:
                    last_id = message_id
                    if fields.get("type") != "assignment_created":
                        continue
                    task_id = fields["task_id"]
                    round_name = fields["round"]
                    board = await get_blackboard(redis, task_id, round_name)
                    print(
                        f"[round-worker {policy.worker_id}] task={task_id} round={round_name} "
                        f"history={len(board['history'])} dead={len(board['dead_ends'])} open={len(board['open_hypotheses'])}",
                        flush=True,
                    )

                    pubsub = redis.pubsub()
                    await pubsub.subscribe(f"ctf:cancel:{task_id}")
                    solve_task = asyncio.create_task(asyncio.sleep(policy.delay))
                    cancel_task = asyncio.create_task(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                    )
                    try:
                        while True:
                            done, _ = await asyncio.wait(
                                {solve_task, cancel_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if solve_task in done:
                                if round_name == "divergent" and policy.mode == "B":
                                    proof = f"round3-proof:{policy.worker_id}:{time.time_ns()}"
                                    await redis.xadd(
                                        "ctf:events",
                                        {
                                            "type": "candidate",
                                            "task_id": task_id,
                                            "worker_id": policy.worker_id,
                                            "value": "LISA{distributed-round-win}",
                                            "proof": proof,
                                        },
                                        maxlen=10000,
                                        approximate=True,
                                    )
                                    print(
                                        f"[round-worker {policy.worker_id}] CANDIDATE round={round_name}",
                                        flush=True,
                                    )
                                else:
                                    await publish_evidence(redis, task_id, policy, round_name)
                                    print(
                                        f"[round-worker {policy.worker_id}] EVIDENCE round={round_name}",
                                        flush=True,
                                    )
                                break

                            message = next(iter(done)).result()
                            if message and message.get("channel") == f"ctf:cancel:{task_id}":
                                solve_task.cancel()
                                print(
                                    f"[round-worker {policy.worker_id}] CANCELLED task={task_id}",
                                    flush=True,
                                )
                                break
                            cancel_task = asyncio.create_task(
                                pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                            )
                    finally:
                        if not solve_task.done():
                            solve_task.cancel()
                        if not cancel_task.done():
                            cancel_task.cancel()
                        await asyncio.gather(solve_task, cancel_task, return_exceptions=True)
                        await pubsub.unsubscribe(f"ctf:cancel:{task_id}")
                        await pubsub.aclose()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed multi-round CTF swarm demo worker")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--mode", choices=["A", "B", "C"], required=True)
    parser.add_argument("--delay", type=float, default=0.2)
    args = parser.parse_args()
    asyncio.run(run(WorkerPolicy(args.worker_id, args.mode, args.delay)))
