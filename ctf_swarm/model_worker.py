from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

from redis.asyncio import Redis

from .agent import AgentContext
from .models import Challenge, Evidence, Round
from .openai_compatible_provider import OpenAICompatibleProvider


def build_prompt(fields: dict[str, str]) -> str:
    return (
        "Solve the following CTF challenge. Work systematically and use the shared evidence.\n"
        f"Title: {fields.get('title', '')}\n"
        f"Category: {fields.get('category', '')}\n"
        f"Round: {fields.get('round', 'independent')}\n"
        f"Points: {fields.get('points', '0')}\n\n"
        "Return either a concise hypothesis/evidence report or a verified flag candidate."
    )


def parse_response(text: str, worker_id: str, task_id: str, round_name: str) -> dict[str, object]:
    stripped = text.strip()
    marker = "LISA{" if "LISA{" in stripped else None
    if marker:
        start = stripped.index(marker)
        end = stripped.find("}", start)
        if end != -1:
            value = stripped[start : end + 1]
            return {
                "type": "candidate",
                "task_id": task_id,
                "worker_id": worker_id,
                "value": value,
                "proof": f"model-response:{worker_id}:{time.time_ns()}",
            }

    payload = {
        "type": "evidence",
        "task_id": task_id,
        "worker_id": worker_id,
        "round": round_name,
        "claim": stripped[:1000],
        "evidence": json.dumps([stripped[:1000]]),
        "tested_paths": json.dumps([]),
        "failed_paths": json.dumps([]),
        "next_hypotheses": json.dumps([]),
        "confidence": "0.30",
    }
    return payload


async def run(worker_id: str, model: str, base_url: str, delay: float) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = Redis.from_url(redis_url, decode_responses=True)
    provider = OpenAICompatibleProvider(model=model, base_url=base_url)
    stream = f"ctf:worker:{worker_id}"
    last_id = "0-0"
    try:
        print(f"[model-worker {worker_id}] model={model} endpoint={base_url}", flush=True)
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
                    round_name = fields.get("round", Round.INDEPENDENT.value)
                    pubsub = redis.pubsub()
                    await pubsub.subscribe(f"ctf:cancel:{task_id}")
                    context = AgentContext(
                        challenge=Challenge(
                            id=task_id,
                            title=fields.get("title", ""),
                            category=fields.get("category", "misc"),
                            points=int(fields.get("points", "0")),
                        ),
                        round=Round(round_name),
                        dead_ends=frozenset(),
                        evidence=(),
                    )
                    solve_task = asyncio.create_task(
                        provider.generate(context, build_prompt(fields))
                    )
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
                                result = solve_task.result()
                                event = parse_response(result.text, worker_id, task_id, round_name)
                                await redis.xadd("ctf:events", event, maxlen=10000, approximate=True)
                                print(f"[model-worker {worker_id}] emitted {event['type']} task={task_id}", flush=True)
                                break
                            cancel_task = asyncio.create_task(
                                pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                            )
                            message = next(iter(done)).result()
                            if message and message.get("channel") == f"ctf:cancel:{task_id}":
                                solve_task.cancel()
                                print(f"[model-worker {worker_id}] CANCELLED task={task_id}", flush=True)
                                break
                    finally:
                        if not solve_task.done():
                            solve_task.cancel()
                        if not cancel_task.done():
                            cancel_task.cancel()
                        await asyncio.gather(solve_task, cancel_task, return_exceptions=True)
                        await pubsub.unsubscribe(f"ctf:cancel:{task_id}")
                        await pubsub.aclose()
                    if delay:
                        await asyncio.sleep(delay)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model-backed CTF swarm worker")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--base-url", default=os.getenv("MODEL_BASE_URL", "http://127.0.0.1:8001/v1"))
    parser.add_argument("--delay", type=float, default=0.0)
    args = parser.parse_args()
    try:
        asyncio.run(run(args.worker_id, args.model, args.base_url, args.delay))
    except KeyboardInterrupt:
        pass
