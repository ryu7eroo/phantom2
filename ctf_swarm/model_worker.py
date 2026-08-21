from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

from redis.asyncio import Redis

from .agent import AgentContext
from .distributed_blackboard import DistributedBlackboard
from .models import Challenge, Evidence, Round
from .openai_compatible_provider import OpenAICompatibleProvider


def build_prompt(fields: dict[str, str], snapshot: dict[str, object]) -> str:
    round_name = fields.get("round", Round.INDEPENDENT.value)
    history = snapshot.get("history", [])
    dead_ends = snapshot.get("dead_ends", [])
    explored_paths = snapshot.get("explored_paths", [])
    open_hypotheses = snapshot.get("open_hypotheses", [])

    if round_name in {Round.INDEPENDENT.value, Round.COLLABORATIVE.value}:
        thinking = "/no_think"
    else:
        thinking = "/think"

    role_instruction = {
        Round.INDEPENDENT.value: "Work independently. Establish concrete observations and the most likely attack surface.",
        Round.COLLABORATIVE.value: "Synthesize the shared evidence. Resolve contradictions and extend the strongest useful lead.",
        Round.DIVERGENT.value: "Deliberately pursue a materially different path from explored/dead paths. Do not merely rename an existing hypothesis.",
        Round.ADVERSARIAL.value: "Act as an adversarial critic. Try to falsify the leading hypotheses, identify unsupported assumptions, and only propose a candidate when it has evidence.",
    }.get(round_name, "Analyze the task systematically.")

    return (
        "You are a CTF security research agent.\n"
        f"{role_instruction}\n\n"
        f"Title: {fields.get('title', '')}\n"
        f"Category: {fields.get('category', '')}\n"
        f"Round: {round_name}\n"
        f"Points: {fields.get('points', '0')}\n\n"
        f"Known dead ends: {json.dumps(dead_ends)}\n"
        f"Explored paths: {json.dumps(explored_paths)}\n"
        f"Open hypotheses: {json.dumps(open_hypotheses)}\n"
        f"Shared evidence history: {json.dumps(history[-12:])}\n\n"
        "Do not fabricate challenge data that was not provided. Distinguish hypotheses from verified facts. "
        "Return a concise evidence report, critique, or verified flag candidate.\n"
        f"Use {thinking}."
    )


def _tuple_field(item: dict[str, object], key: str) -> tuple[str, ...]:
    value = item.get(key, ())
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            value = decoded
        except json.JSONDecodeError:
            return (value,)
    if isinstance(value, (list, tuple, set)):
        return tuple(str(x) for x in value)
    return ()


def parse_response(text: str, worker_id: str, task_id: str, round_name: str) -> dict[str, object]:
    stripped = text.strip()
    marker = "LISA{" if "LISA{" in stripped else None
    if marker:
        start = stripped.index(marker)
        end = stripped.find("}", start)
        if end != -1:
            value = stripped[start : end + 1]
            return {"type": "candidate", "task_id": task_id, "worker_id": worker_id, "value": value, "proof": f"model-response:{worker_id}:{time.time_ns()}"}
    return {
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


async def run(worker_id: str, model: str, base_url: str, delay: float) -> None:
    redis = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
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
                    try:
                        current_round = Round(round_name)
                    except ValueError:
                        current_round = Round.INDEPENDENT
                        round_name = current_round.value

                    snapshot = await DistributedBlackboard(redis, task_id, round_name).snapshot()
                    evidence_items: list[Evidence] = []
                    for item in snapshot.get("history", []):
                        if not isinstance(item, dict):
                            continue
                        evidence_items.append(Evidence(
                            agent_id=str(item.get("agent_id", "unknown")),
                            challenge_id=task_id,
                            claim=str(item.get("claim", "")),
                            evidence=_tuple_field(item, "evidence"),
                            failed_paths=_tuple_field(item, "failed_paths"),
                            confidence=float(item.get("confidence", 0.0) or 0.0),
                            round=Round(str(item.get("round", Round.INDEPENDENT.value))),
                            tested_paths=_tuple_field(item, "tested_paths"),
                            next_hypotheses=_tuple_field(item, "next_hypotheses"),
                        ))

                    explored_paths = frozenset(str(x) for x in snapshot.get("explored_paths", []))
                    open_hypotheses = frozenset(str(x) for x in snapshot.get("open_hypotheses", []))
                    context = AgentContext(
                        challenge=Challenge(id=task_id, title=fields.get("title", ""), category=fields.get("category", "misc"), points=int(fields.get("points", "0"))),
                        round=current_round,
                        dead_ends=frozenset(str(x) for x in snapshot.get("dead_ends", [])),
                        explored_paths=explored_paths,
                        open_hypotheses=open_hypotheses,
                        evidence=tuple(evidence_items[-12:]),
                    )

                    pubsub = redis.pubsub()
                    await pubsub.subscribe(f"ctf:cancel:{task_id}")
                    solve_task = asyncio.create_task(provider.generate(context, build_prompt(fields, snapshot)))
                    cancel_task = asyncio.create_task(pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1))
                    try:
                        while True:
                            done, _ = await asyncio.wait({solve_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)
                            if solve_task in done:
                                result = solve_task.result()
                                event = parse_response(result.text, worker_id, task_id, round_name)
                                await redis.xadd("ctf:events", event, maxlen=10000, approximate=True)
                                print(f"[model-worker {worker_id}] emitted {event['type']} task={task_id} round={round_name}", flush=True)
                                break
                            message = cancel_task.result()
                            if message and message.get("channel") == f"ctf:cancel:{task_id}":
                                solve_task.cancel()
                                print(f"[model-worker {worker_id}] CANCELLED task={task_id}", flush=True)
                                break
                            cancel_task = asyncio.create_task(pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5))
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
