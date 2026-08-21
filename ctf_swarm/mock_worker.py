from __future__ import annotations

import argparse
import asyncio
import os
import time

from redis.asyncio import Redis


async def main(worker_id: str, delay: float, flag: str) -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = Redis.from_url(redis_url, decode_responses=True)
    assignment_stream = f"ctf:worker:{worker_id}"
    last_id = "0-0"

    try:
        print(f"[worker {worker_id}] listening delay={delay:.3f}s", flush=True)
        while True:
            entries = await redis.xread({assignment_stream: last_id}, count=1, block=1000)
            if not entries:
                continue
            for _stream, messages in entries:
                for message_id, fields in messages:
                    last_id = message_id
                    if fields.get("type") != "assignment_created":
                        continue
                    task_id = fields["task_id"]
                    print(f"[worker {worker_id}] assignment task={task_id}", flush=True)
                    pubsub = redis.pubsub()
                    await pubsub.subscribe(f"ctf:cancel:{task_id}")
                    solve_task = asyncio.create_task(asyncio.sleep(delay))
                    cancel_task = asyncio.create_task(pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1))
                    try:
                        while True:
                            done, _ = await asyncio.wait({solve_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)
                            if solve_task in done:
                                value = flag
                                proof = f"mock-proof:{worker_id}:{time.time_ns()}"
                                await redis.xadd("ctf:events", {"type": "candidate", "task_id": task_id, "worker_id": worker_id, "value": value, "proof": proof}, maxlen=10000, approximate=True)
                                print(f"[worker {worker_id}] CANDIDATE task={task_id}", flush=True)
                                break
                            cancel_task = asyncio.create_task(pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5))
                            message = next(iter(done)).result()
                            if message and message.get("channel") == f"ctf:cancel:{task_id}":
                                solve_task.cancel()
                                print(f"[worker {worker_id}] CANCELLED task={task_id}", flush=True)
                                break
                    finally:
                        if not solve_task.done():
                            solve_task.cancel()
                        if not cancel_task.done():
                            cancel_task.cancel()
                        await asyncio.gather(solve_task, cancel_task, return_exceptions=True)
                        await pubsub.unsubscribe(f"ctf:cancel:{task_id}")
                        await pubsub.close()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cancellable CTF Swarm mock worker")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--delay", type=float, required=True)
    parser.add_argument("--flag", default="LISA{mock-race-win}")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.worker_id, args.delay, args.flag))
    except KeyboardInterrupt:
        pass
