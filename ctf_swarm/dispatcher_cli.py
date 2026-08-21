from __future__ import annotations

import asyncio
import os

from redis.asyncio import Redis

from .dispatcher import Dispatcher


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://ctf:ctfdev@localhost:5432/ctf_swarm",
    )
    redis = Redis.from_url(redis_url, decode_responses=True)
    dispatcher = Dispatcher(redis, database_url)
    try:
        await dispatcher.run_forever()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
