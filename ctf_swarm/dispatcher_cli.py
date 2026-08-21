from __future__ import annotations

import asyncio
import logging
import os

from redis.asyncio import Redis

from .dispatcher import Dispatcher
from .persistence import init_database


logging.basicConfig(
    level=os.getenv("CTF_SWARM_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ctf_swarm.dispatcher_cli")


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://ctf:ctfdev@localhost:5432/ctf_swarm",
    )

    logger.info("initializing database")
    await init_database(database_url)

    logger.info("connecting to Redis: %s", redis_url)
    redis = Redis.from_url(redis_url, decode_responses=True)
    dispatcher = Dispatcher(redis, database_url)
    try:
        logger.info("dispatcher started: consumer=%s group=%s", dispatcher.consumer, dispatcher.group)
        await dispatcher.run_forever()
    finally:
        logger.info("dispatcher shutting down")
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("dispatcher stopped by user")
    except Exception:
        logger.exception("dispatcher crashed")
        raise
