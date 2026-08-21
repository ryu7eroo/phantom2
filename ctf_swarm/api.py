from __future__ import annotations

import os

from fastapi import FastAPI
from redis.asyncio import Redis

app = FastAPI(title="CTF Swarm Control Plane", version="0.1.0")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ctf:ctfdev@localhost:5432/ctf_swarm",
)


@app.get("/health")
async def health() -> dict[str, object]:
    redis_ok = False
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        redis_ok = bool(await redis.ping())
    except Exception:
        redis_ok = False
    finally:
        await redis.aclose()

    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "database_configured": bool(DATABASE_URL),
    }
