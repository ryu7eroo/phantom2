from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from .persistence import create_task, get_task, init_database

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ctf:ctfdev@localhost:5432/ctf_swarm",
)


class TaskCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    category: str = Field(min_length=1, max_length=50)
    points: int = Field(default=0, ge=0)


class WorkerCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    model: str = Field(min_length=1, max_length=200)
    capabilities: dict[str, object] = Field(default_factory=dict)


class WorkerHeartbeat(BaseModel):
    status: str = Field(default="ready", min_length=1, max_length=50)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_database(DATABASE_URL)
    yield


app = FastAPI(title="CTF Swarm Control Plane", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, object]:
    redis_ok = False
    database_ok = False

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        redis_ok = bool(await redis.ping())
    except Exception:
        redis_ok = False
    finally:
        await redis.aclose()

    def ping_db() -> bool:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return cur.fetchone() == (1,)
        except Exception:
            return False

    database_ok = await asyncio.to_thread(ping_db)
    healthy = redis_ok and database_ok
    return {
        "status": "ok" if healthy else "degraded",
        "redis": redis_ok,
        "database": database_ok,
    }


@app.post("/tasks", status_code=201)
async def create_task_endpoint(payload: TaskCreate) -> dict[str, object]:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        return await create_task(
            DATABASE_URL,
            redis,
            payload.title,
            payload.category,
            payload.points,
        )
    finally:
        await redis.aclose()


@app.get("/tasks/{task_id}")
async def get_task_endpoint(task_id: UUID) -> dict[str, object]:
    task = await get_task(DATABASE_URL, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return task


@app.post("/workers/register", status_code=201)
async def register_worker_endpoint(payload: WorkerCreate) -> dict[str, object]:
    from .persistence import register_worker

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        return await register_worker(
            DATABASE_URL,
            redis,
            payload.name,
            payload.model,
            payload.capabilities,
        )
    finally:
        await redis.aclose()


@app.post("/workers/{worker_id}/heartbeat")
async def heartbeat_endpoint(worker_id: str, payload: WorkerHeartbeat) -> dict[str, object]:
    from .persistence import heartbeat_worker

    if not await heartbeat_worker(DATABASE_URL, worker_id, payload.status):
        raise HTTPException(status_code=404, detail="worker not found")
    return {"ok": True, "worker_id": worker_id, "status": payload.status}


@app.get("/workers")
async def list_workers_endpoint() -> list[dict[str, object]]:
    from .persistence import list_workers

    return await list_workers(DATABASE_URL)
