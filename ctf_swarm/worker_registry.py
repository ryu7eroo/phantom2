from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .persistence import heartbeat_worker, list_workers, register_worker
from .api import DATABASE_URL, REDIS_URL, Redis

router = APIRouter(prefix="/workers", tags=["workers"])


class WorkerCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    model: str = Field(min_length=1, max_length=200)
    capabilities: dict[str, Any] = Field(default_factory=dict)


class WorkerHeartbeat(BaseModel):
    status: str = Field(default="ready", min_length=1, max_length=50)


@router.post("/register", status_code=201)
async def register_worker_endpoint(payload: WorkerCreate) -> dict[str, Any]:
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


@router.post("/{worker_id}/heartbeat")
async def heartbeat_endpoint(worker_id: str, payload: WorkerHeartbeat) -> dict[str, Any]:
    ok = await heartbeat_worker(DATABASE_URL, worker_id, payload.status)
    if not ok:
        raise HTTPException(status_code=404, detail="worker not found")
    return {"ok": True, "worker_id": worker_id, "status": payload.status}


@router.get("")
async def list_workers_endpoint() -> list[dict[str, Any]]:
    return await list_workers(DATABASE_URL)
