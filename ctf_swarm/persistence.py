from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import psycopg
from redis.asyncio import Redis


CREATE_TASKS_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    points INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    round TEXT NOT NULL DEFAULT 'independent',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_WORKERS_SQL = """
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    model TEXT NOT NULL,
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'ready',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_ASSIGNMENTS_SQL = """
CREATE TABLE IF NOT EXISTS assignments (
    assignment_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    round TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'assigned',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(task_id, worker_id, round)
)
"""


def _serialize_row(row: tuple[Any, ...], columns: list[str]) -> dict[str, Any]:
    result = dict(zip(columns, row))
    for key, value in list(result.items()):
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
    return result


async def init_database(database_url: str) -> None:
    def create() -> None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TASKS_SQL)
                cur.execute(CREATE_WORKERS_SQL)
                cur.execute(CREATE_ASSIGNMENTS_SQL)
            conn.commit()

    await asyncio.to_thread(create)


async def create_task(database_url: str, redis: Redis, title: str, category: str, points: int) -> dict[str, Any]:
    task_id = uuid4()

    def insert() -> dict[str, Any]:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks (id, title, category, points)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, title, category, points, status, round, created_at, updated_at
                    """,
                    (task_id, title, category, points),
                )
                row = cur.fetchone()
            conn.commit()
        assert row is not None
        result = _serialize_row(row, ["id", "title", "category", "points", "status", "round", "created_at", "updated_at"])
        result["id"] = str(result["id"])
        return result

    result = await asyncio.to_thread(insert)
    await redis.xadd("ctf:events", {
        "type": "task_created", "task_id": result["id"],
        "category": result["category"], "points": str(result["points"]),
    }, maxlen=10000, approximate=True)
    return result


async def get_task(database_url: str, task_id: UUID) -> dict[str, Any] | None:
    def fetch() -> dict[str, Any] | None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, title, category, points, status, round, created_at, updated_at FROM tasks WHERE id = %s",
                    (task_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        result = _serialize_row(row, ["id", "title", "category", "points", "status", "round", "created_at", "updated_at"])
        result["id"] = str(result["id"])
        return result

    return await asyncio.to_thread(fetch)


async def claim_task(database_url: str, task_id: UUID, status: str) -> bool:
    now = datetime.now(timezone.utc)

    def claim() -> bool:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE tasks SET status = %s, updated_at = %s WHERE id = %s AND status = 'queued'", (status, now, task_id))
                changed = cur.rowcount == 1
            conn.commit()
        return changed

    return await asyncio.to_thread(claim)


async def register_worker(database_url: str, redis: Redis, name: str, model: str, capabilities: dict[str, Any]) -> dict[str, Any]:
    worker_id = str(uuid4())

    def insert() -> dict[str, Any]:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workers (id, name, model, capabilities, status)
                    VALUES (%s, %s, %s, %s::jsonb, 'ready')
                    RETURNING id, name, model, capabilities, status, last_heartbeat, created_at
                    """,
                    (worker_id, name, model, json.dumps(capabilities)),
                )
                row = cur.fetchone()
            conn.commit()
        assert row is not None
        return _serialize_row(row, ["id", "name", "model", "capabilities", "status", "last_heartbeat", "created_at"])

    result = await asyncio.to_thread(insert)
    await redis.xadd("ctf:events", {"type": "worker_registered", "worker_id": result["id"], "model": result["model"]}, maxlen=10000, approximate=True)
    return result


async def heartbeat_worker(database_url: str, worker_id: str, status: str = "ready") -> bool:
    now = datetime.now(timezone.utc)

    def update() -> bool:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE workers SET status = %s, last_heartbeat = %s WHERE id = %s", (status, now, worker_id))
                changed = cur.rowcount > 0
            conn.commit()
        return changed

    return await asyncio.to_thread(update)


async def list_workers(database_url: str) -> list[dict[str, Any]]:
    def fetch() -> list[dict[str, Any]]:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, model, capabilities, status, last_heartbeat, created_at FROM workers ORDER BY created_at ASC")
                rows = cur.fetchall()
        return [_serialize_row(row, ["id", "name", "model", "capabilities", "status", "last_heartbeat", "created_at"]) for row in rows]

    return await asyncio.to_thread(fetch)


async def list_eligible_workers(database_url: str, category: str) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=45)

    def fetch() -> list[dict[str, Any]]:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, model, capabilities, status, last_heartbeat, created_at
                    FROM workers
                    WHERE status = 'ready'
                      AND last_heartbeat >= %s
                      AND (capabilities->'categories' IS NULL OR capabilities->'categories' @> %s::jsonb)
                    ORDER BY created_at ASC
                    """,
                    (cutoff, json.dumps([category])),
                )
                rows = cur.fetchall()
        return [_serialize_row(row, ["id", "name", "model", "capabilities", "status", "last_heartbeat", "created_at"]) for row in rows]

    return await asyncio.to_thread(fetch)


async def create_assignment(database_url: str, task_id: UUID, worker_id: str, round_name: str) -> dict[str, str]:
    assignment_id = uuid4()

    def insert() -> dict[str, str]:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO assignments (assignment_id, task_id, worker_id, round)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (task_id, worker_id, round)
                    DO UPDATE SET updated_at = NOW()
                    RETURNING assignment_id, task_id, worker_id, round
                    """,
                    (assignment_id, task_id, worker_id, round_name),
                )
                row = cur.fetchone()
            conn.commit()
        assert row is not None
        return {"assignment_id": str(row[0]), "task_id": str(row[1]), "worker_id": str(row[2]), "round": str(row[3])}

    return await asyncio.to_thread(insert)
