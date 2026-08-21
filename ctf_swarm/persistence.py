from __future__ import annotations

from dataclasses import asdict
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


async def init_database(database_url: str) -> None:
    def create() -> None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TASKS_SQL)
            conn.commit()

    import asyncio

    await asyncio.to_thread(create)


async def create_task(
    database_url: str,
    redis: Redis,
    title: str,
    category: str,
    points: int,
) -> dict[str, Any]:
    task_id = uuid4()

    def insert() -> dict[str, Any]:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks (id, title, category, points)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, title, category, points, status, round,
                              created_at, updated_at
                    """,
                    (task_id, title, category, points),
                )
                row = cur.fetchone()
            conn.commit()
        assert row is not None
        columns = [
            "id", "title", "category", "points", "status", "round",
            "created_at", "updated_at",
        ]
        result = dict(zip(columns, row))
        result["id"] = str(result["id"])
        result["created_at"] = result["created_at"].isoformat()
        result["updated_at"] = result["updated_at"].isoformat()
        return result

    import asyncio

    result = await asyncio.to_thread(insert)
    await redis.xadd(
        "ctf:events",
        {
            "type": "task_created",
            "task_id": result["id"],
            "category": result["category"],
            "points": str(result["points"]),
        },
        maxlen=10000,
        approximate=True,
    )
    return result


async def get_task(database_url: str, task_id: UUID) -> dict[str, Any] | None:
    def fetch() -> dict[str, Any] | None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, title, category, points, status, round,
                           created_at, updated_at
                    FROM tasks
                    WHERE id = %s
                    """,
                    (task_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        columns = [
            "id", "title", "category", "points", "status", "round",
            "created_at", "updated_at",
        ]
        result = dict(zip(columns, row))
        result["id"] = str(result["id"])
        result["created_at"] = result["created_at"].isoformat()
        result["updated_at"] = result["updated_at"].isoformat()
        return result

    import asyncio

    return await asyncio.to_thread(fetch)
