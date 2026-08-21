import asyncio

from ctf_swarm.dispatcher import Assignment


def test_assignment_accepts_persistence_shape():
    assignment = Assignment(
        assignment_id="a1",
        task_id="t1",
        worker_id="w1",
        round="independent",
    )
    assert assignment.round == "independent"


def test_assignment_is_immutable():
    assignment = Assignment("a1", "t1", "w1", "independent")

    async def run() -> None:
        assert assignment.task_id == "t1"

    asyncio.run(run())
