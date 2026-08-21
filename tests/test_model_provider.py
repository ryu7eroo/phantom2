import asyncio

from ctf_swarm.agent import AgentContext
from ctf_swarm.model_provider import MockModelProvider
from ctf_swarm.models import Challenge, Round
from ctf_swarm.tool_gateway import SandboxToolGateway


def test_mock_provider_returns_response():
    async def run():
        provider = MockModelProvider("hello")
        ctx = AgentContext(
            challenge=Challenge("x", "x", "misc"),
            round=Round.INDEPENDENT,
            dead_ends=frozenset(),
            explored_paths=frozenset(),
            open_hypotheses=frozenset(),
            evidence=(),
        )
        result = await provider.generate(ctx, "solve")
        assert result.text == "hello"
        assert result.metadata["round"] == "independent"

    asyncio.run(run())


def test_tool_gateway_disabled_by_default():
    async def run():
        gateway = SandboxToolGateway()
        result = await gateway.execute("strings", ["challenge.bin"])
        assert result.ok is False
        assert "disabled" in result.stderr

    asyncio.run(run())


def test_tool_gateway_rejects_unknown_tool():
    async def run():
        gateway = SandboxToolGateway(enabled=True)
        result = await gateway.execute("bash", ["-lc", "id"])
        assert result.ok is False
        assert "not allowed" in result.stderr

    asyncio.run(run())
