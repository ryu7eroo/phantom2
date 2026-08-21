import pytest

from ctf_swarm.model_provider import ModelResponse
from ctf_swarm.tool_loop import parse_tool_call, solve_with_tools
from ctf_swarm.tool_gateway import ToolResult


def test_parse_tool_call_json():
    call = parse_tool_call('before\nTOOL_CALL {"command":"python3 -c \\\"print(7)\\\""}\nafter')
    assert call == {"command": 'python3 -c "print(7)"'}


def test_parse_tool_call_requires_command():
    assert parse_tool_call('TOOL_CALL {"foo":"bar"}') is None


class FakeProvider:
    async def generate(self, context, prompt, *, system=""):
        if "TOOL RESULT" not in prompt:
            return ModelResponse(text='TOOL_CALL {"command":"printf\\n LISA{tool-win}\\n"}')
        return ModelResponse(text="Tool result proves LISA{tool-win}")


class FakeGateway:
    async def execute(self, task_id, command, *, timeout=120.0):
        assert task_id == "task"
        assert "printf" in command
        return ToolResult(True, stdout="LISA{tool-win}\n")


class Context:
    class Challenge:
        id = "task"

    challenge = Challenge()


@pytest.mark.asyncio
async def test_solve_with_tools_replays_tool_result():
    response, trace = await solve_with_tools(FakeProvider(), Context(), "solve", FakeGateway(), max_steps=2)
    assert "LISA{tool-win}" in response.text
    assert trace[0]["tool_call"]["command"].startswith("printf")
    assert trace[0]["tool_result"]["stdout"] == "LISA{tool-win}\n"
