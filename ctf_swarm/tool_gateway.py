from __future__ import annotations

import asyncio
import shlex
from dataclasses import dataclass


@dataclass(frozen=True)
class ToolResult:
    ok: bool
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


class SandboxToolGateway:
    """Explicit tool boundary. Host execution is disabled by default."""

    ALLOWED = frozenset({"python", "strings", "file", "xxd"})

    def __init__(self, *, enabled: bool = False) -> None:
        self.enabled = enabled

    async def execute(self, tool: str, args: list[str], *, timeout: float = 10.0) -> ToolResult:
        if not self.enabled:
            return ToolResult(False, stderr="tool execution disabled")
        if tool not in self.ALLOWED:
            return ToolResult(False, stderr=f"tool not allowed: {tool}")
        cmd = [tool, *args]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return ToolResult(
                proc.returncode == 0,
                stdout=stdout.decode(errors="replace"),
                stderr=stderr.decode(errors="replace"),
                returncode=proc.returncode or 0,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return ToolResult(False, stderr="tool timeout", returncode=124)
        except OSError as exc:
            return ToolResult(False, stderr=str(exc), returncode=127)
