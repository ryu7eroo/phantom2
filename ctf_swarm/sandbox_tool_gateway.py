from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ToolResult:
    ok: bool
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


class DockerSandboxToolGateway:
    """Run unrestricted CTF tooling inside an isolated per-task container.

    The model can execute arbitrary shell commands, install packages, clone repos,
    compile code, and access the network from inside the sandbox. The Docker socket,
    host filesystem, and privileged mode are intentionally not exposed to it.
    """

    def __init__(
        self,
        *,
        image: str = "ctf-swarm-sandbox:latest",
        root: str | os.PathLike[str] = "/tmp/ctf-sandboxes",
        enabled: bool = True,
    ) -> None:
        self.image = image
        self.root = Path(root)
        self.enabled = enabled
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    @staticmethod
    def _safe_task_id(task_id: str) -> str:
        value = re.sub(r"[^a-zA-Z0-9_.-]", "-", task_id)
        return value[:80] or "task"

    def _container_name(self, task_id: str) -> str:
        return f"ctf-sandbox-{self._safe_task_id(task_id)}"

    async def _docker(self, *args: str, timeout: float = 60.0) -> ToolResult:
        proc = await asyncio.create_subprocess_exec(
            "docker", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return ToolResult(False, stderr="docker command timeout", returncode=124)
        return ToolResult(
            proc.returncode == 0,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
            proc.returncode or 0,
        )

    async def _ensure_image(self) -> ToolResult:
        result = await self._docker("image", "inspect", self.image)
        if result.ok:
            return result
        dockerfile = Path(__file__).resolve().parent.parent / "Dockerfile.sandbox"
        return await self._docker("build", "-t", self.image, "-f", str(dockerfile), str(dockerfile.parent), timeout=600)

    async def _ensure_container(self, task_id: str) -> ToolResult:
        name = self._container_name(task_id)
        inspect = await self._docker("inspect", "-f", "{{.State.Running}}", name)
        if inspect.ok and inspect.stdout.strip() == "true":
            return ToolResult(True)
        if inspect.ok:
            started = await self._docker("start", name)
            if started.ok:
                return started
        image = await self._ensure_image()
        if not image.ok:
            return image
        workspace = self.root / self._safe_task_id(task_id)
        workspace.mkdir(parents=True, exist_ok=True)
        return await self._docker(
            "run", "-d",
            "--name", name,
            "--network", "bridge",
            "--workdir", "/workspace",
            "--cpus", os.getenv("CTF_SANDBOX_CPUS", "4"),
            "--memory", os.getenv("CTF_SANDBOX_MEMORY", "8g"),
            "--pids-limit", os.getenv("CTF_SANDBOX_PIDS", "512"),
            "-v", f"{workspace}:/workspace",
            self.image,
        )

    async def execute(
        self,
        task_id: str,
        command: str,
        *,
        timeout: float = 120.0,
    ) -> ToolResult:
        if not self.enabled:
            return ToolResult(False, stderr="sandbox tool execution disabled")
        if not command.strip():
            return ToolResult(False, stderr="empty command", returncode=2)

        async with self._lock:
            ready = await self._ensure_container(task_id)
        if not ready.ok:
            return ready

        name = self._container_name(task_id)
        return await self._docker(
            "exec", name,
            "bash", "-lc", command,
            timeout=timeout,
        )

    async def cleanup(self, task_id: str, *, remove_workspace: bool = False) -> ToolResult:
        name = self._container_name(task_id)
        result = await self._docker("rm", "-f", name)
        if remove_workspace:
            workspace = self.root / self._safe_task_id(task_id)
            try:
                for path in workspace.iterdir():
                    if path.is_dir():
                        import shutil
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                workspace.rmdir()
            except FileNotFoundError:
                pass
        return result
