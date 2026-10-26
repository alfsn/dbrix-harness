"""A SandboxManager stand-in for the front-door tests: no Docker (§8.2, §8.5)."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dbrix.config import Config
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.ids import sandbox_id
from dbrix.lifecycle.manager import Sandbox
from dbrix.lifecycle.runlog import Approval, RunRecord

T0 = datetime(2026, 10, 26, tzinfo=UTC)


class FakeManager:
    def __init__(self, root: Path = Path("/x")) -> None:
        self.root = root
        self.cfg = Config()
        self.created_at = T0
        self.runs: list[tuple[RunRequest, Approval | None]] = []
        self.logged: list[RunRecord] = []
        self.builds = 0
        self.sessions: list[str] = []

    def sandbox(self, agent_id: str, session_id: str) -> Sandbox:
        return Sandbox(agent_id, session_id, self.root / sandbox_id(agent_id, session_id), self.created_at)

    def get_or_create(self, agent_id: str, session_id: str, on_build: Any = None) -> Sandbox:
        self.sessions.append(session_id)
        if self.builds == 0 and on_build:
            on_build(f"agent_{agent_id}_{session_id}")
        self.builds += 1
        return self.sandbox(agent_id, session_id)

    def run(self, sandbox: Sandbox, request: RunRequest, tool: str, mode: Any, approval: Any, on_build: Any) -> Any:
        self.runs.append((request, approval))
        result = RunResult(
            ok=True,
            preview_markdown="| x |\n| --- |\n| 1 |",
            row_count=1,
            full_result_path=f"/results/{request.run_id}.csv",
            duration_ms=1,
        )
        return self.sandbox(sandbox.agent_id, sandbox.session_id), result

    def log_run(self, sandbox: Sandbox, record: RunRecord) -> None:
        self.logged.append(record)
