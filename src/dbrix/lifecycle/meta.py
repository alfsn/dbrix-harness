"""`_sandbox_meta.json` (§9.1, §9.2). Written atomically, while holding the sandbox lock."""

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from dbrix.config import SANDBOX_META_FILE
from dbrix.sampler import Sampling

Status = Literal["CREATING", "IDLE", "ACTIVE", "FAILED"]


class ActiveRun(BaseModel):
    run_id: str
    started_at: datetime


class SandboxMeta(BaseModel):
    spec_version: Literal[1] = 1
    sandbox_id: str
    agent_id: str
    session_id: str
    status: Status
    created_at: datetime
    last_used_at: datetime
    active_run: ActiveRun | None = None
    sampling: Sampling | None = None  # None only when creation failed before sampling finished
    row_counts: dict[str, int] = {}
    container_id: str | None = None
    run_count: int = 0
    last_error: str | None = None


def now() -> datetime:
    return datetime.now(UTC)


def read_meta(sandbox_dir: Path) -> SandboxMeta:
    return SandboxMeta.model_validate_json((sandbox_dir / SANDBOX_META_FILE).read_text())


def write_meta(sandbox_dir: Path, meta: SandboxMeta) -> None:
    path = sandbox_dir / SANDBOX_META_FILE
    tmp = path.with_suffix(".tmp")
    tmp.write_text(meta.model_dump_json(indent=2))
    os.replace(tmp, path)
