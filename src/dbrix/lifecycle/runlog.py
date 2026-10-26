"""`runs.jsonl`: one line per run, append-only (§9.3)."""

import json
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from dbrix.config import RUNS_FILE, OperatingMode
from dbrix.policy.report import Language, WriteOp

Outcome = Literal["OK", "POLICY_BLOCKED", "RUNTIME_ERROR", "TIMEOUT", "REJECTED"]


class Approval(BaseModel):
    decision: Literal["approve", "edit", "reject"]
    by: str | None = None
    at: datetime


class RunRecord(BaseModel):
    run_id: str  # the tool_call_id
    started_at: datetime
    duration_ms: int
    tool: str
    language: Language
    mode: OperatingMode
    code: str
    rewritten_sql: list[str] = []
    outcome: Outcome
    rule_id: str | None = None
    error_message: str | None = None
    row_count: int | None = None
    writes: list[WriteOp] = []
    approval: Approval | None = None


def append_run(sandbox_dir: Path, record: RunRecord) -> bool:
    """Append `record` unless its run_id is already logged (a resumed step repeats, §8.3). True when written."""
    path = sandbox_dir / RUNS_FILE
    if path.exists():
        with path.open() as f:
            if any(json.loads(line)["run_id"] == record.run_id for line in f):
                return False
    with path.open("a") as f:
        f.write(record.model_dump_json() + "\n")
    return True
