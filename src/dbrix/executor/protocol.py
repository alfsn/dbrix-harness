"""One NDJSON request and its response between the host and the executor (§6.3, §6.4). Shared by both sides."""

from pydantic import BaseModel, Field

from dbrix.config import RUN_ID_PATTERN
from dbrix.policy.report import Language, WriteOp


class RunRequest(BaseModel):
    run_id: str = Field(pattern=f"^{RUN_ID_PATTERN}$")
    language: Language
    code: str
    allow_writes: bool


class Column(BaseModel):
    name: str
    type: str  # Spark's simple type string, e.g. "bigint", "array<string>"


class AgentError(BaseModel):
    type: str  # "SecurityException" for policy errors (§6.5), else the exception class name
    message: str  # reverse-mapped
    hint: str | None = None
    rule_id: str | None = None


class RunResult(BaseModel):
    ok: bool
    preview_markdown: str | None = None  # first `preview_rows` rows
    row_count: int | None = None  # None when more than `result_file_max_rows`
    columns: list[Column] | None = None
    full_result_path: str | None = None  # "/results/<run_id>.csv" (agent-visible path)
    stdout: str = ""  # truncated to 8 KB
    stderr: str = ""  # Python stderr, truncated to 8 KB
    truncated: bool = False  # true when rows were cut off
    duration_ms: int
    rewritten_sql: list[str] = []  # for runs.jsonl only, never shown to the agent
    writes_applied: list[WriteOp] = []
    error: AgentError | None = None
