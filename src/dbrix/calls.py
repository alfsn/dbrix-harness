"""answer (§1 steps 3–8): one run_sql / run_pyspark call, for every front door (§8.2, §8.5). No agent framework:
approval is a callback. Policy and runtime errors come back as error text; host configuration errors are raised."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dbrix.config import ExecutorConfig, OperatingMode
from dbrix.executor.client import ExecutorError
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.jobs import JobError
from dbrix.lifecycle.manager import ExecutorBusy, OnBuild, Sandbox, SandboxManager
from dbrix.lifecycle.meta import now
from dbrix.lifecycle.runlog import Approval, Outcome, RunRecord
from dbrix.policy import SandboxSecurityException, WriteOp, check
from dbrix.policy.report import Language
from dbrix.policy.sql import split_statements

RECREATED = (
    "Note: your sandbox expired and was recreated from the same starting data. "
    "Tables you created or changed earlier are gone."
)
READ_ONLY_RULE = "READ_ONLY_WRITE"
DECISIONS = ["approve", "edit", "reject"]

# Takes the approval request (Deep Agents' HITL shape, §8.3), returns {"decisions": [decision]}.
Approve = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class ToolSpec:
    language: Language
    code_arg: str  # the tool argument that holds the code


DEFAULT_TOOLS = {"run_sql": ToolSpec("sql", "query"), "run_pyspark": ToolSpec("pyspark", "code")}


@dataclass(frozen=True)
class Answer:
    text: str
    error: bool
    created_at: str | None  # the sandbox's, for the recreation notice; None when the call failed before running


def answer(
    manager: SandboxManager,
    tool: str,
    call_id: str,
    code: str,
    spec: ToolSpec,
    mode: OperatingMode,
    agent_id: str,
    session_id: str,
    known_created_at: str | None,
    on_build: OnBuild,
    approve: Approve | None,
) -> Answer:
    """`known_created_at` is the sandbox the caller saw last; a different one adds the recreation notice (§8.2).
    Before `approve` only get_or_create and the pure policy check run, so a step resumed after a LangGraph interrupt
    is safe to repeat (§8.3)."""
    try:
        sandbox = manager.get_or_create(agent_id, session_id, on_build)
    except JobError:
        return Answer("The sandbox could not be created; try again.", True, None)

    def log(outcome: Outcome, approval: Approval | None, rule_id: str | None, message: str | None) -> None:
        """A call that did not run (§9.3)."""
        record = RunRecord(
            run_id=call_id,
            started_at=now(),
            duration_ms=0,
            tool=tool,
            language=spec.language,
            mode=mode,
            code=code,
            outcome=outcome,
            rule_id=rule_id,
            error_message=message,
            approval=approval,
        )
        manager.log_run(sandbox, record)

    def reply(sandbox: Sandbox, text: str, error: bool) -> Answer:
        created = sandbox.created_at.isoformat()
        if known_created_at not in (None, created):
            text = f"{RECREATED}\n\n{text}"
        return Answer(text, error, created)

    approval: Approval | None = None
    while True:
        try:
            report = check(code, spec.language)
        except SandboxSecurityException as e:
            log("POLICY_BLOCKED", approval, e.rule_id, e.message)
            return reply(sandbox, _error_text("SecurityException", e.message, e.hint), True)
        if report.writes and mode == OperatingMode.READ_ONLY:
            message = "This session is read-only. These statements write: " + _statements(code, spec, report.writes)
            log("POLICY_BLOCKED", approval, READ_ONLY_RULE, message)
            hint = "Query without writing, or ask the user for write access."
            return reply(sandbox, _error_text("SecurityException", message, hint), True)
        if not (mode == OperatingMode.READ_WRITE and (report.writes or report.has_dynamic_sql)):
            break
        if approve is None:
            raise ValueError("READ_WRITE needs an approve callback")
        decision = approve(_approval_request(tool, spec, code, report.writes, report.has_dynamic_sql, sandbox))
        decision = decision["decisions"][0]
        approval = Approval(decision=decision["type"], at=now())
        if decision["type"] == "approve":
            break
        if decision["type"] == "reject":
            feedback = decision.get("message")
            log("REJECTED", approval, None, feedback)
            text = "The user rejected this call; nothing ran." + (f" Feedback: {feedback}" if feedback else "")
            return reply(sandbox, text, True)
        code = decision["edited_action"]["args"][spec.code_arg]  # edit: check again, ask again if it writes

    run = RunRequest(run_id=call_id, language=spec.language, code=code, allow_writes=approval is not None)
    try:
        sandbox, result = manager.run(sandbox, run, tool, mode, approval, on_build)
    except ExecutorBusy:
        return Answer("Executor busy, try again.", True, None)
    except (JobError, ExecutorError):
        return Answer("The sandbox could not be started; try again.", True, None)
    return reply(sandbox, _format(result, manager.cfg.executor), not result.ok)


def _format(r: RunResult, cfg: ExecutorConfig) -> str:
    """Preview, row summary, stdout, stderr, error (§6.4)."""
    parts = [r.preview_markdown] if r.preview_markdown else []
    if r.full_result_path:
        total = f"more than {cfg.result_file_max_rows} rows" if r.row_count is None else f"{r.row_count} rows"
        shown = cfg.preview_rows if r.row_count is None else min(cfg.preview_rows, r.row_count)
        parts.append(f"{total} total, showing {shown}; full result at {r.full_result_path}")
    if r.stdout:
        parts.append(f"stdout:\n{r.stdout}")
    if r.stderr:
        parts.append(f"stderr:\n{r.stderr}")
    if r.error:
        parts.append(_error_text(r.error.type, r.error.message, r.error.hint))
    return "\n\n".join(parts) or "OK (no result)"


def _error_text(error_type: str, message: str, hint: str | None) -> str:
    return f"Error ({error_type}): {message}" + (f"\nHint: {hint}" if hint else "")


def _statements(code: str, spec: ToolSpec, writes: list[WriteOp]) -> str:
    """Each write as the agent wrote it: its SQL statement or its Python line (§8.3)."""
    sql = split_statements(code) if spec.language == "sql" else []
    lines = code.splitlines()
    shown = []
    for w in writes:
        if w.location.statement is not None and w.location.statement < len(sql):
            shown.append(sql[w.location.statement])
        elif w.location.line:
            shown.append(lines[w.location.line - 1].strip())
        else:
            shown.append(f"{w.kind} {w.target or 'unknown'}")
    return "; ".join(dict.fromkeys(shown))


def _approval_request(
    tool: str, spec: ToolSpec, code: str, writes: list[WriteOp], dynamic: bool, sandbox: Sandbox
) -> dict[str, Any]:
    """The same shape as Deep Agents' HITL middleware (`HITLRequest`), so its clients handle ours unchanged
    (§8.3). Plain dicts: those types are TypedDicts, and this module imports no agent framework."""
    lines = [f"This call writes to sandbox {sandbox.id}."]
    if writes:
        lines.append("Writes: " + _statements(code, spec, writes))
    if dynamic:
        lines.append("Warning: some SQL or table names are built at runtime and may write too.")
    return {
        "action_requests": [{"name": tool, "args": {spec.code_arg: code}, "description": "\n".join(lines)}],
        "review_configs": [{"action_name": tool, "allowed_decisions": DECISIONS}],
    }
