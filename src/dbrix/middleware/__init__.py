"""DbrixSandboxMiddleware (§8.2, §8.3): answers run_sql / run_pyspark calls from the sandbox. The real tool bodies
never run. Policy and runtime errors go back to the agent as an error ToolMessage; host configuration errors are
raised."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, NotRequired

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.human_in_the_loop import ActionRequest, HITLRequest, ReviewConfig
from langchain_core.messages import ToolMessage
from langgraph.config import get_stream_writer
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command, interrupt

from dbrix.config import OperatingMode
from dbrix.executor.client import ExecutorError
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.ids import sandbox_id
from dbrix.jobs import JobError
from dbrix.lifecycle.manager import ExecutorBusy, Sandbox, SandboxManager
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


def stream_build_event(sandbox_id: str) -> None:
    """A custom stream event while a sandbox is built (15 s to minutes), so UIs can show progress (§8.2)."""
    get_stream_writer()({"dbrix_sandbox": "creating", "sandbox_id": sandbox_id})


@dataclass(frozen=True)
class ToolSpec:
    language: Language
    code_arg: str  # the tool argument that holds the code


DEFAULT_TOOLS = {"run_sql": ToolSpec("sql", "query"), "run_pyspark": ToolSpec("pyspark", "code")}


class SandboxState(AgentState[Any]):
    dbrix_sandbox_created_at: NotRequired[str]  # tells when the sandbox was rebuilt (§8.2)


Reply = ToolMessage | Command[Any]


class DbrixSandboxMiddleware(AgentMiddleware[SandboxState]):
    state_schema = SandboxState

    def __init__(
        self,
        manager: SandboxManager,
        tool_map: dict[str, ToolSpec] = DEFAULT_TOOLS,
        mode: OperatingMode = OperatingMode.READ_ONLY,
        agent_id: str = "agent",
    ) -> None:
        super().__init__()
        self.manager = manager
        self.tool_map = tool_map
        self.mode = mode
        self.agent_id = agent_id

    def wrap_tool_call(self, request: ToolCallRequest, handler: Callable[[ToolCallRequest], Reply]) -> Reply:
        spec = self.tool_map.get(request.tool_call["name"])
        return handler(request) if spec is None else self._handle(request, spec)

    async def awrap_tool_call(
        self, request: ToolCallRequest, handler: Callable[[ToolCallRequest], Awaitable[Reply]]
    ) -> Reply:
        spec = self.tool_map.get(request.tool_call["name"])
        if spec is None:
            return await handler(request)
        # The lock wait, Docker I/O, and the run block; the event loop must not (§8.2).
        return await asyncio.to_thread(self._handle, request, spec)

    def _handle(self, request: ToolCallRequest, spec: ToolSpec) -> Reply:
        """Before interrupt() only get_or_create and the pure policy check run, so a resumed step is safe to
        repeat (§8.3)."""
        call = request.tool_call
        session_id = request.runtime.config.get("configurable", {}).get("thread_id")
        sandbox_id(self.agent_id, str(session_id))  # raises InvalidSandboxIdError into the graph: a host error

        try:
            sandbox = self.manager.get_or_create(self.agent_id, str(session_id), stream_build_event)
        except JobError:
            return _message(request, "The sandbox could not be created; try again.", error=True)

        code: str = call["args"][spec.code_arg]
        approval: Approval | None = None
        while True:
            try:
                report = check(code, spec.language)
            except SandboxSecurityException as e:
                self._log(sandbox, request, spec, code, "POLICY_BLOCKED", approval, e.rule_id, e.message)
                return self._reply(request, sandbox, _error_text("SecurityException", e.message, e.hint), True)
            if report.writes and self.mode == OperatingMode.READ_ONLY:
                message = "This session is read-only. These statements write: " + _statements(code, spec, report.writes)
                self._log(sandbox, request, spec, code, "POLICY_BLOCKED", approval, READ_ONLY_RULE, message)
                hint = "Query without writing, or ask the user for write access."
                return self._reply(request, sandbox, _error_text("SecurityException", message, hint), True)
            needs_approval = self.mode == OperatingMode.READ_WRITE and (report.writes or report.has_dynamic_sql)
            if not needs_approval:
                break
            decision = interrupt(
                _approval_request(call["name"], spec, code, report.writes, report.has_dynamic_sql, sandbox)
            )
            decision = decision["decisions"][0]
            approval = Approval(decision=decision["type"], at=now())
            if decision["type"] == "approve":
                break
            if decision["type"] == "reject":
                feedback = decision.get("message")
                self._log(sandbox, request, spec, code, "REJECTED", approval, None, feedback)
                text = "The user rejected this call; nothing ran." + (f" Feedback: {feedback}" if feedback else "")
                return self._reply(request, sandbox, text, True)
            code = decision["edited_action"]["args"][spec.code_arg]  # edit: check again, ask again if it writes

        run = RunRequest(run_id=str(call["id"]), language=spec.language, code=code, allow_writes=approval is not None)
        try:
            sandbox, result = self.manager.run(sandbox, run, call["name"], self.mode, approval, stream_build_event)
        except ExecutorBusy:
            return _message(request, "Executor busy, try again.", error=True)
        except (JobError, ExecutorError):
            return _message(request, "The sandbox could not be started; try again.", error=True)
        return self._reply(request, sandbox, self._format(result), not result.ok)

    def _log(
        self,
        sandbox: Sandbox,
        request: ToolCallRequest,
        spec: ToolSpec,
        code: str,
        outcome: Outcome,
        approval: Approval | None,
        rule_id: str | None,
        error_message: str | None,
    ) -> None:
        """A call that did not run (§9.3)."""
        record = RunRecord(
            run_id=str(request.tool_call["id"]),
            started_at=now(),
            duration_ms=0,
            tool=request.tool_call["name"],
            language=spec.language,
            mode=self.mode,
            code=code,
            outcome=outcome,
            rule_id=rule_id,
            error_message=error_message,
            approval=approval,
        )
        self.manager.log_run(sandbox, record)

    def _reply(self, request: ToolCallRequest, sandbox: Sandbox, text: str, error: bool) -> Reply:
        """The ToolMessage, with the recreation notice when the sandbox was rebuilt since the agent last saw it."""
        created = sandbox.created_at.isoformat()
        known = request.state.get("dbrix_sandbox_created_at")
        if known not in (None, created):
            text = f"{RECREATED}\n\n{text}"
        message = _message(request, text, error)
        if known == created:
            return message
        return Command(update={"messages": [message], "dbrix_sandbox_created_at": created})

    def _format(self, r: RunResult) -> str:
        """Preview, row summary, stdout, stderr, error (§6.4)."""
        cfg = self.manager.cfg.executor
        parts = [r.preview_markdown] if r.preview_markdown else []
        if r.full_result_path:
            total = f"more than {cfg.result_file_max_rows} rows" if r.row_count is None else f"{r.row_count} rows"
            shown = cfg.preview_rows if r.row_count is None else min(cfg.preview_rows, r.row_count)
            parts.append(f"{total} total, showing {shown}; full result at {r.full_result_path} (read with read_file)")
        if r.stdout:
            parts.append(f"stdout:\n{r.stdout}")
        if r.stderr:
            parts.append(f"stderr:\n{r.stderr}")
        if r.error:
            parts.append(_error_text(r.error.type, r.error.message, r.error.hint))
        return "\n\n".join(parts) or "OK (no result)"


def _message(request: ToolCallRequest, text: str, error: bool) -> ToolMessage:
    return ToolMessage(
        text,
        tool_call_id=request.tool_call["id"],
        name=request.tool_call["name"],
        status="error" if error else "success",
    )


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
) -> HITLRequest:
    """The same shape as Deep Agents' HITL middleware, so its clients handle ours unchanged (§8.3)."""
    lines = [f"This call writes to sandbox {sandbox.id}."]
    if writes:
        lines.append("Writes: " + _statements(code, spec, writes))
    if dynamic:
        lines.append("Warning: some SQL or table names are built at runtime and may write too.")
    return HITLRequest(
        action_requests=[ActionRequest(name=tool, args={spec.code_arg: code}, description="\n".join(lines))],
        review_configs=[ReviewConfig(action_name=tool, allowed_decisions=DECISIONS)],  # type: ignore[typeddict-item]
    )
