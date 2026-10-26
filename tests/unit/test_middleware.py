"""DbrixSandboxMiddleware on plain create_agent, with a scripted model and a fake manager: no Docker (§8.2, §8.3)."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from dbrix.config import Config, OperatingMode
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.ids import InvalidSandboxIdError
from dbrix.lifecycle.manager import Sandbox
from dbrix.lifecycle.runlog import Approval, RunRecord
from dbrix.middleware import RECREATED, DbrixSandboxMiddleware
from dbrix.middleware.tools import NotSandboxedError, run_pyspark, run_sql
from tests.scripted import ScriptedModel, call

T0 = datetime(2026, 10, 26, tzinfo=UTC)


class FakeManager:
    def __init__(self) -> None:
        self.cfg = Config()
        self.created_at = T0
        self.runs: list[tuple[RunRequest, Approval | None]] = []
        self.logged: list[RunRecord] = []
        self.builds = 0

    def sandbox(self, agent_id: str, session_id: str) -> Sandbox:
        return Sandbox(agent_id, session_id, Path(f"/x/agent_{agent_id}_{session_id}"), self.created_at)

    def get_or_create(self, agent_id: str, session_id: str, on_build: Any) -> Sandbox:
        if self.builds == 0:
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


@tool
def other_tool() -> str:
    """Not a code tool."""
    return "other ran"


def agent(manager: FakeManager, mode: OperatingMode, *script: AIMessage) -> Any:
    middleware = DbrixSandboxMiddleware(manager, mode=mode, agent_id="analyst")  # type: ignore[arg-type]
    model = ScriptedModel(responses=[*script, AIMessage("done")])
    return create_agent(
        model, [run_sql, run_pyspark, other_tool], middleware=[middleware], checkpointer=InMemorySaver()
    )


def invoke(graph: Any, value: Any, thread: str = "t1") -> dict[str, Any]:
    result: dict[str, Any] = graph.invoke(value, {"configurable": {"thread_id": thread}})
    return result


def tool_messages(state: dict[str, Any]) -> list[ToolMessage]:
    return [m for m in state["messages"] if isinstance(m, ToolMessage)]


START = {"messages": [{"role": "user", "content": "go"}]}


def test_read_runs_and_formats_the_result() -> None:
    m = FakeManager()
    state = invoke(agent(m, OperatingMode.READ_ONLY, call("run_sql", {"query": "SELECT 1"})), START)
    [msg] = tool_messages(state)
    assert msg.status == "success"
    assert (
        msg.content
        == "| x |\n| --- |\n| 1 |\n\n1 rows total, showing 1; full result at /results/c1.csv (read with read_file)"
    )
    [(request, approval)] = m.runs
    assert (request.run_id, request.allow_writes, approval) == ("c1", False, None)
    assert state["dbrix_sandbox_created_at"] == T0.isoformat()


def test_other_tools_pass_through() -> None:
    m = FakeManager()
    state = invoke(agent(m, OperatingMode.READ_ONLY, call("other_tool", {})), START)
    assert tool_messages(state)[0].content == "other ran" and m.builds == 0


def test_policy_error_is_feedback_and_logged() -> None:
    m = FakeManager()
    state = invoke(agent(m, OperatingMode.READ_WRITE, call("run_pyspark", {"code": "import os"})), START)
    [msg] = tool_messages(state)
    assert msg.status == "error" and "SecurityException" in str(msg.content) and not m.runs
    assert [r.outcome for r in m.logged] == ["POLICY_BLOCKED"]


def test_read_only_blocks_writes_with_the_statement() -> None:
    m = FakeManager()
    sql = "SELECT 1; DROP TABLE prod_catalog.gold.campaign_performance"
    state = invoke(agent(m, OperatingMode.READ_ONLY, call("run_sql", {"query": sql})), START)
    [msg] = tool_messages(state)
    assert msg.status == "error" and "DROP TABLE prod_catalog.gold.campaign_performance" in str(msg.content)
    assert not m.runs and m.logged[0].rule_id == "READ_ONLY_WRITE"


WRITE = "CREATE TABLE prod_catalog.gold.my_summary AS SELECT 1 AS x"


def test_write_asks_then_approve_runs_once() -> None:
    m = FakeManager()
    graph = agent(m, OperatingMode.READ_WRITE, call("run_sql", {"query": WRITE}))
    state = invoke(graph, START)
    [pending] = state["__interrupt__"]
    [action] = pending.value["action_requests"]
    assert action["name"] == "run_sql" and action["args"] == {"query": WRITE}
    assert WRITE in action["description"] and "agent_analyst_t1" in action["description"]
    assert pending.value["review_configs"][0]["allowed_decisions"] == ["approve", "edit", "reject"]
    assert not m.runs and not m.logged  # nothing is written before the interrupt

    state = invoke(graph, Command(resume={"decisions": [{"type": "approve"}]}))
    [(request, approval)] = m.runs
    assert request.allow_writes and approval and approval.decision == "approve"
    assert tool_messages(state)[0].status == "success"


def test_reject_runs_nothing_and_returns_feedback() -> None:
    m = FakeManager()
    graph = agent(m, OperatingMode.READ_WRITE, call("run_sql", {"query": WRITE}))
    invoke(graph, START)
    state = invoke(graph, Command(resume={"decisions": [{"type": "reject", "message": "not now"}]}))
    [msg] = tool_messages(state)
    assert msg.status == "error" and "not now" in str(msg.content) and not m.runs
    assert [r.outcome for r in m.logged] == ["REJECTED"]


def test_edit_goes_through_the_policy_again() -> None:
    m = FakeManager()
    graph = agent(m, OperatingMode.READ_WRITE, call("run_sql", {"query": WRITE}))
    invoke(graph, START)
    edited = "INSERT INTO prod_catalog.gold.my_summary SELECT 2"
    edit = {"type": "edit", "edited_action": {"name": "run_sql", "args": {"query": edited}}}
    state = invoke(graph, Command(resume={"decisions": [edit]}))
    assert state["__interrupt__"][0].value["action_requests"][0]["args"] == {"query": edited}  # still writes: ask
    invoke(graph, Command(resume={"decisions": [{"type": "approve"}]}))
    [(request, _)] = m.runs
    assert request.code == edited and request.allow_writes


def test_edit_to_blocked_code_is_feedback() -> None:
    m = FakeManager()
    graph = agent(m, OperatingMode.READ_WRITE, call("run_sql", {"query": WRITE}))
    invoke(graph, START)
    edit = {"type": "edit", "edited_action": {"name": "run_sql", "args": {"query": "ADD JAR '/x.jar'"}}}
    state = invoke(graph, Command(resume={"decisions": [edit]}))
    assert tool_messages(state)[0].status == "error" and not m.runs


def test_recreation_notice_after_a_rebuild() -> None:
    """Same thread: the notice once, then the state knows the new sandbox (§8.2)."""
    m = FakeManager()
    graph = agent(
        m,
        OperatingMode.READ_ONLY,
        call("run_sql", {"query": "SELECT 1"}, "c1"),
        AIMessage("first answer"),
        call("run_sql", {"query": "SELECT 2"}, "c2"),
    )
    invoke(graph, START)
    m.created_at = T0.replace(day=27)  # GC removed the sandbox; it was rebuilt
    state = invoke(graph, {"messages": [{"role": "user", "content": "again"}]})
    first, second = tool_messages(state)
    assert not str(first.content).startswith(RECREATED) and str(second.content).startswith(RECREATED)
    assert state["dbrix_sandbox_created_at"] == m.created_at.isoformat()


def test_build_streams_a_progress_event() -> None:
    graph = agent(FakeManager(), OperatingMode.READ_ONLY, call("run_sql", {"query": "SELECT 1"}))
    events = list(graph.stream(START, {"configurable": {"thread_id": "t1"}}, stream_mode="custom"))
    assert events == [{"dbrix_sandbox": "creating", "sandbox_id": "agent_analyst_t1"}]


def test_bad_thread_id_is_raised() -> None:
    graph = agent(FakeManager(), OperatingMode.READ_ONLY, call("run_sql", {"query": "SELECT 1"}))
    with pytest.raises(InvalidSandboxIdError):
        invoke(graph, START, thread="a_b")


def test_async_call_and_interrupt() -> None:
    m = FakeManager()
    graph = agent(m, OperatingMode.READ_WRITE, call("run_sql", {"query": WRITE}))
    config = {"configurable": {"thread_id": "t1"}}
    state = asyncio.run(graph.ainvoke(START, config))
    assert state["__interrupt__"]
    asyncio.run(graph.ainvoke(Command(resume={"decisions": [{"type": "approve"}]}), config))
    assert m.runs[0][0].allow_writes


def test_stubs_fail_closed() -> None:
    with pytest.raises(NotSandboxedError):
        run_sql.invoke({"query": "SELECT 1"})
    with pytest.raises(NotSandboxedError):
        run_pyspark.invoke({"code": "1"})
