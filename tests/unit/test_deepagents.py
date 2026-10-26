"""create_sandboxed_deep_agent without Docker: tool list, subagent enforcement, read-only sandbox files (§8.4)."""

from pathlib import Path
from typing import Any, ClassVar

import pytest
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, ToolMessage

from dbrix.config import CATALOG_DOCS_DIR, Config, OperatingMode
from dbrix.deepagents import MisconfiguredSandboxError, create_sandboxed_deep_agent, enforce
from dbrix.lifecycle.manager import Sandbox, SandboxManager
from dbrix.middleware import DbrixSandboxMiddleware
from dbrix.middleware.tools import run_sql
from tests.fakes import T0
from tests.scripted import ScriptedModel, call
from tests.unit.test_middleware import START, invoke


class RecordingModel(ScriptedModel):
    tool_names: ClassVar[list[list[str]]] = []

    def bind_tools(self, tools: Any, **kwargs: Any) -> "RecordingModel":
        RecordingModel.tool_names.append([t.name if hasattr(t, "name") else t["name"] for t in tools])
        return self


def build(*script: AIMessage, model: type[ScriptedModel] = ScriptedModel, **kwargs: Any) -> Any:
    return create_sandboxed_deep_agent(
        model(responses=[*script, AIMessage("done")]),
        agent_id="analyst",
        config=Config(),
        docker=object(),  # type: ignore[arg-type]
        **kwargs,
    )


def test_code_tools_are_ours_and_no_shell() -> None:
    RecordingModel.tool_names = []
    invoke(build(model=RecordingModel), START)
    [names] = RecordingModel.tool_names
    assert {"run_sql", "run_pyspark", "read_file", "task"} <= set(names)
    assert not {"execute", "eval"} & set(names)


def test_compiled_subagent_without_middleware_is_rejected() -> None:
    runnable = create_agent(ScriptedModel(responses=[AIMessage("x")]), [run_sql])
    with pytest.raises(MisconfiguredSandboxError, match="'rogue'"):
        build(subagents=[{"name": "rogue", "description": "d", "runnable": runnable}])


def test_compiled_subagent_without_code_tools_is_fine() -> None:
    runnable = create_agent(ScriptedModel(responses=[AIMessage("x")]), [])
    build(subagents=[{"name": "talker", "description": "d", "runnable": runnable}])


def test_enforce_on_specs() -> None:
    guard = DbrixSandboxMiddleware(None)  # type: ignore[arg-type]
    enforce([{"name": "a", "description": "d", "tools": [run_sql], "middleware": [guard]}])
    enforce([{"name": "b", "description": "d", "tools": []}])
    with pytest.raises(MisconfiguredSandboxError):
        enforce([{"name": "c", "description": "d", "tools": [run_sql]}])
    with pytest.raises(MisconfiguredSandboxError):  # no `tools`: it gets the main agent's
        enforce([{"name": "d", "description": "d"}])


def test_user_subagent_gets_the_middleware(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []
    monkeypatch.setattr("dbrix.deepagents.enforce", seen.extend)
    build(subagents=[{"name": "helper", "description": "d", "system_prompt": "p"}])
    by_name = {s["name"]: s for s in seen}
    assert set(by_name) == {"general-purpose", "explorer", "helper"}
    for name, mode in [("general-purpose", OperatingMode.READ_ONLY), ("explorer", OperatingMode.READ_ONLY)]:
        [guard] = by_name[name]["middleware"]
        assert guard.mode == mode
    assert isinstance(by_name["helper"]["middleware"][0], DbrixSandboxMiddleware)

    seen.clear()
    build(mode=OperatingMode.READ_WRITE)
    modes = {s["name"]: s["middleware"][0].mode for s in seen}
    assert modes == {"general-purpose": OperatingMode.READ_WRITE, "explorer": OperatingMode.READ_ONLY}


def test_catalog_is_readable_not_writable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox = tmp_path / "agent_analyst_t1"
    (sandbox / CATALOG_DOCS_DIR).mkdir(parents=True)
    (sandbox / CATALOG_DOCS_DIR / "README.md").write_text("# prod_catalog\n")
    sessions: list[str] = []

    def get_or_create(self: Any, agent_id: str, session_id: str, on_build: Any = None) -> Sandbox:
        sessions.append(session_id)
        return Sandbox(agent_id, session_id, sandbox, T0)

    monkeypatch.setattr(SandboxManager, "get_or_create", get_or_create)
    graph = build(
        call("read_file", {"file_path": "/catalog/README.md"}, "r1"),
        call("write_file", {"file_path": "/catalog/x.md", "content": "hi"}, "w1"),
    )
    read, write = [m for m in invoke(graph, START)["messages"] if isinstance(m, ToolMessage)]
    assert "# prod_catalog" in str(read.content)
    assert "permission denied" in str(write.content)
    assert not (sandbox / CATALOG_DOCS_DIR / "x.md").exists()
    assert sessions and set(sessions) == {"t1"}  # the thread picks the sandbox
