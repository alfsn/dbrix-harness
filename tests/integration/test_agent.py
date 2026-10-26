"""A Deep Agent end to end on real executors, with a scripted model (§11.2). The sandbox starts as a copy of the
suite's one real build; the rebuild after GC is a second real build."""

import shutil
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import docker
import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command

from dbrix.config import OperatingMode
from dbrix.deepagents import create_sandboxed_deep_agent
from dbrix.executor.client import remove_executors
from dbrix.lifecycle import gc
from dbrix.lifecycle.meta import read_meta, write_meta
from dbrix.middleware import RECREATED
from dbrix.sampler import SampleResult
from tests.scripted import ScriptedModel, call

from .conftest import CONFIG

pytestmark = pytest.mark.docker

THREAD = {"configurable": {"thread_id": "e2e"}}
SID = "agent_agent_e2e"


@pytest.fixture
def root(
    client: docker.DockerClient, prod_small: Path, sandbox_small: tuple[Path, SampleResult], tmp_path: Path
) -> Iterator[Path]:
    d = Path(shutil.copytree(sandbox_small[0], tmp_path / SID))
    write_meta(d, read_meta(d).model_copy(update={"sandbox_id": SID, "session_id": "e2e"}))
    yield tmp_path
    remove_executors(client, SID)


def messages(state: dict[str, Any]) -> dict[str, ToolMessage]:
    return {str(m.tool_call_id): m for m in state["messages"] if isinstance(m, ToolMessage)}


def test_deep_agent_end_to_end(client: docker.DockerClient, prod_small: Path, root: Path) -> None:
    script = [
        call("read_file", {"file_path": "/catalog/silver/brands.md"}, "doc"),
        call("run_sql", {"query": "SELECT COUNT(*) AS n FROM prod_catalog.silver.brands"}, "count"),
        call("read_file", {"file_path": "/results/count.csv"}, "csv"),
        call("run_pyspark", {"code": "result = spark.table('prod_catalog.silver.posts').limit(3)"}, "py"),
        call("run_sql", {"query": "CREATE TABLE prod_catalog.gold.my_summary AS SELECT 1 AS x"}, "create"),
        call("run_sql", {"query": "SELECT x FROM prod_catalog.gold.my_summary"}, "readback"),
        call("task", {"subagent_type": "explorer", "description": "Drop my_summary."}, "explore"),
        call("run_sql", {"query": "DROP TABLE prod_catalog.gold.my_summary"}, "drop"),  # the explorer's call
        AIMessage("The drop was blocked."),  # the explorer's answer
        call("write_file", {"file_path": "/catalog/x.md", "content": "hi"}, "write"),
        AIMessage("done"),
        call("run_sql", {"query": "SELECT x FROM prod_catalog.gold.my_summary"}, "after_gc"),
        AIMessage("done again"),
    ]
    cfg = CONFIG.model_copy(update={"sandbox_root": root, "prod_catalog_dir": prod_small})
    agent = create_sandboxed_deep_agent(
        ScriptedModel(responses=script), mode=OperatingMode.READ_WRITE, config=cfg, docker=client
    )

    state = agent.invoke({"messages": [{"role": "user", "content": "go"}]}, THREAD)
    [pending] = state["__interrupt__"]  # the CREATE TABLE waits for approval
    assert "CREATE TABLE prod_catalog.gold.my_summary" in pending.value["action_requests"][0]["description"]
    state = agent.invoke(Command(resume={"decisions": [{"type": "approve"}]}), THREAD)
    m = messages(state)

    assert "# prod_catalog.silver.brands" in str(m["doc"].content) and "## Sample rows" in str(m["doc"].content)
    assert m["count"].status == "success" and "/results/count.csv" in str(m["count"].content)
    assert "n" in str(m["csv"].content).splitlines()[0]
    assert "3 rows total" in str(m["py"].content)
    assert m["create"].status == "success"
    assert m["readback"].status == "success" and "| 1 |" in str(m["readback"].content)
    assert "blocked" in str(m["explore"].content)  # the explorer is READ_ONLY while the parent is READ_WRITE
    assert "permission denied" in str(m["write"].content)
    assert not (root / SID / "_catalog" / "x.md").exists()

    # The run log has one line per call that reached the sandbox, the write with its approval.
    runs = (root / SID / "runs.jsonl").read_text().splitlines()
    assert len(runs) == 5 and '"decision":"approve"' in runs[2] and '"POLICY_BLOCKED"' in runs[4]

    # GC removes the sandbox and its container; the next call rebuilds it and tells the agent.
    later = datetime.now(UTC) + timedelta(seconds=cfg.lifecycle.ttl_seconds + 1)
    report = gc.run(root, cfg.lifecycle.ttl_seconds, cfg.executor.run_timeout, client, now=later)
    assert report.removed == [SID] and not client.containers.list(
        all=True, filters={"label": f"dbrix.sandbox_id={SID}"}
    )
    state = agent.invoke({"messages": [{"role": "user", "content": "again"}]}, THREAD)
    after = messages(state)["after_gc"]
    assert str(after.content).startswith(RECREATED) and after.status == "error"  # my_summary is gone
    assert "`prod_catalog`.`gold`.`my_summary`" in str(after.content) and "/sandbox" not in str(after.content)
