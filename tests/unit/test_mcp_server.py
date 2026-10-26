"""The MCP server with an in-process client and a fake manager: no Docker (§8.5). The call pipeline itself is
covered through the middleware (test_middleware.py); these tests cover what the MCP front door adds."""

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from mcp import Client
from mcp.shared.exceptions import MCPError
from mcp_types import CallToolResult, TextContent
from starlette.testclient import TestClient

from dbrix.calls import RECREATED
from dbrix.config import McpConfig
from dbrix.mcp_server import create_server, http_app
from dbrix.middleware.tools import run_pyspark, run_sql
from tests.fakes import FakeManager

WRITE = "CREATE TABLE prod_catalog.gold.t AS SELECT 1"


def connect(manager: FakeManager, steps: Callable[[Client], Awaitable[Any]]) -> Any:
    async def go() -> Any:
        async with Client(create_server(manager)) as client:  # type: ignore[arg-type]
            return await steps(client)

    return asyncio.run(go())


def text(result: CallToolResult) -> str:
    block = result.content[0]
    assert isinstance(block, TextContent)
    return block.text


def test_tools_are_the_stubs() -> None:
    tools = connect(FakeManager(), lambda c: c.list_tools())
    found = {t.name: t for t in tools.tools}
    assert found.keys() == {"run_sql", "run_pyspark"}
    assert found["run_sql"].description == run_sql.description
    assert found["run_pyspark"].description == run_pyspark.description
    assert set(found["run_sql"].input_schema["properties"]) == {"query"}
    assert set(found["run_pyspark"].input_schema["properties"]) == {"code"}


def test_read_returns_the_preview() -> None:
    m = FakeManager()
    result = connect(m, lambda c: c.call_tool("run_sql", {"query": "SELECT 1"}))
    assert not result.is_error
    assert text(result).startswith("| x |\n| --- |\n| 1 |\n\n1 rows total, showing 1; full result at /results/")
    assert not m.runs[0][0].allow_writes


def test_write_is_read_only() -> None:
    m = FakeManager()
    result = connect(m, lambda c: c.call_tool("run_sql", {"query": WRITE}))
    assert result.is_error and "read-only" in text(result) and WRITE in text(result)
    assert not m.runs and m.logged[0].outcome == "POLICY_BLOCKED"


def test_recreation_notice_after_a_rebuild() -> None:
    m = FakeManager()

    async def steps(c: Client) -> list[str]:
        first = await c.call_tool("run_sql", {"query": "SELECT 1"})
        m.created_at = m.created_at.replace(day=27)  # GC removed the sandbox; it was rebuilt
        second = await c.call_tool("run_sql", {"query": "SELECT 2"})
        third = await c.call_tool("run_sql", {"query": "SELECT 3"})
        return [text(first), text(second), text(third)]

    first, second, third = connect(m, steps)
    assert not first.startswith(RECREATED) and second.startswith(RECREATED) and not third.startswith(RECREATED)


def test_build_reports_progress() -> None:
    messages: list[str | None] = []

    async def on_progress(progress: float, total: float | None, message: str | None) -> None:
        messages.append(message)

    connect(FakeManager(), lambda c: c.call_tool("run_sql", {"query": "SELECT 1"}, progress_callback=on_progress))
    assert len(messages) == 1 and str(messages[0]).startswith("Building sandbox agent_mcp_")


def test_one_sandbox_per_server() -> None:
    m = FakeManager()
    for _ in range(3):
        connect(m, lambda c: c.call_tool("run_sql", {"query": "SELECT 1"}))
    assert len(set(m.sessions)) == 3  # three servers, three sandboxes


def test_resources(tmp_path: Path) -> None:
    m = FakeManager(tmp_path)
    secret = tmp_path / "secret.txt"
    secret.write_text("host file")

    async def steps(c: Client) -> dict[str, Any]:
        await c.call_tool("run_sql", {"query": "SELECT 1"})  # learn the server's sandbox dir
        box = m.sandbox("mcp", m.sessions[0]).dir
        (box / "_catalog" / "silver").mkdir(parents=True)
        (box / "_catalog" / "silver" / "brands.md").write_text("# brands")
        (box / "_results").mkdir()
        (box / "_results" / "leak.csv").symlink_to(secret)  # left by hostile code in the executor
        out: dict[str, Any] = {
            "doc": await c.read_resource("dbrix://catalog/silver/brands.md"),
            "skill": await c.read_resource("dbrix://skills/databricks-analyst/SKILL.md"),
        }
        for name, uri in [
            ("symlink", "dbrix://results/leak.csv"),
            ("traversal", "dbrix://catalog/../../secret.txt"),
            ("missing", "dbrix://results/none.csv"),
        ]:
            try:
                await c.read_resource(uri)
                out[name] = "read"
            except MCPError as e:
                out[name] = e.error.message
        return out

    out = connect(m, steps)
    assert out["doc"].contents[0].text == "# brands"
    assert "databricks-analyst" in out["skill"].contents[0].text
    assert "outside" in out["symlink"]
    assert out["traversal"].startswith("Unknown resource") and out["missing"] == "no file none.csv"


def test_instructions_name_the_resources() -> None:
    server = create_server(FakeManager())  # type: ignore[arg-type]
    assert "read-only" in str(server.instructions) and "dbrix://catalog/README.md" in str(server.instructions)


def test_http_needs_the_token() -> None:
    app = http_app(create_server(FakeManager()), McpConfig(host="0.0.0.0", token="s3cret"))  # type: ignore[arg-type]
    with TestClient(app) as client:
        assert client.post("/mcp", json={}).status_code == 401
        assert client.post("/mcp", json={}, headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert client.post("/mcp", json={}, headers={"Authorization": "Bearer s3cret"}).status_code != 401


def test_http_refuses_to_start_without_a_token() -> None:
    with pytest.raises(ValueError, match="token"):
        http_app(create_server(FakeManager()), McpConfig())  # type: ignore[arg-type]
