"""The MCP server over stdio, as Claude Code starts it (§8.5, §11.2). A real sandbox build: the server makes its
session id at startup, so it can't start from the suite's copy."""

import asyncio
import sys
from pathlib import Path

import docker
import pytest
from mcp import Client, StdioServerParameters
from mcp_types import TextContent

from dbrix.executor.client import remove_executors

from .conftest import CONFIG, REPO

pytestmark = pytest.mark.docker


def test_stdio_server(client: docker.DockerClient, prod_small: Path, tmp_path: Path) -> None:
    sandboxes = tmp_path / "sandboxes"
    (tmp_path / "dbrix.toml").write_text(
        f'prod_catalog_dir = "{prod_small}"\nsandbox_root = "{sandboxes}"\nmanifest = "{REPO / CONFIG.manifest}"\n'
    )
    server = StdioServerParameters(command=sys.executable, args=["-m", "dbrix.mcp_server"], cwd=str(tmp_path))

    async def steps() -> tuple[str, str, bool]:
        async with Client(server, read_timeout_seconds=600) as c:
            readme = await c.read_resource("dbrix://catalog/README.md")  # builds the sandbox
            result = await c.call_tool("run_sql", {"query": "SELECT count(*) AS n FROM prod_catalog.silver.brands"})
            block = result.content[0]
            assert isinstance(block, TextContent)
            return readme.contents[0].text, block.text, bool(result.is_error)  # type: ignore[union-attr]

    try:
        readme, answer, error = asyncio.run(steps())
    finally:
        for sandbox in sandboxes.glob("agent_mcp_*"):
            remove_executors(client, sandbox.name)
    assert readme.startswith("# prod_catalog")
    assert not error and "| n |" in answer, answer
