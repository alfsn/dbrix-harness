"""The MCP server (§8.5): run_sql and run_pyspark through dbrix.calls.answer, READ_ONLY, one sandbox per process.
The catalog docs, results, and skills are resources, so the client needs nothing from dbrix."""

import hmac
import uuid
from pathlib import Path

import anyio.from_thread
import anyio.to_thread
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ResourceNotFoundError
from mcp_types import CallToolResult, TextContent
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from dbrix.calls import DEFAULT_TOOLS, answer
from dbrix.catalog_docs import agent_instructions
from dbrix.config import (
    CATALOG_DOCS_DIR,
    CATALOG_PATH,
    MCP_AGENT_ID,
    RESOURCE_SCHEME,
    RESULTS_DIR,
    RESULTS_PATH,
    SKILLS_DIR,
    SKILLS_PATH,
    McpConfig,
    OperatingMode,
)
from dbrix.lifecycle.manager import SandboxManager
from dbrix.manifest import load_manifest
from dbrix.middleware.tools import run_pyspark, run_sql

MODE = OperatingMode.READ_ONLY


def uri(agent_path: str) -> str:
    """The resource URI of an agent-visible path: `/catalog/README.md` -> `dbrix://catalog/README.md`."""
    return f"{RESOURCE_SCHEME}:/{agent_path}"


def create_server(manager: SandboxManager) -> MCPServer:
    session_id = uuid.uuid4().hex
    created_at: str | None = None  # the sandbox the client saw last, for the recreation notice

    async def run(tool: str, code: str, ctx: Context) -> CallToolResult:
        nonlocal created_at

        def on_build(sandbox_id: str) -> None:
            anyio.from_thread.run(ctx.report_progress, 0, None, f"Building sandbox {sandbox_id} (up to a few minutes)")

        a = await anyio.to_thread.run_sync(
            answer,
            manager,
            tool,
            uuid.uuid4().hex,
            code,
            DEFAULT_TOOLS[tool],
            MODE,
            MCP_AGENT_ID,
            session_id,
            created_at,
            on_build,
            None,
        )
        created_at = a.created_at or created_at
        return CallToolResult(content=[TextContent(type="text", text=a.text)], is_error=a.error)

    async def sql(query: str, ctx: Context) -> CallToolResult:
        return await run(run_sql.name, query, ctx)

    async def pyspark(code: str, ctx: Context) -> CallToolResult:
        return await run(run_pyspark.name, code, ctx)

    instructions = agent_instructions(load_manifest(manager.cfg.manifest), MODE) + (
        f"\nFiles under `{CATALOG_PATH}`, `{RESULTS_PATH}`, and `{SKILLS_PATH}` are MCP resources: "
        f"`{CATALOG_PATH}/README.md` is `{uri(CATALOG_PATH)}/README.md`.\n"
    )
    server = MCPServer("dbrix", instructions=instructions)
    server.add_tool(sql, name=run_sql.name, description=run_sql.description)
    server.add_tool(pyspark, name=run_pyspark.name, description=run_pyspark.description)

    def sandbox_file(subdir: str, path: str) -> str:
        """Reading builds the sandbox when it doesn't exist yet, as in the Deep Agent (§8.4)."""
        return _read(manager.get_or_create(MCP_AGENT_ID, session_id).dir / subdir, path)

    @server.resource(uri(f"{CATALOG_PATH}/{{+path}}"), name="catalog", mime_type="text/markdown")
    def catalog(path: str) -> str:
        return sandbox_file(CATALOG_DOCS_DIR, path)

    @server.resource(uri(f"{RESULTS_PATH}/{{+path}}"), name="results", mime_type="text/csv")
    def results(path: str) -> str:
        return sandbox_file(RESULTS_DIR, path)

    @server.resource(uri(f"{SKILLS_PATH}/{{+path}}"), name="skills", mime_type="text/markdown")
    def skills(path: str) -> str:
        return _read(SKILLS_DIR, path)

    return server


def _read(root: Path, path: str) -> str:
    """A file under `root`. The SDK already rejects `..` and absolute paths; the untrusted executor can leave
    symlinks in `_results/`, so the resolved path must stay under `root` too (§8.5)."""
    file = (root / path).resolve()
    if not file.is_relative_to(root.resolve()):
        raise ResourceError(f"{path} is outside {root.name}")
    if not file.is_file():
        raise ResourceNotFoundError(f"no file {path}")
    return file.read_text()


def http_app(server: MCPServer, cfg: McpConfig) -> ASGIApp:
    """Streamable HTTP at /mcp. Every request needs `Authorization: Bearer <token>` (§8.5)."""
    if not cfg.token:
        raise ValueError("set [mcp] token in dbrix.toml to serve over HTTP")
    app = server.streamable_http_app(host=cfg.host)
    expected = f"Bearer {cfg.token}".encode()

    async def guarded(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and not hmac.compare_digest(
            dict(scope["headers"]).get(b"authorization", b""), expected
        ):
            await PlainTextResponse("Unauthorized", status_code=401)(scope, receive, send)
            return
        await app(scope, receive, send)

    return guarded
