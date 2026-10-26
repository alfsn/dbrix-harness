"""create_sandboxed_deep_agent (§8.4): a Deep Agent whose only code tools are run_sql and run_pyspark, answered by
DbrixSandboxMiddleware on the main agent and on every subagent."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from deepagents import CompiledSubAgent, FilesystemPermission, SubAgent, create_deep_agent
from deepagents.backends import BackendProtocol, CompositeBackend, FilesystemBackend, StateBackend
from deepagents.backends.protocol import FileDownloadResponse, GlobResult, GrepResult, LsResult, ReadResult
from deepagents.middleware.subagents import GENERAL_PURPOSE_SUBAGENT
from docker import DockerClient, from_env
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_config
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Checkpointer

from dbrix.catalog_docs import agent_instructions
from dbrix.config import (
    CATALOG_DOCS_DIR,
    CATALOG_PATH,
    RESULTS_DIR,
    RESULTS_PATH,
    Config,
    OperatingMode,
    SamplingConfig,
    load_config,
)
from dbrix.lifecycle.manager import SandboxManager
from dbrix.manifest import load_manifest
from dbrix.middleware import DEFAULT_TOOLS, DbrixSandboxMiddleware, stream_build_event
from dbrix.middleware.tools import run_pyspark, run_sql

SKILLS_DIR = Path(__file__).parent / "skills"
SKILLS_PATH = "/skills"

EXPLORER: SubAgent = {
    "name": "explorer",
    "description": "Profiles tables (row counts, value ranges, nulls, join keys) and returns findings. Read-only.",
    "system_prompt": (
        "Profile the tables you are asked about with run_sql or run_pyspark and report concise findings: row "
        "counts, key columns, value ranges, null rates, and how tables join. You cannot write data."
    ),
}


class MisconfiguredSandboxError(Exception):
    pass


class SandboxFiles(BackendProtocol):
    """Read-only files of the current thread's sandbox (`_results/` or `_catalog/`). The agent is built once and
    serves every thread, so the sandbox is looked up on each call from the LangGraph thread, as StateBackend does.
    Reading builds the sandbox when it doesn't exist yet. Write methods are not implemented, so writes fail."""

    def __init__(self, manager: SandboxManager, agent_id: str, subdir: str) -> None:
        self.manager, self.agent_id, self.subdir = manager, agent_id, subdir

    def _files(self) -> FilesystemBackend:
        session_id = str(get_config().get("configurable", {}).get("thread_id"))
        sandbox = self.manager.get_or_create(self.agent_id, session_id, stream_build_event)
        return FilesystemBackend(root_dir=sandbox.dir / self.subdir, virtual_mode=True)

    def ls(self, path: str) -> LsResult:
        return self._files().ls(path)

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return self._files().read(file_path, offset, limit)

    def grep(
        self, pattern: str, path: str | None = None, glob: str | None = None, max_count: int | None = None
    ) -> GrepResult:
        return self._files().grep(pattern, path, glob, max_count=max_count)

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        return self._files().glob(pattern, path)

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        return self._files().download_files(paths)


def create_sandboxed_deep_agent(
    model: str | BaseChatModel,
    mode: OperatingMode | None = None,
    agent_id: str = "agent",
    sampling: SamplingConfig | None = None,
    subagents: Sequence[SubAgent | CompiledSubAgent] = (),
    extra_tools: Sequence[BaseTool] = (),
    config: Config | None = None,
    docker: DockerClient | None = None,
    checkpointer: Checkpointer | None = None,
) -> CompiledStateGraph[Any, Any, Any, Any]:
    """`config` defaults to dbrix.toml; `sampling` and `mode` override it. `extra_tools` must not run code."""
    cfg = config or load_config()
    if sampling:
        cfg = cfg.model_copy(update={"sampling": sampling})
    mode = mode or cfg.default_mode
    manager = SandboxManager(cfg, docker or from_env())
    guard = DbrixSandboxMiddleware(manager, mode=mode, agent_id=agent_id)
    read_only = DbrixSandboxMiddleware(manager, mode=OperatingMode.READ_ONLY, agent_id=agent_id)
    tools = [run_sql, run_pyspark, *extra_tools]
    specs: list[Any] = [  # SubAgent and CompiledSubAgent dicts
        # Ours replaces Deep Agents' default general-purpose subagent, which would not get the middleware.
        {**GENERAL_PURPOSE_SUBAGENT, "tools": tools, "middleware": [guard]},
        {**EXPLORER, "tools": [run_sql, run_pyspark], "middleware": [read_only]},
        *(_with_guard(s, guard) for s in subagents),
    ]
    enforce(specs)
    files = {
        f"{CATALOG_PATH}/": SandboxFiles(manager, agent_id, CATALOG_DOCS_DIR),
        f"{RESULTS_PATH}/": SandboxFiles(manager, agent_id, RESULTS_DIR),
        f"{SKILLS_PATH}/": FilesystemBackend(root_dir=SKILLS_DIR, virtual_mode=True),
    }
    return create_deep_agent(
        model=model,
        tools=tools,
        system_prompt=agent_instructions(load_manifest(cfg.manifest), mode),
        middleware=[guard],
        subagents=specs,
        skills=[f"{SKILLS_PATH}/"],
        permissions=[FilesystemPermission(operations=["write"], paths=[f"{p}**" for p in files], mode="deny")],
        backend=CompositeBackend(default=StateBackend(), routes=files),
        checkpointer=checkpointer or InMemorySaver(),
    )


def _with_guard(spec: Mapping[str, Any], guard: DbrixSandboxMiddleware) -> Mapping[str, Any]:
    """A spec with the middleware added, unless it has one. A compiled subagent can't be changed; enforce checks it."""
    if "runnable" in spec or any(isinstance(m, DbrixSandboxMiddleware) for m in spec.get("middleware", [])):
        return spec
    return {**spec, "middleware": [*spec.get("middleware", []), guard]}


def enforce(specs: Sequence[Mapping[str, Any]]) -> None:
    """Every subagent that can call a code tool has the middleware (§8.4). A spec without `tools` gets the main
    agent's tools. A compiled subagent's tools are read from its graph."""
    for spec in specs:
        if "runnable" in spec:
            tools_node = getattr(spec["runnable"], "nodes", {}).get("tools")
            names = set(getattr(getattr(tools_node, "bound", None), "tools_by_name", {}))
            guarded = False
        else:
            names = {getattr(t, "name", None) for t in spec["tools"]} if "tools" in spec else set(DEFAULT_TOOLS)
            guarded = any(isinstance(m, DbrixSandboxMiddleware) for m in spec.get("middleware", []))
        if names & set(DEFAULT_TOOLS) and not guarded:
            raise MisconfiguredSandboxError(
                f"subagent {spec['name']!r} can call {sorted(names & set(DEFAULT_TOOLS))} without "
                "DbrixSandboxMiddleware; pass it as a SubAgent spec so the middleware is added"
            )
