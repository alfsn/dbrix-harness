"""DbrixSandboxMiddleware (§8.2, §8.3): answers run_sql / run_pyspark calls from the sandbox through
dbrix.calls.answer. The real tool bodies never run. Approval is a LangGraph interrupt."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, NotRequired

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain_core.messages import ToolMessage
from langgraph.config import get_stream_writer
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command, interrupt

from dbrix.calls import DEFAULT_TOOLS, ToolSpec, answer
from dbrix.config import OperatingMode
from dbrix.lifecycle.manager import SandboxManager


def stream_build_event(sandbox_id: str) -> None:
    """A custom stream event while a sandbox is built (15 s to minutes), so UIs can show progress (§8.2)."""
    get_stream_writer()({"dbrix_sandbox": "creating", "sandbox_id": sandbox_id})


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
        """The ToolMessage, plus the new sandbox in state when it changed since the agent last saw it."""
        call = request.tool_call
        session_id = request.runtime.config.get("configurable", {}).get("thread_id")
        known = request.state.get("dbrix_sandbox_created_at")
        a = answer(
            self.manager,
            call["name"],
            str(call["id"]),
            call["args"][spec.code_arg],
            spec,
            self.mode,
            self.agent_id,
            str(session_id),
            known,
            stream_build_event,
            interrupt,
        )
        message = ToolMessage(
            a.text, tool_call_id=call["id"], name=call["name"], status="error" if a.error else "success"
        )
        if a.created_at in (None, known):
            return message
        return Command(update={"messages": [message], "dbrix_sandbox_created_at": a.created_at})
