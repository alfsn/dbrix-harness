"""A chat model that answers from a script, so agent tests need no API key (§11)."""

from typing import Any

from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage


class ScriptedModel(FakeMessagesListChatModel):
    """Returns `responses` in order, across the main agent and its subagents."""

    def bind_tools(self, tools: Any, **kwargs: Any) -> "ScriptedModel":
        return self


def call(name: str, args: dict[str, Any], call_id: str = "c1") -> AIMessage:
    return AIMessage("", tool_calls=[{"name": name, "args": args, "id": call_id}])
