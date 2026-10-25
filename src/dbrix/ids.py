"""Agent, session, and sandbox ids (§4.4)."""

import re

from dbrix.config import ID_PATTERN


class InvalidSandboxIdError(ValueError):
    pass


def sandbox_id(agent_id: str, session_id: str) -> str:
    """`agent_{agent_id}_{session_id}`. Raises InvalidSandboxIdError unless both ids match ID_PATTERN."""
    for name, value in [("agent_id", agent_id), ("session_id", session_id)]:
        if not re.fullmatch(ID_PATTERN, value):
            raise InvalidSandboxIdError(f"{name} {value!r} must match {ID_PATTERN}")
    return f"agent_{agent_id}_{session_id}"
