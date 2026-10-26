"""The error for code blocked in every mode (§5.1)."""

from dbrix.policy.report import Language, Location


class SandboxSecurityException(Exception):
    def __init__(
        self,
        rule_id: str,
        message: str,
        language: Language,
        location: Location | None = None,
        snippet: str | None = None,
        hint: str | None = None,
    ) -> None:
        super().__init__(f"{rule_id}: {message}" + (f" (hint: {hint})" if hint else ""))
        self.rule_id = rule_id
        self.message = message
        self.language = language
        self.location = location
        self.snippet = snippet
        self.hint = hint
