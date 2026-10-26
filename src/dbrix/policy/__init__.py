"""Security & AST inspection (§5): what code reads and writes, and what is blocked in every mode."""

from collections.abc import Callable

from dbrix.policy.exceptions import SandboxSecurityException
from dbrix.policy.python import check_pyspark
from dbrix.policy.report import Language, Location, PolicyReport, TableRef, WriteOp
from dbrix.policy.sql import check_sql

__all__ = ["Location", "PolicyReport", "SandboxSecurityException", "TableRef", "WriteOp", "check"]

CHECKS: dict[Language, Callable[[str], PolicyReport]] = {"sql": check_sql, "pyspark": check_pyspark}


def check(code: str, language: Language) -> PolicyReport:
    """Mode-free. Raises SandboxSecurityException for code blocked in every mode."""
    return CHECKS[language](code)
