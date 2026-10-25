import pytest

from dbrix.policy import SandboxSecurityException, check
from dbrix.policy.report import Language

from .cases import BLOCKED, DYNAMIC


@pytest.mark.parametrize(("language", "code"), BLOCKED)
def test_policy_blocks(language: Language, code: str) -> None:
    with pytest.raises(SandboxSecurityException):
        check(code, language)


@pytest.mark.parametrize("code", DYNAMIC)
def test_policy_flags_dynamic(code: str) -> None:
    assert check(code, "pyspark").has_dynamic_sql
