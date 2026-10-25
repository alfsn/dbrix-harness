import pytest
from pydantic import ValidationError

from dbrix.executor.protocol import RunRequest


@pytest.mark.parametrize("run_id", ["toolu_01ABC-x", "a" * 128])
def test_run_id_accepted(run_id: str) -> None:
    RunRequest(run_id=run_id, language="sql", code="SELECT 1", allow_writes=False)


@pytest.mark.parametrize("run_id", ["", "../x", "a/b", "a.csv", "a" * 129, "a\n"])
def test_run_id_rejected(run_id: str) -> None:
    """run_id names the result file, so no path characters (§6.3)."""
    with pytest.raises(ValidationError):
        RunRequest(run_id=run_id, language="sql", code="SELECT 1", allow_writes=False)
