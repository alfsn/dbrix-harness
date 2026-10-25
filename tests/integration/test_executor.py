import csv
from collections.abc import Iterator
from pathlib import Path

import docker
import pytest

from dbrix.config import RESULTS_DIR
from dbrix.executor.client import Executor, run, start_executor
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.policy.report import Language

from .conftest import CONFIG

pytestmark = pytest.mark.docker


@pytest.fixture(scope="module")
def sandbox(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("sandbox")


@pytest.fixture(scope="module")
def executor(client: docker.DockerClient, sandbox: Path) -> Iterator[Executor]:
    e = start_executor(client, CONFIG.executor.image, sandbox, "agent_a_s1", "prod_catalog", CONFIG.executor)
    yield e
    e.container.remove(force=True)


def call(executor: Executor, language: Language, code: str, run_id: str = "r1") -> RunResult:
    return run(executor, RunRequest(run_id=run_id, language=language, code=code, allow_writes=False), 60)


def test_run_sql_writes_csv(executor: Executor, sandbox: Path) -> None:
    r = call(executor, "sql", "SELECT 2 AS y; SELECT 1 AS x, array(1, 2) AS a, CAST('ab' AS BINARY) AS b", "sql1")
    assert r.ok, r.error
    assert [(c.name, c.type) for c in r.columns or []] == [("x", "int"), ("a", "array<int>"), ("b", "binary")]
    assert (r.row_count, r.truncated, r.full_result_path) == (1, False, "/results/sql1.csv")
    assert r.preview_markdown == "| x | a | b |\n| --- | --- | --- |\n| 1 | [1,2] | YWI= |"
    rows = list(csv.reader((sandbox / RESULTS_DIR / "sql1.csv").open()))
    assert rows == [["x", "a", "b"], ["1", "[1,2]", "YWI="]]


def test_run_pyspark_last_expression_and_stdout(executor: Executor) -> None:
    r = call(executor, "pyspark", 'print("hello")\nspark.range(5)')
    assert (r.ok, r.row_count, r.stdout) == (True, 5, "hello\n")


def test_run_pyspark_result_variable(executor: Executor) -> None:
    r = call(executor, "pyspark", "result = spark.range(3)\nx = 1\nx")
    assert r.row_count == 3


def test_run_pyspark_without_dataframe(executor: Executor) -> None:
    r = call(executor, "pyspark", "x = 1")
    assert (r.ok, r.columns, r.full_result_path) == (True, None, None)


def test_truncated(executor: Executor, sandbox: Path) -> None:
    n = CONFIG.executor.result_file_max_rows
    r = call(executor, "pyspark", f"spark.range({n + 1})", "big")
    assert (r.row_count, r.truncated) == (None, True)
    assert len((sandbox / RESULTS_DIR / "big.csv").read_text().splitlines()) == n + 1  # header + n rows


def test_agent_output_never_breaks_protocol(executor: Executor) -> None:
    """Prints and raw writes to fd 1 do not reach the protocol channel (§6.3)."""
    r = call(executor, "pyspark", 'import os\nos.write(1, b"garbage\\n")\nprint(\'{"ok": false}\')\nspark.range(1)')
    assert (r.ok, r.stdout) == (True, '{"ok": false}\n')
    assert call(executor, "sql", "SELECT 1").ok


def test_errors_are_results(executor: Executor) -> None:
    r = call(executor, "pyspark", "x = 1\n1 / 0")
    assert not r.ok and r.error
    assert (r.error.type, r.error.message) == ("ZeroDivisionError", "line 2: division by zero")
    r = call(executor, "sql", "SELECT * FROM silver.nope")
    assert r.error and r.error.type == "AnalysisException"
    assert "/sandbox" not in r.error.message
