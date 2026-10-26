import csv
import shutil
from collections.abc import Iterator
from pathlib import Path

import docker
import pytest

from dbrix.config import RESULTS_DIR
from dbrix.executor.client import Executor, ExecutorTimeout, restart_executor, run, start_executor
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.policy.report import Language
from dbrix.sampler import SampleResult

from .conftest import CONFIG

pytestmark = pytest.mark.docker


@pytest.fixture(scope="module")
def sandbox(sandbox_small: tuple[Path, SampleResult], tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A copy of the shared sandbox: these tests create and drop tables."""
    return Path(shutil.copytree(sandbox_small[0], tmp_path_factory.mktemp("executor") / "agent_agent_s1"))


@pytest.fixture(scope="module")
def executor(client: docker.DockerClient, sandbox: Path) -> Iterator[Executor]:
    e = start_executor(client, CONFIG.executor.image, sandbox, "agent_agent_s1", "prod_catalog", CONFIG.executor)
    yield e
    e.container.remove(force=True)


def call(
    executor: Executor,
    language: Language,
    code: str,
    run_id: str = "r1",
    allow_writes: bool = False,
    timeout: float = CONFIG.executor.run_timeout,
) -> RunResult:
    request = RunRequest(run_id=run_id, language=language, code=code, allow_writes=allow_writes)
    return run(executor, request, timeout)


def blocked_rule(r: RunResult) -> str | None:
    assert not r.ok and r.error and r.error.type == "SecurityException", r
    return r.error.rule_id


# The runtime (§6.2–§6.4)


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


@pytest.mark.parametrize(
    ("language", "code"),
    [
        ("sql", "SELECT no_such_column FROM prod_catalog.silver.brands"),
        ("pyspark", 'spark.table("prod_catalog.silver.brands").select("no_such_column")'),
    ],
)
def test_spark_errors_log_nothing(executor: Executor, language: Language, code: str) -> None:
    """PySpark's own error log (JVM stack trace, container paths) does not reach stderr (§6.5)."""
    r = call(executor, language, code)
    assert not r.ok and r.error and "no_such_column" in r.error.message
    assert r.stderr == "", r.stderr


# The wrapper (§6.1)


def test_prod_names_read_the_sandbox(executor: Executor, sandbox_small: tuple[Path, SampleResult]) -> None:
    brands = sandbox_small[1].row_counts["silver.brands"]
    r = call(executor, "sql", "SELECT count(*) AS n FROM prod_catalog.silver.brands")
    assert r.preview_markdown and r.preview_markdown.endswith(f"| {brands} |")
    assert r.rewritten_sql == ["SELECT COUNT(*) AS n FROM silver.brands"]
    code = (
        'a = spark.table("prod_catalog.silver.brands")\n'
        'b = spark.read.table("prod_catalog.silver.brands").where("brand_id IS NOT NULL")\n'
        "a.unionByName(b).selectExpr('count(*) AS n')"
    )
    r = call(executor, "pyspark", code)
    assert r.ok, r.error
    assert r.preview_markdown and r.preview_markdown.endswith(f"| {2 * brands} |")


@pytest.mark.parametrize(
    ("language", "code", "rule_id"),
    [
        ("sql", "SELECT * FROM silver.brands", "TABLE_NAME_NOT_ALLOWED"),
        ("sql", "CREATE TABLE prod_catalog.gold.t AS SELECT 1 AS x", "WRITE_NOT_ALLOWED"),
        ("pyspark", 't = "prod_catalog.gold." + "t"\nspark.sql(f"DROP TABLE {t}")', "WRITE_NOT_ALLOWED"),
        ("pyspark", 't = "prod_catalog.gold." + "t"\nspark.range(1).write.saveAsTable(t)', "WRITE_NOT_ALLOWED"),
        ("pyspark", 'spark.sql("SELECT {x}", x=1)', "SQL_ARGS_NOT_ALLOWED"),
        ("pyspark", 'spark.sql("SELECT 1; SELECT 2")', "SQL_MULTIPLE_STATEMENTS"),
        (
            "pyspark",
            "f = \"refl\" + \"ect('java.lang.Runtime', 'getRuntime')\"\nspark.range(1).select(F.expr(f))",
            "SQL_FUNCTION_BLOCKED",
        ),
        ("pyspark", 'c = "input_file" + "_name()"\nspark.range(1).where(c)', "SQL_FUNCTION_BLOCKED"),
    ],
)
def test_blocked_at_runtime(executor: Executor, language: Language, code: str, rule_id: str) -> None:
    """Names, writes, and SQL built at runtime are checked when they run (§5.2)."""
    assert blocked_rule(call(executor, language, code)) == rule_id


def test_path_methods_blocked_even_with_writes(executor: Executor) -> None:
    """An alias hides the chain from the static check (§5.5); the runtime still blocks it."""
    r = call(executor, "pyspark", 'w = spark.range(1).write\nw.save("/sandbox/x")', allow_writes=True)
    assert blocked_rule(r) == "PY_PATH_ACCESS"


def test_agent_tables(executor: Executor) -> None:
    r = call(executor, "sql", "CREATE TABLE prod_catalog.gold.my_summary AS SELECT 1 AS x", allow_writes=True)
    assert r.ok, r.error
    assert [(w.kind, w.target) for w in r.writes_applied] == [("CREATE_TABLE", "prod_catalog.gold.my_summary")]
    r = call(executor, "pyspark", 'spark.table("prod_catalog.gold.my_summary")')
    assert r.row_count == 1
    r = call(executor, "sql", "DESCRIBE DETAIL prod_catalog.gold.my_summary")
    assert r.preview_markdown and "| delta |" in r.preview_markdown  # Delta by default, as on Databricks
    code = 'spark.range(2).write.mode("append").insertInto("prod_catalog.gold.my_summary")'
    r = call(executor, "pyspark", code, allow_writes=True)
    assert [(w.kind, w.target) for w in r.writes_applied] == [("INSERT", "prod_catalog.gold.my_summary")]
    assert call(executor, "sql", "SELECT * FROM prod_catalog.gold.my_summary").row_count == 3
    assert call(executor, "sql", "DROP TABLE prod_catalog.gold.my_summary", allow_writes=True).ok
    r = call(executor, "sql", "SELECT * FROM prod_catalog.gold.my_summary")
    assert r.error and "`prod_catalog`.`gold`.`my_summary`" in r.error.message


def test_temp_views_across_calls(executor: Executor) -> None:
    assert call(executor, "sql", "CREATE OR REPLACE TEMP VIEW v AS SELECT * FROM prod_catalog.silver.brands").ok
    assert call(executor, "sql", "SELECT * FROM v").ok
    assert call(executor, "pyspark", 'spark.table("v")').ok
    assert call(executor, "sql", "DROP VIEW v").ok


# Timeout and restart (§6.3)


def test_timeout_restarts_and_tables_survive(client: docker.DockerClient, executor: Executor) -> None:
    for sql in [
        "CREATE TABLE prod_catalog.gold.kept AS SELECT 1 AS x",
        "CREATE TABLE prod_catalog.gold.dropped AS SELECT 1 AS x",
        "DROP TABLE prod_catalog.gold.dropped",
    ]:
        assert call(executor, "sql", sql, allow_writes=True).ok
    assert call(executor, "sql", "CREATE TEMP VIEW tv AS SELECT 1 AS x").ok
    with pytest.raises(ExecutorTimeout):
        call(executor, "pyspark", "import time\ntime.sleep(600)", timeout=2)
    restart_executor(client, executor)
    assert call(executor, "sql", "SELECT * FROM prod_catalog.gold.kept").row_count == 1
    r = call(executor, "sql", "SELECT * FROM prod_catalog.gold.dropped")
    assert r.error and "`prod_catalog`.`gold`.`dropped`" in r.error.message
    assert blocked_rule(call(executor, "sql", "SELECT * FROM tv")) == "TABLE_NAME_NOT_ALLOWED"  # temp views are lost


# Keeping the illusion (§6.5)


@pytest.mark.parametrize(
    ("language", "code", "expected"),
    [
        ("sql", "SHOW CATALOGS", "prod_catalog"),
        ("sql", "SHOW TABLES IN prod_catalog.silver", "| silver | brands |"),
        ("sql", "SHOW SCHEMAS IN prod_catalog", "| silver |"),
        ("sql", "DESCRIBE DETAIL prod_catalog.silver.brands", "prod_catalog.silver.brands"),
        ("sql", "DESCRIBE EXTENDED prod_catalog.silver.brands", "prod_catalog.silver.brands"),
        ("sql", "DESCRIBE HISTORY prod_catalog.silver.brands", "CREATE TABLE AS SELECT"),
        ("sql", "EXPLAIN EXTENDED SELECT * FROM prod_catalog.silver.brands", "prod_catalog.silver.brands"),
        ("sql", "SELECT current_catalog() AS c", "| prod_catalog |"),
        ("sql", "SELECT * FROM prod_catalog.silver.no_such_table", "`prod_catalog`.`silver`.`no_such_table`"),
        ("pyspark", 'spark.table("prod_catalog.silver.brands").explain(True)', "prod_catalog.silver.brands"),
        ("pyspark", 'spark.sql("DESCRIBE DETAIL prod_catalog.silver.brands")', "prod_catalog.silver.brands"),
    ],
)
def test_illusion(executor: Executor, sandbox: Path, language: Language, code: str, expected: str) -> None:
    r = call(executor, language, code, "illusion")
    csv_text = (sandbox / RESULTS_DIR / "illusion.csv").read_text() if r.full_result_path else ""
    text = "\n".join([r.preview_markdown or "", csv_text, r.stdout, r.stderr, r.error.message if r.error else ""])
    assert "/sandbox" not in text and "spark_catalog" not in text, text
    assert expected in text, text
