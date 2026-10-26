"""The executor's runtime (§6.2–§6.4): runs agent code on one long-lived SparkSession, one NDJSON request at a time.
Runs inside the executor container only."""

import ast
import csv
import io
import os
import sys
import time
import traceback
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbrix.config import OUTPUT_LIMIT, RESULTS_DIR, RESULTS_PATH, SANDBOX_MOUNT
from dbrix.executor.protocol import AgentError, Column, RunRequest, RunResult
from dbrix.executor.runtime.wrap import RunState, install
from dbrix.policy import SandboxSecurityException
from dbrix.policy.sql import split_statements
from dbrix.rewrite import reverse_map
from dbrix.spark import create_session

AGENT_FILE = "<agent>"  # the file name agent code is compiled with, so tracebacks show only its frames

# Patches Spark for agent code and returns the per-run state. The host picks it at startup: the red-team
# containment test runs without the wrapper (§11.3).
Install = Callable[[SparkSession, str], RunState]


def run_sql(spark: SparkSession, code: str) -> DataFrame | None:
    """Each statement through `spark.sql`; the result is the last one's DataFrame."""
    df = None
    for statement in split_statements(code):
        df = spark.sql(statement)
    return df


def run_pyspark(spark: SparkSession, code: str) -> DataFrame | None:
    """The code with `spark`, `F`, `T` defined. The result is `result`, else the last expression (§6.2)."""
    tree = ast.parse(code)
    last = tree.body[-1] if tree.body else None
    if isinstance(last, ast.Expr):
        tree.body.pop()
    names: dict[str, Any] = {"spark": spark, "F": F, "T": T}
    exec(compile(tree, AGENT_FILE, "exec"), names)  # noqa: S102 - running agent code is this module's job
    value = eval(compile(ast.Expression(last.value), AGENT_FILE, "eval"), names) if isinstance(last, ast.Expr) else None
    result = names.get("result", value)
    return result if isinstance(result, DataFrame) else None


RUNNERS = {"sql": run_sql, "pyspark": run_pyspark}


def _cell(value: Any) -> str:
    return "null" if value is None else str(value).replace("|", "\\|").replace("\n", " ")


def save_result(df: DataFrame, path: Path, preview_rows: int, max_rows: int) -> tuple[str, int | None, bool]:
    """Write up to `max_rows` rows as CSV (§6.4). Returns the markdown preview, the row count (None when cut off),
    and whether rows were cut off. Collects once: `max_rows + 1` rows tell whether there are more."""
    flat = df.select(
        [
            F.to_json(f.name).alias(f.name)
            if isinstance(f.dataType, (T.StructType, T.ArrayType, T.MapType))
            else F.base64(f.name).alias(f.name)
            if isinstance(f.dataType, T.BinaryType)
            else F.col(f.name)
            for f in df.schema.fields
        ]
    )
    rows = flat.limit(max_rows + 1).collect()
    truncated = len(rows) > max_rows
    rows = rows[:max_rows]
    path.parent.mkdir(exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows(rows)
    lines = ["| " + " | ".join(df.columns) + " |", "|" + " --- |" * len(df.columns)]
    lines += ["| " + " | ".join(map(_cell, row)) + " |" for row in rows[:preview_rows]]
    return "\n".join(lines), None if truncated else len(rows), truncated


def _error(e: Exception, catalog: str) -> AgentError:
    if isinstance(e, SandboxSecurityException):  # the class name is not shown (§6.5)
        return AgentError(type="SecurityException", message=e.message, hint=e.hint, rule_id=e.rule_id)
    frames = [f for f in traceback.extract_tb(e.__traceback__) if f.filename == AGENT_FILE]
    where = "".join(f"line {f.lineno}: " for f in frames[-1:])
    return AgentError(type=type(e).__name__, message=reverse_map(f"{where}{e}", catalog))


def run(
    spark: SparkSession, state: RunState, request: RunRequest, catalog: str, preview_rows: int, max_rows: int
) -> RunResult:
    start = time.monotonic()
    state.allow_writes, state.rewritten_sql, state.writes_applied = request.allow_writes, [], []
    out, err = io.StringIO(), io.StringIO()
    result: dict[str, Any] = {}
    try:
        with redirect_stdout(out), redirect_stderr(err):
            df = RUNNERS[request.language](spark, request.code)
        if df is not None and df.columns:
            path = Path(SANDBOX_MOUNT, RESULTS_DIR, f"{request.run_id}.csv")
            preview, row_count, truncated = save_result(df, path, preview_rows, max_rows)
            result = {
                "preview_markdown": preview,
                "row_count": row_count,
                "truncated": truncated,
                "columns": [Column(name=f.name, type=f.dataType.simpleString()) for f in df.schema.fields],
                "full_result_path": f"{RESULTS_PATH}/{request.run_id}.csv",
            }
    except Exception as e:  # noqa: BLE001 - every agent error goes back as feedback (§8.2)
        result = {"error": _error(e, catalog)}
    return RunResult(
        ok="error" not in result,
        stdout=reverse_map(out.getvalue()[:OUTPUT_LIMIT], catalog),
        stderr=reverse_map(err.getvalue()[:OUTPUT_LIMIT], catalog),
        duration_ms=int((time.monotonic() - start) * 1000),
        rewritten_sql=state.rewritten_sql,
        writes_applied=state.writes_applied,
        **result,
    )


def serve(catalog: str, preview_rows: int, max_rows: int, install: Install = install) -> None:
    """Answer one RunResult line per RunRequest line until stdin closes."""
    # Before Spark starts: the protocol gets a private copy of fd 1, and fd 1 goes to stderr (§6.3).
    protocol = os.fdopen(os.dup(1), "w")
    os.dup2(2, 1)
    spark = create_session(SANDBOX_MOUNT)
    state = install(spark, catalog)
    protocol.write('{"ready": true}\n')
    protocol.flush()
    for line in sys.stdin:
        result = run(spark, state, RunRequest.model_validate_json(line), catalog, preview_rows, max_rows)
        protocol.write(result.model_dump_json() + "\n")
        protocol.flush()
