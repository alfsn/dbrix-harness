"""The runtime wrapper (§6.1): every SQL string and table name the agent uses is checked, rewritten to the sandbox,
and its writes allowed only when the run has `allow_writes`. Patches the classes at their lowest entry points, so
each name is handled once."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, NoReturn

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from sqlglot import exp

from dbrix.policy import SandboxSecurityException, check
from dbrix.policy.python import PATH_HINT, PATH_METHODS
from dbrix.policy.report import Location, WriteKind, WriteOp
from dbrix.policy.sql import check_expression, name_ref, parse_statement
from dbrix.rewrite import reverse_rules, rewrite_name, rewrite_sql


@dataclass
class RunState:
    """What one run may do and did. The runtime resets it for each request."""

    allow_writes: bool = False
    rewritten_sql: list[str] = field(default_factory=list)
    writes_applied: list[WriteOp] = field(default_factory=list)


def _blocked(rule_id: str, message: str, hint: str | None = None) -> NoReturn:
    raise SandboxSecurityException(rule_id, message, "pyspark", None, None, hint)


def _reverse_cells(df: DataFrame, catalog: str) -> DataFrame:
    """String cells of SHOW, DESCRIBE, and EXPLAIN results with the reverse mapping applied, in Spark (§6.5)."""

    def mapped(name: str) -> Any:
        column = df[name]
        for pattern, replacement in reverse_rules(catalog):
            column = F.regexp_replace(column, pattern, replacement)
        return column.alias(name)

    return df.select([mapped(f.name) if isinstance(f.dataType, T.StringType) else df[f.name] for f in df.schema.fields])


def _patch(owner: Any, name: str, wrapper: Callable[..., Any]) -> None:
    """Replace `owner.name` with a function that calls `wrapper(original, *args)`. No `functools.wraps`: it would
    expose the original as `__wrapped__`."""
    original = getattr(owner, name)
    setattr(owner, name, lambda *args, **kwargs: wrapper(original, *args, **kwargs))


def install(spark: SparkSession, catalog: str) -> RunState:
    """Patch Spark for agent code. Returns the state the runtime sets for each run."""
    state = RunState()

    def is_temp_view(name: str) -> bool:
        return any(t.isTemporary and t.name == name for t in spark.catalog.listTables())

    def sql(original: Any, self: SparkSession, sqlQuery: str, args: Any = None, **kwargs: Any) -> DataFrame:
        if args is not None or kwargs:
            _blocked("SQL_ARGS_NOT_ALLOWED", "spark.sql takes only the query string", "use one SQL string")
        writes = check(sqlQuery, "sql").writes
        if writes and not state.allow_writes:
            _blocked("WRITE_NOT_ALLOWED", "this statement writes, which is not allowed in this run")
        rewritten = rewrite_sql(sqlQuery, catalog, is_temp_view)
        state.rewritten_sql.append(rewritten)
        df: DataFrame = original(self, rewritten)
        state.writes_applied += writes
        is_metadata = isinstance(parse_statement(sqlQuery), (exp.Describe, exp.Command))  # SHOW and EXPLAIN
        return _reverse_cells(df, catalog) if is_metadata else df

    def read(original: Any, self: Any, name: str) -> Any:
        return original(self, rewrite_name(name, catalog, is_temp_view))

    def named_write(kind: WriteKind) -> Callable[..., Any]:
        def wrapper(original: Any, self: Any, name: str, *args: Any, **kwargs: Any) -> Any:
            if not state.allow_writes:
                _blocked("WRITE_NOT_ALLOWED", f"writing to {name} is not allowed in this run")
            result = original(self, rewrite_name(name, catalog, is_temp_view), *args, **kwargs)
            # For writeTo the write happens later, on the returned builder.
            state.writes_applied.append(WriteOp(kind=kind, target=name_ref(name), location=Location()))
            return result

        return wrapper

    def select_expr(original: Any, self: DataFrame, *expr: Any) -> Any:
        for e in expr[0] if len(expr) == 1 and isinstance(expr[0], list) else expr:
            check_expression(e)
        return original(self, *expr)

    def filter_(original: Any, self: DataFrame, condition: Any) -> Any:
        if isinstance(condition, str):
            check_expression(condition)
        return original(self, condition)

    def expr(original: Any, text: str) -> Any:
        check_expression(text)
        return original(text)

    def path_method(original: Any, *_: Any, **__: Any) -> NoReturn:
        _blocked("PY_PATH_ACCESS", f"path-based '.{original.__name__}()' is not allowed", PATH_HINT)

    session_cls, df_cls = type(spark), type(spark.range(0))
    patches: list[tuple[Any, str, Callable[..., Any]]] = [
        (session_cls, "sql", sql),
        (session_cls, "table", read),  # goes to the JVM directly, not through DataFrameReader.table
        (DataFrameReader, "table", read),
        (DataFrameWriter, "saveAsTable", named_write("SAVE_AS_TABLE")),
        (DataFrameWriter, "insertInto", named_write("INSERT")),
        (df_cls, "writeTo", named_write("WRITE_TO")),
        (df_cls, "selectExpr", select_expr),
        (df_cls, "filter", filter_),  # `where` calls `filter`
        (F, "expr", expr),
    ]
    patches += [
        (cls, name, path_method) for cls in (DataFrameReader, DataFrameWriter) for name in PATH_METHODS & set(dir(cls))
    ]
    for owner, name, wrapper in patches:
        _patch(owner, name, wrapper)
    return state
