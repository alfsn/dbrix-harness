"""Table-name rewriting (§6.1) and reverse mapping (§6.5). Pure functions: no Spark, no Docker."""

import re
from collections.abc import Callable

from sqlglot import exp

from dbrix.config import SANDBOX_MOUNT, SCHEMAS
from dbrix.policy.exceptions import SandboxSecurityException
from dbrix.policy.report import Language
from dbrix.policy.sql import (
    DIALECT,
    SHOW,
    name_ref,
    parse_statement,
    parse_table,
    split_command,
    split_explain,
    split_statements,
    tables,
)

IsTempView = Callable[[str], bool]  # lowercase 1-part name -> is it a temp view in the session?

SCHEMA = "(" + "|".join(SCHEMAS) + ")"


def _not_allowed(ref: str, language: Language, hint: str) -> SandboxSecurityException:
    return SandboxSecurityException("TABLE_NAME_NOT_ALLOWED", f"'{ref}' is not allowed", language, None, ref, hint)


def _rewrite_table(table: exp.Table, catalog: str, is_temp_view: IsTempView, language: Language) -> None:
    """Resolve one table reference in place (the §6.1 table)."""
    parts = [p.name.lower() for p in table.parts]
    if len(parts) == 3 and parts[0] == catalog and parts[1] in SCHEMAS:
        table.set("catalog", None)
    elif len(parts) == 1 and is_temp_view(parts[0]):
        pass
    elif len(parts) < 3:
        schema = parts[0] if len(parts) == 2 else "<schema>"
        raise _not_allowed(".".join(parts), language, f"use a 3-part name: {catalog}.{schema}.{parts[-1]}")
    else:
        raise _not_allowed(".".join(parts), language, f"tables live in {catalog}.<{'|'.join(SCHEMAS)}>.<table>")


def _view_target(stmt: exp.Expr) -> exp.Table | None:
    """The 1-part name of CREATE TEMP VIEW or DROP VIEW: a temp view that may not exist yet."""
    if not isinstance(stmt, (exp.Create, exp.Drop)) or stmt.args.get("kind") != "VIEW":
        return None
    if isinstance(stmt, exp.Create) and not stmt.find(exp.TemporaryProperty):
        return None
    target = stmt.find(exp.Table)
    return target if target is not None and not target.db else None


def _show_schema(what: str, name: str, catalog: str) -> str:
    """The `IN …` part of SHOW TABLES (`<catalog>.<schema>`) or SHOW SCHEMAS (`<catalog>`)."""
    parts = name_ref(name).split(".")
    if what == "TABLES" and len(parts) == 2 and parts[0] == catalog and parts[1] in SCHEMAS:
        return parts[1]
    if what in ("SCHEMAS", "DATABASES") and parts == [catalog]:
        return "spark_catalog"
    form = f"{catalog}.<schema>" if what == "TABLES" else catalog
    raise _not_allowed(".".join(parts), "sql", f"use SHOW {what} IN {form}")


def rewrite_sql(statement: str, catalog: str, is_temp_view: IsTempView) -> str:
    """One statement with every table name resolved and `current_catalog()` replaced (§6.1).
    Raises SandboxSecurityException for a name outside `catalog`, or for more than one statement."""
    if len(split_statements(statement)) > 1:
        hint = "run one statement per spark.sql call"
        raise SandboxSecurityException(
            "SQL_MULTIPLE_STATEMENTS", "one statement at a time", "sql", None, statement, hint
        )
    stmt = parse_statement(statement)
    if not isinstance(stmt, exp.Command):
        skip = _view_target(stmt)
        for table in tables(stmt):
            if table is not skip:
                _rewrite_table(table, catalog, is_temp_view, "sql")
        stmt = stmt.transform(lambda n: exp.Literal.string(catalog) if isinstance(n, exp.CurrentCatalog) else n)
        return stmt.sql(dialect=DIALECT)
    word, rest = split_command(statement)
    if word == "EXPLAIN":
        mode, inner = split_explain(rest)
        return f"EXPLAIN {mode}{rewrite_sql(inner, catalog, is_temp_view)}"
    rest = rest.strip()
    if word != "SHOW" or not (m := SHOW.fullmatch(rest)):
        return statement
    if m["table"]:
        table = parse_table(m["table"])
        _rewrite_table(table, catalog, is_temp_view, "sql")
        group, new = "table", table.sql(dialect=DIALECT)
    elif m["schema"]:
        group, new = "schema", _show_schema(m["what"].upper(), m["schema"], catalog)
    else:
        return statement
    start, end = m.span(group)
    return f"SHOW {rest[:start]}{new}{rest[end:]}"


def rewrite_name(name: str, catalog: str, is_temp_view: IsTempView) -> str:
    """A table name from PySpark (`spark.table`, `saveAsTable`, …), resolved like a name in SQL."""
    table = parse_table(name)
    _rewrite_table(table, catalog, is_temp_view, "pyspark")
    return table.sql(dialect=DIALECT)


def reverse_rules(catalog: str) -> list[tuple[str, str]]:
    """(regex, replacement with `$n` groups), applied in order. The patterns work in both Python and Java, so the
    same list maps text here and result cells in Spark (`regexp_replace`)."""
    return [
        (rf"(?:file:)?{SANDBOX_MOUNT}/{SCHEMA}/(\w+)(?![\w/])", rf"{catalog}.$1.$2"),
        (rf"(?:file:)?{SANDBOX_MOUNT}[\w./-]*", "<sandbox>"),
        (rf"(?<![\w.`])(?:`spark_catalog`\.)?`{SCHEMA}`\.`(\w+)`", rf"`{catalog}`.`$1`.`$2`"),
        (rf"(?<![\w.`/])(?:spark_catalog\.)?{SCHEMA}\.(\w+)", rf"{catalog}.$1.$2"),
        (r"\bspark_catalog\b", catalog),
    ]


def reverse_map(text: str, catalog: str) -> str:
    """Sandbox names and paths in `text` shown as prod names (§6.5)."""
    for pattern, replacement in reverse_rules(catalog):
        text = re.sub(pattern, replacement.replace("$", "\\"), text)
    return text
