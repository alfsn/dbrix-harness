"""SQL policy (§5.3): sqlglot, dialect `spark`."""

import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Final, Literal, NoReturn

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from dbrix.policy.exceptions import SandboxSecurityException
from dbrix.policy.report import Location, PolicyReport, TableRef, WriteKind, WriteOp

DIALECT = "spark"

# sqlglot warns on stderr for every statement it keeps as a Command (SHOW, EXPLAIN); those are expected here.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Every function of the pinned Spark, reviewed once; a Docker test compares it with the image (§5.2).
AUDITED_FUNCTIONS = Path(__file__).with_name("spark_functions.txt")

BLOCKED_FUNCTIONS = frozenset(
    {
        # call Java
        "reflect",
        "java_method",
        "try_reflect",
        # read a path
        "table_changes_by_path",
        "from_protobuf",
        "to_protobuf",
        # leak sandbox paths or names (§6.5)
        "input_file_name",
        "input_file_block_start",
        "input_file_block_length",
        "current_path",
    }
)

READ: Final = "READ"

# (statement node, its `kind` for CREATE/DROP/ALTER) -> READ or the write kind. Anything else is blocked.
STATEMENTS: dict[tuple[type[exp.Expr], str | None], WriteKind | Literal["READ"]] = {
    (exp.Select, None): READ,
    (exp.Union, None): READ,
    (exp.Intersect, None): READ,
    (exp.Except, None): READ,
    (exp.Describe, None): READ,
    (exp.Insert, None): "INSERT",
    (exp.Update, None): "UPDATE",
    (exp.Delete, None): "DELETE",
    (exp.Merge, None): "MERGE",
    (exp.TruncateTable, None): "TRUNCATE",
    (exp.Create, "TABLE"): "CREATE_TABLE",
    (exp.Create, "VIEW"): "CREATE_VIEW",
    (exp.Alter, "TABLE"): "ALTER_TABLE",
    (exp.Drop, "TABLE"): "DROP_TABLE",
    (exp.Drop, "VIEW"): "DROP_VIEW",
}

STATEMENT_HINTS: dict[type[exp.Expr], str] = {
    exp.Use: "write 3-part table names: <catalog>.<schema>.<table>",
    exp.Set: "session settings are fixed",
}

# sqlglot keeps these as unparsed text (Command) or fails on them; they are parsed here.
DESCRIBE_DETAIL = re.compile(r"DESC(?:RIBE)?\s+DETAIL\s+(?P<table>\S+)", re.IGNORECASE)
COMMAND = re.compile(r"(?P<word>\w+)\s*(?P<rest>.*)", re.DOTALL)
EXPLAIN_MODE = re.compile(r"(?:EXTENDED|CODEGEN|COST|FORMATTED)\s+", re.IGNORECASE)
SHOW = re.compile(
    r"(?P<what>TABLES|SCHEMAS|DATABASES|CATALOGS)(?:\s+(?:IN|FROM)\s+(?P<schema>\S+))?(?:\s+(?:LIKE\s+)?'[^']*')?"
    r"|COLUMNS\s+(?:IN|FROM)\s+(?P<table>\S+)",
    re.IGNORECASE,
)

PATH_HINT = "read tables by name: <catalog>.<schema>.<table>"


def _function_name(node: exp.Func) -> str:
    return (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()


def _is_path_source(node: exp.Expr) -> bool:
    if isinstance(node, (exp.LocationProperty, exp.Directory)):
        return True
    if not isinstance(node, exp.Table):
        return False
    if isinstance(node.this, exp.Func):  # table-valued function: read_files('/p') is a path, range(10) is not
        return any(lit.is_string for lit in node.this.find_all(exp.Literal))
    return any("/" in part.name for part in node.parts)


# (rule_id, matches node, message, hint) checked on every node of a statement or expression.
TREE_RULES: list[tuple[str, Callable[[exp.Expr], bool], str, str | None]] = [
    ("SQL_PATH_SOURCE", _is_path_source, "path-based sources and locations are not allowed", PATH_HINT),
    (
        "SQL_FUNCTION_BLOCKED",
        lambda n: isinstance(n, exp.Func) and _function_name(n) in BLOCKED_FUNCTIONS,
        "this function is not allowed",
        None,
    ),
    (
        "SQL_METADATA_COLUMN",
        lambda n: isinstance(n, exp.Identifier) and n.name.lower() == "_metadata",
        "the _metadata column is not allowed",
        None,
    ),
]


def _blocked(rule_id: str, message: str, location: Location | None, snippet: str, hint: str | None = None) -> NoReturn:
    raise SandboxSecurityException(rule_id, message, "sql", location, snippet, hint)


def table_ref(table: exp.Table) -> TableRef:
    return ".".join(part.name.lower() for part in table.parts)


def parse_table(name: str, location: Location | None = None) -> exp.Table:
    """A table name in a string (`spark.table("…")`, `SHOW COLUMNS IN …`), path rule applied."""
    try:
        table = exp.to_table(name, dialect=DIALECT)
    except (ParseError, TokenError) as e:
        _blocked("SQL_PARSE_ERROR", f"the table name does not parse: {e}", location, name)
    _check_tree(table, location)
    return table


def name_ref(name: str, location: Location | None = None) -> TableRef:
    return table_ref(parse_table(name, location))


def tables(tree: exp.Expr) -> list[exp.Table]:
    """Named tables in `tree`, without CTE names and table-valued functions."""
    ctes = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    return [t for t in tree.find_all(exp.Table) if isinstance(t.this, exp.Identifier) and (t.db or t.name not in ctes)]


def split_statements(sql: str) -> list[str]:
    """The statements of `sql`, cut at `;` tokens (so `;` in strings and comments is kept)."""
    cuts = [t.start for t in sqlglot.tokenize(sql, dialect=DIALECT) if t.token_type == sqlglot.TokenType.SEMICOLON]
    parts = [sql[i:j].strip() for i, j in zip([0] + [c + 1 for c in cuts], cuts + [len(sql)])]
    return [p for p in parts if p]


def _check_tree(tree: exp.Expr, location: Location | None) -> None:
    for node in tree.walk():
        for rule_id, matches, message, hint in TREE_RULES:
            if matches(node):
                _blocked(rule_id, message, location, node.sql(dialect=DIALECT), hint)


def _kind(stmt: exp.Expr) -> WriteKind | Literal["READ"] | None:
    kind = stmt.args.get("kind") if isinstance(stmt, (exp.Create, exp.Drop, exp.Alter)) else None
    if kind == "VIEW" and isinstance(stmt, exp.Create) and stmt.find(exp.TemporaryProperty):
        return None if stmt.find(exp.GlobalProperty) else READ
    if kind == "VIEW" and isinstance(stmt, exp.Drop) and all(not t.db for t in stmt.find_all(exp.Table)):
        return READ  # a 1-part name is a temp view (permanent views need 3-part names, §6.1)
    return STATEMENTS.get((type(stmt), kind))


def parse_statement(text: str, location: Location | None = None) -> exp.Expr:
    """One statement as a tree. sqlglot keeps SHOW and EXPLAIN as `exp.Command`; DESCRIBE DETAIL is built here."""
    try:
        if m := DESCRIBE_DETAIL.fullmatch(text):
            return exp.Describe(this=exp.to_table(m["table"], dialect=DIALECT), style="DETAIL")
        return sqlglot.parse_one(text, dialect=DIALECT)
    except (ParseError, TokenError) as e:
        _blocked("SQL_PARSE_ERROR", f"the statement does not parse: {e}", location, text)


def split_command(text: str) -> tuple[str, str]:
    """An `exp.Command` statement -> (first word in upper case, the rest)."""
    m = COMMAND.fullmatch(text)
    return (m["word"].upper(), m["rest"]) if m else ("", text)


def split_explain(rest: str) -> tuple[str, str]:
    """The text after EXPLAIN -> (its mode word with trailing space, or "", the explained statement)."""
    m = EXPLAIN_MODE.match(rest)
    return (m[0], rest[m.end() :]) if m else ("", rest)


def _check_command(text: str, location: Location, report: PolicyReport) -> None:
    """A statement sqlglot keeps as unparsed text: only EXPLAIN and a few SHOW forms are allowed."""
    word, rest = split_command(text)
    if word == "EXPLAIN":  # treated like the statement it explains (§5.3)
        return _check_statement(split_explain(rest)[1], location, report)
    if word == "SHOW" and (m := SHOW.fullmatch(rest.strip())):
        if m["schema"] and "/" in m["schema"]:
            _blocked("SQL_PATH_SOURCE", "path-based sources and locations are not allowed", location, text, PATH_HINT)
        if m["table"]:
            report.reads.add(name_ref(m["table"], location))
        return None
    _blocked("SQL_STMT_NOT_ALLOWED", f"{word} statements are not allowed", location, text)


def _check_statement(text: str, location: Location, report: PolicyReport) -> None:
    stmt = parse_statement(text, location)
    if isinstance(stmt, exp.Command):
        return _check_command(text, location, report)
    kind = _kind(stmt)
    if kind is None:
        _blocked(
            "SQL_STMT_NOT_ALLOWED", "this statement is not allowed", location, text, STATEMENT_HINTS.get(type(stmt))
        )
    _check_tree(stmt, location)
    target = stmt.find(exp.Table) if kind != READ or isinstance(stmt, (exp.Create, exp.Drop)) else None
    if kind != READ:
        report.writes.append(WriteOp(kind=kind, target=table_ref(target) if target else None, location=location))
    report.reads |= {table_ref(t) for t in tables(stmt) if t is not target}


def check_sql(sql: str) -> PolicyReport:
    """Check every statement of `sql` (§5.3). Raises SandboxSecurityException on the first blocked one."""
    report = PolicyReport()
    try:
        statements = split_statements(sql)
    except TokenError as e:
        _blocked("SQL_PARSE_ERROR", f"the SQL does not parse: {e}", None, sql)
    for i, text in enumerate(statements):
        _check_statement(text, Location(statement=i), report)
    return report


def check_expression(text: str, location: Location | None = None) -> None:
    """Check a SQL expression string (`F.expr`, `selectExpr`, …) with the function and `_metadata` rules."""
    try:
        tree = sqlglot.parse_one(text, dialect=DIALECT)
    except (ParseError, TokenError) as e:
        _blocked("SQL_PARSE_ERROR", f"the expression does not parse: {e}", location, text)
    _check_tree(tree, location)
