"""PySpark policy (§5.4): Python `ast`."""

import ast
import importlib
from collections.abc import Callable
from types import ModuleType
from typing import NamedTuple, NoReturn

from dbrix.policy.exceptions import SandboxSecurityException
from dbrix.policy.report import Location, PolicyReport, WriteKind, WriteOp
from dbrix.policy.sql import BLOCKED_FUNCTIONS, check_expression, check_sql, name_ref

# Names the runtime defines before agent code runs (§6.2).
PREDEFINED_MODULES = {"F": "pyspark.sql.functions", "T": "pyspark.sql.types"}

ALLOWED_MODULES = frozenset(
    {
        "pyspark.sql",
        "pyspark.sql.functions",
        "pyspark.sql.types",
        "pyspark.sql.window",
        "math",
        "datetime",
        "decimal",
        "json",
        "re",
        "collections",
        "itertools",
        "functools",
        "statistics",
        "typing",
    }
)

BLOCKED_BUILTINS = frozenset(
    {
        "exec",
        "eval",
        "compile",
        "__import__",
        "open",
        "getattr",
        "setattr",
        "delattr",
        "globals",
        "locals",
        "vars",
        "input",
        "breakpoint",
        "memoryview",
        "exit",
        "quit",
        "help",
    }
)

PATH_HINT = 'read and write tables by name: spark.table("<catalog>.<schema>.<table>")'

# Attribute (or `parent.attr`) -> hint. Python names of blocked SQL functions are blocked the same way.
BLOCKED_ATTRS: dict[str, str | None] = {
    "sparkContext": None,
    "rdd": "use DataFrame methods",
    "catalog": "use SQL: SHOW TABLES, SHOW COLUMNS, DESCRIBE",
    "pipe": None,
    "addFile": None,
    "addPyFile": None,
    "registerJavaFunction": None,
    "newSession": "use the predefined `spark`",
    "builder": "use the predefined `spark`",
    "dataSource": None,
    "addArtifact": None,
    "addArtifacts": None,
    "copyFromLocalToFs": None,
    "inputFiles": None,
    "readStream": PATH_HINT,
    "writeStream": PATH_HINT,
    "conf.set": "session settings are fixed",
    "call_function": "call the function directly",
} | dict.fromkeys(BLOCKED_FUNCTIONS)

# Reader/writer methods that take a path, blocked when called on a `.read` or `.write` chain.
PATH_METHODS = frozenset({"load", "save", "parquet", "csv", "json", "text", "orc", "xml", "jdbc"})
WRITE_METHODS: dict[str, WriteKind] = {"saveAsTable": "SAVE_AS_TABLE", "insertInto": "INSERT", "writeTo": "WRITE_TO"}
# Methods whose keyword arguments are options; `path=` and `**` (which may hide `path`) are blocked.
OPTION_KEYWORD_METHODS = frozenset({"options", "saveAsTable"})
SQL_EXPRESSION_METHODS = frozenset({"expr", "selectExpr", "filter", "where"})

Violation = tuple[str, str, str | None]  # rule_id, message, hint


class Scope(NamedTuple):
    modules: dict[str, str]  # name bound to a module -> module name
    attribute_bases: set[int]  # ids of nodes used as the base of an attribute


def _location(node: ast.AST, statement: int | None = None) -> Location:
    return Location(statement=statement, line=getattr(node, "lineno", None), col=getattr(node, "col_offset", None))


def _blocked(code: str, node: ast.AST, violation: Violation) -> NoReturn:
    rule_id, message, hint = violation
    raise SandboxSecurityException(
        rule_id, message, "pyspark", _location(node), ast.get_source_segment(code, node), hint
    )


def _receiver_attrs(node: ast.expr) -> list[str]:
    """Attribute names along a call chain: `spark.read.format("x").load` -> [load, format, read]."""
    names = []
    while isinstance(node, (ast.Attribute, ast.Call)):
        if isinstance(node, ast.Call):
            node = node.func
        else:
            names.append(node.attr)
            node = node.value
    return names


def _reachable(module: str) -> bool:
    """An allowed module, or a package on the way to one (`pyspark` for `pyspark.sql`)."""
    return any(m == module or m.startswith(module + ".") for m in ALLOWED_MODULES)


def _first_arg(call: ast.Call) -> ast.expr | None:
    return call.args[0] if call.args else (call.keywords[0].value if call.keywords else None)


def _string(node: ast.expr | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


# Rules: each looks at one node and returns a violation or None. New rules go in RULES.


def _private_names(node: ast.AST, _: Scope) -> Violation | None:
    match node:
        case ast.Attribute(attr=attr) if attr.startswith("_"):
            hint = None if attr.startswith("__") else f'for a field, use row["{attr}"]'
            return "PY_PRIVATE_NAME", f"private attribute '{attr}' is not allowed", hint
        case ast.alias(name=name) if name.split(".")[-1].startswith("_"):
            return "PY_PRIVATE_NAME", f"importing private name '{name}' is not allowed", None
    names: list[str | None] = []
    match node:
        case ast.Name(id=n) | ast.arg(arg=n) | ast.keyword(arg=n) | ast.ExceptHandler(name=n):
            names = [n]
        case ast.FunctionDef(name=n) | ast.AsyncFunctionDef(name=n) | ast.ClassDef(name=n):
            names = [n]
        case ast.alias(asname=n):
            names = [n]
        case ast.Global(names=ns) | ast.Nonlocal(names=ns):
            names = list(ns)
    for n in names:
        if n and n.startswith("__"):
            return "PY_PRIVATE_NAME", f"dunder name '{n}' is not allowed", None
    return None


def _builtins(node: ast.AST, _: Scope) -> Violation | None:
    if isinstance(node, ast.Name) and node.id in BLOCKED_BUILTINS:
        return "PY_FORBIDDEN_BUILTIN", f"'{node.id}' is not allowed", None
    return None


def _attributes(node: ast.AST, _: Scope) -> Violation | None:
    if isinstance(node, ast.alias):  # from pyspark.sql.functions import reflect
        keys = [node.name]
    elif isinstance(node, ast.Attribute):
        keys = [node.attr] + ([f"{node.value.attr}.{node.attr}"] if isinstance(node.value, ast.Attribute) else [])
    else:
        return None
    for key in keys:
        if key in BLOCKED_ATTRS:
            return "PY_FORBIDDEN_API", f"'{key}' is not allowed", BLOCKED_ATTRS[key]
    return None


def _modules(node: ast.AST, scope: Scope) -> Violation | None:
    """A chain from a module may only pass through allowed modules; a module is only used as `name.attr`."""
    if isinstance(node, ast.Name) and node.id in scope.modules and id(node) not in scope.attribute_bases:
        return "PY_FORBIDDEN_MODULE", f"module '{node.id}' may only be used as {node.id}.<name>", None
    if not isinstance(node, ast.Attribute):
        return None
    attrs: list[str] = []
    root: ast.expr = node
    while isinstance(root, ast.Attribute):
        attrs.append(root.attr)
        root = root.value
    if not (isinstance(root, ast.Name) and root.id in scope.modules):
        return None
    obj: object = importlib.import_module(scope.modules[root.id])
    for attr in reversed(attrs):
        if not isinstance(obj, ModuleType):
            return None  # past the modules; the other rules apply
        parent, obj = obj.__name__, getattr(obj, attr, None)
        allowed = _reachable(obj.__name__) if isinstance(obj, ModuleType) else parent in ALLOWED_MODULES
        if not allowed:
            return "PY_FORBIDDEN_MODULE", f"'{parent}.{attr}' is not allowed", None
    return None


def _paths(node: ast.AST, _: Scope) -> Violation | None:
    if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
        return None
    method = node.func.attr
    if method in PATH_METHODS and {"read", "write"} & set(_receiver_attrs(node.func.value)):
        return "PY_PATH_ACCESS", f"path-based '.{method}()' is not allowed", PATH_HINT
    keywords = [k.arg for k in node.keywords] if method in OPTION_KEYWORD_METHODS else []
    if None in keywords:
        return "PY_PATH_ACCESS", "options unpacked with ** are not allowed", "pass each option by name"
    path_option = method == "option" and (_string(_first_arg(node)) or "").lower() == "path"
    if path_option or any((k or "").lower() == "path" for k in keywords):
        return "PY_PATH_ACCESS", "a 'path' option is not allowed", PATH_HINT
    return None


def _metadata_strings(node: ast.AST, _: Scope) -> Violation | None:
    value = _string(node) if isinstance(node, ast.expr) else None
    if value is not None and value.lower().split(".")[0] == "_metadata":
        return "PY_METADATA_COLUMN", "the _metadata column is not allowed", None
    return None


RULES: list[Callable[[ast.AST, Scope], Violation | None]] = [
    _private_names,
    _builtins,
    _attributes,
    _modules,
    _paths,
    _metadata_strings,
]


def _imports(code: str, tree: ast.Module) -> dict[str, str]:
    """Check every import against ALLOWED_MODULES; return the names they bind to modules."""
    modules = dict(PREDEFINED_MODULES)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in ALLOWED_MODULES:
                    _blocked(code, node, ("PY_FORBIDDEN_IMPORT", f"importing '{alias.name}' is not allowed", None))
                bound = alias.asname or alias.name.split(".")[0]  # `import a.b` binds `a`
                modules[bound] = alias.name if alias.asname else bound
        elif isinstance(node, ast.ImportFrom):
            if node.level or node.module not in ALLOWED_MODULES:
                source = "." * node.level + (node.module or "")
                _blocked(code, node, ("PY_FORBIDDEN_IMPORT", f"importing from '{source}' is not allowed", None))
            module = importlib.import_module(node.module)
            for alias in node.names:
                obj = getattr(module, alias.name, None)
                if isinstance(obj, ModuleType):
                    if obj.__name__ not in ALLOWED_MODULES:
                        _blocked(code, node, ("PY_FORBIDDEN_IMPORT", f"importing '{alias.name}' is not allowed", None))
                    modules[alias.asname or alias.name] = obj.__name__
    return modules


def _record(node: ast.Call, report: PolicyReport) -> None:
    """Add the reads, writes, and SQL of one call to `report`. A name or SQL built at runtime is dynamic."""
    if not isinstance(node.func, ast.Attribute):
        return
    method, text = node.func.attr, _string(_first_arg(node))
    if method in WRITE_METHODS:
        target = name_ref(text, _location(node)) if text else None
        report.writes.append(WriteOp(kind=WRITE_METHODS[method], target=target, location=_location(node)))
        report.has_dynamic_sql |= target is None
    elif method == "table":
        if text is None:
            report.has_dynamic_sql = True
        else:
            report.reads.add(name_ref(text, _location(node)))
    elif method == "sql":
        if text is None:
            report.has_dynamic_sql = True
            return
        try:
            found = check_sql(text)
        except SandboxSecurityException as e:  # point at the spark.sql call
            e.location = _location(node, e.location.statement if e.location else None)
            raise
        report.reads |= found.reads
        report.writes += [
            w.model_copy(update={"location": _location(node, w.location.statement)}) for w in found.writes
        ]
    elif method in SQL_EXPRESSION_METHODS:
        for arg in filter(None, map(_string, node.args)):
            check_expression(arg, _location(node))


def check_pyspark(code: str) -> PolicyReport:
    """Check PySpark code (§5.4). Raises SandboxSecurityException on the first blocked construct."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        raise SandboxSecurityException(
            "PY_SYNTAX_ERROR",
            f"the code does not parse: {e.msg}",
            "pyspark",
            Location(line=e.lineno, col=e.offset and e.offset - 1),
        ) from None
    scope = Scope(_imports(code, tree), {id(n.value) for n in ast.walk(tree) if isinstance(n, ast.Attribute)})
    report = PolicyReport()
    for node in ast.walk(tree):
        for rule in RULES:
            if violation := rule(node, scope):
                _blocked(code, node, violation)
        if isinstance(node, ast.Call):
            _record(node, report)
    return report
