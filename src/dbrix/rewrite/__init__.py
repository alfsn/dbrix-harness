"""Table-name rewriting (§6.1)."""

import sqlglot
from sqlglot import exp


def to_sandbox_sql(sql: str, catalog: str) -> str:
    """Rewrite `<catalog>.<schema>.<table>` to `<schema>.<table>`, the name in the local metastore."""
    tree = sqlglot.parse_one(sql, dialect="spark")
    for table in tree.find_all(exp.Table):
        if table.catalog == catalog:
            table.set("catalog", None)
    return tree.sql(dialect="spark")
