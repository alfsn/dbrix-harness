"""What the policy found in a piece of code (§5.1)."""

from typing import Literal

from pydantic import BaseModel

Language = Literal["sql", "pyspark"]

# Dotted table name, lowercase and unquoted: "prod_catalog.silver.posts".
TableRef = str

WriteKind = Literal[
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "CREATE_TABLE",
    "CREATE_VIEW",
    "ALTER_TABLE",
    "DROP_TABLE",
    "DROP_VIEW",
    "TRUNCATE",
    "SAVE_AS_TABLE",
    "WRITE_TO",
]


class Location(BaseModel):
    statement: int | None = None  # SQL: index of the statement, from 0
    line: int | None = None  # Python; for SQL inside spark.sql, the call's position
    col: int | None = None


class WriteOp(BaseModel):
    kind: WriteKind
    target: TableRef | None  # None when the name is built at runtime
    location: Location


class PolicyReport(BaseModel):
    reads: set[TableRef] = set()
    writes: list[WriteOp] = []
    has_dynamic_sql: bool = False
