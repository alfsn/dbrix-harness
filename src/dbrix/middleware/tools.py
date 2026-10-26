"""`run_sql` and `run_pyspark` as a Databricks agent would see them (§8.1). Their bodies never run: the middleware
answers every call. Called without it, they fail closed."""

from langchain_core.tools import tool


class NotSandboxedError(RuntimeError):
    pass


def _fail() -> str:
    raise NotSandboxedError("this tool only runs behind DbrixSandboxMiddleware; nothing was executed")


@tool
def run_sql(query: str) -> str:
    """Run Spark SQL against the Databricks catalog and return the result. Use 3-part table names
    (`prod_catalog.<schema>.<table>`). Several statements may be separated by `;`; the result is the last one's."""
    return _fail()


@tool
def run_pyspark(code: str) -> str:
    """Run PySpark code against the Databricks catalog. `spark`, `F` (pyspark.sql.functions), and `T`
    (pyspark.sql.types) are defined. The result is the DataFrame in `result`, or the last expression's value if it
    is a DataFrame; printed output is returned too."""
    return _fail()
