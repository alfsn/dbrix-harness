import pytest

from dbrix.rewrite import to_sandbox_sql


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM prod_catalog.silver.posts", "SELECT * FROM silver.posts"),
        (
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t JOIN prod_catalog.gold.a AS a ON a.x = t.x",
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t JOIN gold.a AS a ON a.x = t.x",
        ),
        ("SELECT * FROM other.silver.posts", "SELECT * FROM other.silver.posts"),
    ],
)
def test_to_sandbox_sql(sql: str, expected: str) -> None:
    assert to_sandbox_sql(sql, "prod_catalog") == expected
