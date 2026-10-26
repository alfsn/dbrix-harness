import re

import pytest

from dbrix.policy import SandboxSecurityException
from dbrix.rewrite import reverse_map, reverse_rules, rewrite_name, rewrite_sql

CATALOG = "prod_catalog"


def temp_views(name: str) -> bool:
    return name == "v"


# Every row of the §6.1 table, plus each statement kind of §5.3.
@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM prod_catalog.silver.posts", "SELECT * FROM silver.posts"),
        ("SELECT * FROM PROD_CATALOG.Silver.posts", "SELECT * FROM Silver.posts"),
        ("SELECT * FROM `prod_catalog`.`gold`.`a-b`", "SELECT * FROM `gold`.`a-b`"),
        (
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t JOIN prod_catalog.gold.a AS a ON a.x = t.x",
            "WITH t AS (SELECT 1 AS x) SELECT * FROM t JOIN gold.a AS a ON a.x = t.x",
        ),
        ("SELECT * FROM v", "SELECT * FROM v"),
        ("SELECT * FROM range(10)", "SELECT * FROM RANGE(10)"),
        ("SELECT current_catalog()", "SELECT 'prod_catalog'"),
        (
            "CREATE TEMP VIEW w AS SELECT * FROM prod_catalog.silver.posts",
            "CREATE TEMPORARY VIEW w AS SELECT * FROM silver.posts",
        ),
        ("DROP VIEW w", "DROP VIEW w"),
        ("DROP VIEW prod_catalog.gold.w", "DROP VIEW gold.w"),
        ("CREATE TABLE prod_catalog.gold.s AS SELECT 1 AS x", "CREATE TABLE gold.s AS SELECT 1 AS x"),
        ("INSERT INTO prod_catalog.gold.s SELECT * FROM v", "INSERT INTO gold.s SELECT * FROM v"),
        ("UPDATE prod_catalog.gold.s SET x = 2", "UPDATE gold.s SET x = 2"),
        ("DELETE FROM prod_catalog.gold.s WHERE x = 1", "DELETE FROM gold.s WHERE x = 1"),
        (
            (
                "MERGE INTO prod_catalog.gold.s t USING prod_catalog.silver.posts p ON t.x = p.x "
                "WHEN MATCHED THEN UPDATE SET *"
            ),
            "MERGE INTO gold.s AS t USING silver.posts AS p ON t.x = p.x WHEN MATCHED THEN UPDATE SET *",
        ),
        ("ALTER TABLE prod_catalog.gold.s ADD COLUMNS (y INT)", "ALTER TABLE gold.s ADD COLUMNS (y INT)"),
        ("TRUNCATE TABLE prod_catalog.gold.s", "TRUNCATE TABLE gold.s"),
        ("DROP TABLE prod_catalog.gold.s", "DROP TABLE gold.s"),
        ("DESCRIBE prod_catalog.silver.posts", "DESCRIBE silver.posts"),
        ("DESCRIBE EXTENDED prod_catalog.silver.posts", "DESCRIBE EXTENDED silver.posts"),
        ("DESCRIBE HISTORY prod_catalog.silver.posts", "DESCRIBE HISTORY silver.posts"),
        ("DESCRIBE DETAIL prod_catalog.silver.posts", "DESCRIBE DETAIL silver.posts"),
        ("EXPLAIN SELECT cost FROM prod_catalog.silver.posts", "EXPLAIN SELECT cost FROM silver.posts"),
        ("EXPLAIN COST SELECT * FROM prod_catalog.silver.posts", "EXPLAIN COST SELECT * FROM silver.posts"),
        ("SHOW TABLES", "SHOW TABLES"),
        ("SHOW TABLES IN prod_catalog.silver LIKE 'p*'", "SHOW TABLES IN silver LIKE 'p*'"),
        ("SHOW SCHEMAS IN prod_catalog", "SHOW SCHEMAS IN spark_catalog"),
        ("SHOW CATALOGS", "SHOW CATALOGS"),
        ("SHOW COLUMNS IN prod_catalog.silver.posts", "SHOW COLUMNS IN silver.posts"),
    ],
)
def test_rewrite_sql(sql: str, expected: str) -> None:
    assert rewrite_sql(sql, CATALOG, temp_views) == expected


@pytest.mark.parametrize(
    ("sql", "hint"),
    [
        ("SELECT * FROM posts", "use a 3-part name: prod_catalog.<schema>.posts"),
        ("SELECT * FROM silver.posts", "use a 3-part name: prod_catalog.silver.posts"),
        ("SELECT * FROM other.silver.posts", "tables live in prod_catalog.<bronze|silver|gold>.<table>"),
        ("SELECT * FROM prod_catalog.default.posts", "tables live in prod_catalog.<bronze|silver|gold>.<table>"),
        ("SELECT * FROM spark_catalog.silver.posts", "tables live in prod_catalog.<bronze|silver|gold>.<table>"),
        ("CREATE VIEW w AS SELECT 1", "use a 3-part name: prod_catalog.<schema>.w"),
        ("EXPLAIN SELECT * FROM silver.posts", "use a 3-part name: prod_catalog.silver.posts"),
        ("SHOW TABLES IN silver", "use SHOW TABLES IN prod_catalog.<schema>"),
        ("SHOW SCHEMAS IN other", "use SHOW SCHEMAS IN prod_catalog"),
        ("SHOW COLUMNS IN posts", "use a 3-part name: prod_catalog.<schema>.posts"),
    ],
)
def test_rewrite_sql_blocks(sql: str, hint: str) -> None:
    with pytest.raises(SandboxSecurityException) as e:
        rewrite_sql(sql, CATALOG, temp_views)
    assert (e.value.rule_id, e.value.hint) == ("TABLE_NAME_NOT_ALLOWED", hint)


def test_rewrite_sql_one_statement() -> None:
    assert rewrite_sql("SELECT 1;", CATALOG, temp_views) == "SELECT 1"
    with pytest.raises(SandboxSecurityException) as e:
        rewrite_sql("SELECT * FROM prod_catalog.silver.posts; SELECT 2", CATALOG, temp_views)
    assert e.value.rule_id == "SQL_MULTIPLE_STATEMENTS"


def test_rewrite_name() -> None:
    assert rewrite_name("prod_catalog.gold.s", CATALOG, temp_views) == "gold.s"
    assert rewrite_name("v", CATALOG, temp_views) == "v"
    with pytest.raises(SandboxSecurityException) as e:
        rewrite_name("gold.s", CATALOG, temp_views)
    assert e.value.language == "pyspark"


@pytest.mark.parametrize(
    "name",
    ["prod_catalog.silver.posts", "prod_catalog.gold.my_summary", "prod_catalog.bronze.raw_creator_profiles"],
)
def test_names_round_trip(name: str) -> None:
    sandbox = rewrite_name(name, CATALOG, temp_views)
    assert reverse_map(sandbox, CATALOG) == name
    assert reverse_map(f"spark_catalog.{sandbox}", CATALOG) == name
    assert reverse_map(f"/sandbox/{sandbox.replace('.', '/')}", CATALOG) == name


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `silver`.`postz` cannot be found.",
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `prod_catalog`.`silver`.`postz` cannot be found.",
        ),
        ("`spark_catalog`.`gold`.`s`", "`prod_catalog`.`gold`.`s`"),
        ("Location: file:/sandbox/gold/s", "Location: prod_catalog.gold.s"),
        ("file:/sandbox/gold/s/_delta_log/00000.json", "<sandbox>"),
        ("Path does not exist: /sandbox/_results/x.csv.", "Path does not exist: <sandbox>"),
        ("spark_catalog", "prod_catalog"),
        ("Relation spark_catalog.silver.posts[post_id#1]", "Relation prod_catalog.silver.posts[post_id#1]"),
        ("prod_catalog.silver.posts", "prod_catalog.silver.posts"),
        ("`prod_catalog`.`silver`.`posts`", "`prod_catalog`.`silver`.`posts`"),
        ("posts.silver_count", "posts.silver_count"),
        ("x.silver.posts", "x.silver.posts"),
    ],
)
def test_reverse_map(text: str, expected: str) -> None:
    assert reverse_map(text, CATALOG) == expected


def test_reverse_rules_are_java_compatible() -> None:
    """Spark runs the same patterns with Java regex: keep to the shared subset (no named groups, no \\Z, …)."""
    for pattern, replacement in reverse_rules(CATALOG):
        re.compile(pattern)
        assert "(?P" not in pattern and "\\" not in replacement
