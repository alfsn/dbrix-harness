import inspect

import pytest

from dbrix.policy import SandboxSecurityException, check
from dbrix.policy.sql import AUDITED_FUNCTIONS, BLOCKED_FUNCTIONS


def blocked_rule(code: str, language: str) -> str:
    with pytest.raises(SandboxSecurityException) as e:
        check(code, language)  # type: ignore[arg-type]
    return e.value.rule_id


# §5.3: statements that only read, with the tables they read.
@pytest.mark.parametrize(
    ("sql", "reads"),
    [
        (
            "SELECT * FROM prod_catalog.Silver.Posts p JOIN prod_catalog.silver.brands b ON 1 = 1",
            {"prod_catalog.silver.posts", "prod_catalog.silver.brands"},
        ),
        ("WITH t AS (SELECT * FROM prod_catalog.silver.posts) SELECT * FROM t", {"prod_catalog.silver.posts"}),
        ("SELECT 1 UNION ALL SELECT 2 EXCEPT SELECT 3", set()),
        ("SELECT * FROM `prod_catalog`.silver.posts", {"prod_catalog.silver.posts"}),
        ("SELECT * FROM range(10)", set()),
        ("DESCRIBE prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("DESCRIBE TABLE EXTENDED prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("DESCRIBE HISTORY prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("DESCRIBE DETAIL prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("desc detail prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("SHOW TABLES", set()),
        ("SHOW TABLES IN prod_catalog.silver LIKE 'p*'", set()),
        ("SHOW SCHEMAS", set()),
        ("SHOW DATABASES", set()),
        ("SHOW CATALOGS", set()),
        ("SHOW COLUMNS IN prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("EXPLAIN SELECT * FROM prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("EXPLAIN FORMATTED SELECT 1", set()),
        ("CREATE TEMP VIEW v AS SELECT * FROM prod_catalog.silver.posts", {"prod_catalog.silver.posts"}),
        ("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1", set()),
        ("DROP VIEW v", set()),
        ("DROP VIEW IF EXISTS v", set()),
        ("SELECT 'a;b' AS x; SELECT 2 -- c;d", set()),
    ],
)
def test_sql_reads(sql: str, reads: set[str]) -> None:
    report = check(sql, "sql")
    assert (report.reads, report.writes, report.has_dynamic_sql) == (reads, [], False)


# §5.3: statements that write, with each write's kind and target, and the tables read.
@pytest.mark.parametrize(
    ("sql", "writes", "reads"),
    [
        (
            "INSERT INTO prod_catalog.gold.t SELECT * FROM prod_catalog.silver.posts",
            [("INSERT", "prod_catalog.gold.t")],
            {"prod_catalog.silver.posts"},
        ),
        ("INSERT OVERWRITE prod_catalog.gold.t SELECT 1", [("INSERT", "prod_catalog.gold.t")], set()),
        ("INSERT INTO prod_catalog.gold.t (x) VALUES (1)", [("INSERT", "prod_catalog.gold.t")], set()),
        ("UPDATE prod_catalog.gold.t SET x = 1", [("UPDATE", "prod_catalog.gold.t")], set()),
        ("DELETE FROM prod_catalog.gold.t WHERE x = 1", [("DELETE", "prod_catalog.gold.t")], set()),
        (
            "MERGE INTO prod_catalog.gold.t t USING prod_catalog.silver.posts s ON t.x = s.x WHEN MATCHED THEN DELETE",
            [("MERGE", "prod_catalog.gold.t")],
            {"prod_catalog.silver.posts"},
        ),
        (
            "CREATE TABLE prod_catalog.gold.t AS SELECT * FROM prod_catalog.silver.posts",
            [("CREATE_TABLE", "prod_catalog.gold.t")],
            {"prod_catalog.silver.posts"},
        ),
        ("CREATE TABLE prod_catalog.gold.t (x INT)", [("CREATE_TABLE", "prod_catalog.gold.t")], set()),
        ("CREATE OR REPLACE TABLE prod_catalog.gold.t AS SELECT 1", [("CREATE_TABLE", "prod_catalog.gold.t")], set()),
        ("CREATE VIEW prod_catalog.gold.v AS SELECT 1", [("CREATE_VIEW", "prod_catalog.gold.v")], set()),
        ("ALTER TABLE prod_catalog.gold.t ADD COLUMNS (y INT)", [("ALTER_TABLE", "prod_catalog.gold.t")], set()),
        ("DROP TABLE prod_catalog.gold.t", [("DROP_TABLE", "prod_catalog.gold.t")], set()),
        ("DROP VIEW prod_catalog.gold.v", [("DROP_VIEW", "prod_catalog.gold.v")], set()),
        ("TRUNCATE TABLE prod_catalog.gold.t", [("TRUNCATE", "prod_catalog.gold.t")], set()),
        # EXPLAIN X is treated like X, though nothing runs (§5.3)
        ("EXPLAIN DROP TABLE prod_catalog.gold.t", [("DROP_TABLE", "prod_catalog.gold.t")], set()),
        (
            "SELECT * FROM prod_catalog.silver.posts; DROP TABLE prod_catalog.gold.t",
            [("DROP_TABLE", "prod_catalog.gold.t")],
            {"prod_catalog.silver.posts"},
        ),
    ],
)
def test_sql_writes(sql: str, writes: list[tuple[str, str]], reads: set[str]) -> None:
    report = check(sql, "sql")
    assert [(w.kind, w.target) for w in report.writes] == writes
    assert report.reads == reads


def test_sql_write_location_is_the_statement_index() -> None:
    report = check("SELECT 1; SELECT 2; DROP TABLE prod_catalog.gold.t", "sql")
    assert report.writes[0].location.statement == 2


@pytest.mark.parametrize(
    ("sql", "rule_id"),
    [
        ("CREATE CATALOG c", "SQL_STMT_NOT_ALLOWED"),
        ("DROP CATALOG c", "SQL_STMT_NOT_ALLOWED"),
        ("CREATE SCHEMA s", "SQL_STMT_NOT_ALLOWED"),
        ("DROP SCHEMA s", "SQL_STMT_NOT_ALLOWED"),
        ("CREATE DATABASE d", "SQL_STMT_NOT_ALLOWED"),
        ("DROP DATABASE d", "SQL_STMT_NOT_ALLOWED"),
        ("USE prod_catalog.silver", "SQL_STMT_NOT_ALLOWED"),
        ("CREATE GLOBAL TEMP VIEW v AS SELECT 1", "SQL_STMT_NOT_ALLOWED"),
        ("SET spark.sql.shuffle.partitions = 1", "SQL_STMT_NOT_ALLOWED"),
        ("RESET", "SQL_STMT_NOT_ALLOWED"),
        ("ADD JAR /tmp/x.jar", "SQL_STMT_NOT_ALLOWED"),
        ("ADD FILE /tmp/x", "SQL_STMT_NOT_ALLOWED"),
        ("CREATE FUNCTION f AS 'x.Y' USING JAR '/tmp/x.jar'", "SQL_STMT_NOT_ALLOWED"),
        ("CREATE TEMPORARY FUNCTION f AS 'x.Y'", "SQL_STMT_NOT_ALLOWED"),
        ("REFRESH TABLE prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("CACHE TABLE prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("UNCACHE TABLE prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("MSCK REPAIR TABLE prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("VACUUM prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("OPTIMIZE prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("EXECUTE IMMEDIATE 'DROP TABLE prod_catalog.gold.t'", "SQL_STMT_NOT_ALLOWED"),
        ("SHOW CREATE TABLE prod_catalog.silver.posts", "SQL_STMT_NOT_ALLOWED"),
        ("EXPLAIN USE x", "SQL_STMT_NOT_ALLOWED"),
        ("BEGIN SELECT 1; END", "SQL_PARSE_ERROR"),
        ("SELECT FROM WHERE (((", "SQL_PARSE_ERROR"),
        ("SELECT 'unterminated", "SQL_PARSE_ERROR"),
        ("SELECT * FROM delta.`/prod_catalog/silver/posts`", "SQL_PATH_SOURCE"),
        ("SELECT * FROM parquet.`/tmp/x`", "SQL_PATH_SOURCE"),
        ("SELECT * FROM read_files('/tmp/x')", "SQL_PATH_SOURCE"),
        ("CREATE TABLE prod_catalog.gold.t (x INT) LOCATION '/tmp/x'", "SQL_PATH_SOURCE"),
        ("INSERT OVERWRITE DIRECTORY '/tmp/x' SELECT 1", "SQL_PATH_SOURCE"),
        ("CREATE TABLE prod_catalog.gold.t USING parquet OPTIONS (path '/tmp/x') AS SELECT 1", "SQL_PARSE_ERROR"),
        ("DESCRIBE DETAIL delta.`/tmp/x`", "SQL_PATH_SOURCE"),
        ("SHOW COLUMNS IN delta.`/tmp/x`", "SQL_PATH_SOURCE"),
        ("SHOW TABLES IN delta.`/tmp/x`", "SQL_PATH_SOURCE"),
        ("EXPLAIN SELECT * FROM delta.`/tmp/x`", "SQL_PATH_SOURCE"),
        *[(f"SELECT {f}('a', 'b')", "SQL_FUNCTION_BLOCKED") for f in sorted(BLOCKED_FUNCTIONS)],
        ("SELECT REFLECT('java.lang.Runtime', 'getRuntime')", "SQL_FUNCTION_BLOCKED"),
        ("SELECT * FROM table_changes_by_path('/tmp/x', 0)", "SQL_PATH_SOURCE"),
        ("SELECT _metadata FROM prod_catalog.silver.posts", "SQL_METADATA_COLUMN"),
        ("SELECT _metadata.file_path FROM prod_catalog.silver.posts", "SQL_METADATA_COLUMN"),
        ("SELECT p._metadata FROM prod_catalog.silver.posts p", "SQL_METADATA_COLUMN"),
        ("SELECT 1; SET a = 1", "SQL_STMT_NOT_ALLOWED"),
    ],
)
def test_sql_blocked(sql: str, rule_id: str) -> None:
    assert blocked_rule(sql, "sql") == rule_id


def test_sql_error_details() -> None:
    with pytest.raises(SandboxSecurityException) as e:
        check("SELECT 1; USE prod_catalog.silver", "sql")
    assert (e.value.language, e.value.snippet) == ("sql", "USE prod_catalog.silver")
    assert e.value.location is not None and e.value.location.statement == 1
    assert e.value.hint and "3-part" in e.value.hint


def test_blocked_functions_are_spark_functions() -> None:
    assert BLOCKED_FUNCTIONS <= set(AUDITED_FUNCTIONS.read_text().split())


EVERY_ALLOWED_IMPORT = """
from pyspark.sql import functions as G, Window
from pyspark.sql.types import StringType
import json, math, re, datetime, decimal, collections, itertools, functools, statistics, typing
import pyspark.sql.functions as H
G.col("a"); H.lit(1); T.StringType(); Window.partitionBy("a")
"""


# §5.4: allowed PySpark, with its reads, writes, and whether SQL or names are built at runtime.
@pytest.mark.parametrize(
    ("code", "reads", "writes", "dynamic"),
    [
        (
            'result = spark.table("prod_catalog.silver.posts").groupBy("x").agg(F.count("*"))',
            {"prod_catalog.silver.posts"},
            [],
            False,
        ),
        ('spark.read.table("prod_catalog.silver.posts")', {"prod_catalog.silver.posts"}, [], False),
        ('spark.sql("SELECT * FROM prod_catalog.silver.posts")', {"prod_catalog.silver.posts"}, [], False),
        ('spark.sql(sqlQuery="SELECT 1")', set(), [], False),
        (EVERY_ALLOWED_IMPORT, set(), [], False),
        ("import pyspark.sql\npyspark.sql.functions.col('a')", set(), [], False),
        ("from pyspark.sql.functions import *", set(), [], False),
        ("for _ in range(3):\n    _tmp = 1\ndef _helper(): pass", set(), [], False),
        ('row["_1"]; F.col("_c0")', set(), [], False),
        ('df.filter("x > 1").where("y = 2").selectExpr("count(*) AS n"); F.expr("upper(x)")', set(), [], False),
        ("df.filter(F.col('x') > 1)", set(), [], False),
        ('spark.conf.get("spark.sql.shuffle.partitions")', set(), [], False),
        ('json.loads("{}"); json.dumps({})', set(), [], False),
        ("@F.udf('int')\ndef plus_one(x):\n    return x + 1", set(), [], False),
        ('df.write.saveAsTable("prod_catalog.gold.t")', set(), [("SAVE_AS_TABLE", "prod_catalog.gold.t")], False),
        (
            'df.write.saveAsTable("prod_catalog.gold.t", mode="overwrite")',
            set(),
            [("SAVE_AS_TABLE", "prod_catalog.gold.t")],
            False,
        ),
        (
            'df.write.mode("append").insertInto("prod_catalog.gold.t")',
            set(),
            [("INSERT", "prod_catalog.gold.t")],
            False,
        ),
        ('df.writeTo("prod_catalog.gold.t").create()', set(), [("WRITE_TO", "prod_catalog.gold.t")], False),
        (
            'spark.sql("CREATE TABLE prod_catalog.gold.t AS SELECT * FROM prod_catalog.silver.posts")',
            {"prod_catalog.silver.posts"},
            [("CREATE_TABLE", "prod_catalog.gold.t")],
            False,
        ),
        ("df.write.saveAsTable(name)", set(), [("SAVE_AS_TABLE", None)], True),
        ('t = "x"\nspark.sql(f"DROP TABLE {t}")', set(), [], True),
        ("spark.sql(query)", set(), [], True),
        ('spark.sql("SELECT " + "1")', set(), [], True),
        ("spark.table(name)", set(), [], True),
    ],
)
def test_pyspark_allowed(code: str, reads: set[str], writes: list[tuple[str, str | None]], dynamic: bool) -> None:
    report = check(code, "pyspark")
    assert report.reads == reads
    assert [(w.kind, w.target) for w in report.writes] == writes
    assert report.has_dynamic_sql == dynamic


@pytest.mark.parametrize(
    ("code", "rule_id"),
    [
        ("import os", "PY_FORBIDDEN_IMPORT"),
        ("import subprocess as sp", "PY_FORBIDDEN_IMPORT"),
        ("import pyspark", "PY_FORBIDDEN_IMPORT"),
        ("from pyspark import SparkContext", "PY_FORBIDDEN_IMPORT"),
        ("from . import x", "PY_FORBIDDEN_IMPORT"),
        ("from pyspark.sql.types import os", "PY_FORBIDDEN_IMPORT"),
        ("from pyspark.sql import catalog", "PY_FORBIDDEN_IMPORT"),
        *[
            (f"{b}('x')", "PY_FORBIDDEN_BUILTIN")
            for b in [
                "exec",
                "eval",
                "compile",
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
            ]
        ],
        ("f = eval", "PY_FORBIDDEN_BUILTIN"),
        # underscore rules (§5.4)
        ("__import__('os')", "PY_PRIVATE_NAME"),
        ("__builtins__", "PY_PRIVATE_NAME"),
        ("x = __name__", "PY_PRIVATE_NAME"),
        ("def __f(): pass", "PY_PRIVATE_NAME"),
        ("class __C: pass", "PY_PRIVATE_NAME"),
        ("def f(__x): pass", "PY_PRIVATE_NAME"),
        ("f(__x=1)", "PY_PRIVATE_NAME"),
        ("import json as __j", "PY_PRIVATE_NAME"),
        ("from pyspark.sql.functions import _to_java_column", "PY_PRIVATE_NAME"),
        ("spark._jvm", "PY_PRIVATE_NAME"),
        ("df._jdf", "PY_PRIVATE_NAME"),
        ("x.__class__", "PY_PRIVATE_NAME"),
        ("row._1", "PY_PRIVATE_NAME"),
        # modules reached through allowed modules
        ("T.os.system('sh')", "PY_FORBIDDEN_MODULE"),
        ("F.sys.modules['os']", "PY_FORBIDDEN_MODULE"),
        ("import typing\ntyping.sys", "PY_FORBIDDEN_MODULE"),
        ("import statistics\nstatistics.random", "PY_FORBIDDEN_MODULE"),
        ("import pyspark.sql\npyspark.SparkContext", "PY_FORBIDDEN_MODULE"),
        ("m = T", "PY_FORBIDDEN_MODULE"),
        ("import json\nf(json)", "PY_FORBIDDEN_MODULE"),
        # Spark APIs
        ("spark.sparkContext", "PY_FORBIDDEN_API"),
        ("df.rdd.map(f)", "PY_FORBIDDEN_API"),
        ("x.pipe('sh')", "PY_FORBIDDEN_API"),
        ("sc.addFile('x')", "PY_FORBIDDEN_API"),
        ("sc.addPyFile('x')", "PY_FORBIDDEN_API"),
        ("spark.conf.set('a', 'b')", "PY_FORBIDDEN_API"),
        ("spark.udf.registerJavaFunction('f', 'x.Y')", "PY_FORBIDDEN_API"),
        ("spark.newSession()", "PY_FORBIDDEN_API"),
        ("SparkSession.builder.getOrCreate()", "PY_FORBIDDEN_API"),
        ("spark.catalog.listTables()", "PY_FORBIDDEN_API"),
        ("spark.dataSource.register(x)", "PY_FORBIDDEN_API"),
        ("spark.addArtifact('x')", "PY_FORBIDDEN_API"),
        ("spark.addArtifacts('x')", "PY_FORBIDDEN_API"),
        ("spark.copyFromLocalToFs('a', 'b')", "PY_FORBIDDEN_API"),
        ("df.inputFiles()", "PY_FORBIDDEN_API"),
        ("spark.readStream.table('x')", "PY_FORBIDDEN_API"),
        ("df.writeStream.start()", "PY_FORBIDDEN_API"),
        ("F.reflect(F.lit('a'), F.lit('b'))", "PY_FORBIDDEN_API"),
        ("F.java_method(F.lit('a'))", "PY_FORBIDDEN_API"),
        ("F.try_reflect(F.lit('a'))", "PY_FORBIDDEN_API"),
        ("F.input_file_name()", "PY_FORBIDDEN_API"),
        ("F.call_function('reflect', F.lit('a'))", "PY_FORBIDDEN_API"),
        ("from pyspark.sql.functions import reflect", "PY_FORBIDDEN_API"),
        # SQL expression strings and _metadata
        ("F.expr(\"reflect('a', 'b')\")", "SQL_FUNCTION_BLOCKED"),
        ("df.selectExpr('input_file_name() AS f')", "SQL_FUNCTION_BLOCKED"),
        ("df.filter('current_path() IS NOT NULL')", "SQL_FUNCTION_BLOCKED"),
        ("df.where('_metadata.file_size > 0')", "SQL_METADATA_COLUMN"),
        ("df.select('_metadata')", "PY_METADATA_COLUMN"),
        ("F.col('_metadata.file_path')", "PY_METADATA_COLUMN"),
        # paths
        ("spark.read.parquet('/prod_catalog/silver/posts')", "PY_PATH_ACCESS"),
        *[(f"spark.read.{m}('/x')", "PY_PATH_ACCESS") for m in ["load", "csv", "json", "text", "orc", "xml"]],
        ("spark.read.format('delta').load('/x')", "PY_PATH_ACCESS"),
        ("spark.read.jdbc('jdbc:x', 't')", "PY_PATH_ACCESS"),
        ("df.write.save('/x')", "PY_PATH_ACCESS"),
        ("df.write.mode('overwrite').parquet('/x')", "PY_PATH_ACCESS"),
        ("df.write.format('delta').save('/x')", "PY_PATH_ACCESS"),
        ("df.write.option('path', '/x').saveAsTable('prod_catalog.gold.t')", "PY_PATH_ACCESS"),
        ("df.write.options(path='/x').saveAsTable('prod_catalog.gold.t')", "PY_PATH_ACCESS"),
        ("df.write.saveAsTable('prod_catalog.gold.t', path='/x')", "PY_PATH_ACCESS"),
        ("df.write.options(**opts).saveAsTable('prod_catalog.gold.t')", "PY_PATH_ACCESS"),
        ("df.write.saveAsTable('prod_catalog.gold.t', **opts)", "PY_PATH_ACCESS"),
        ("spark.table('delta.`/x`')", "SQL_PATH_SOURCE"),
        ("df.write.saveAsTable('delta.`/x`')", "SQL_PATH_SOURCE"),
        # SQL inside spark.sql
        ("spark.sql('USE prod_catalog.silver')", "SQL_STMT_NOT_ALLOWED"),
        ('spark.sql("SELECT * FROM delta.`/x`")', "SQL_PATH_SOURCE"),
        # UDF bodies are code like any other
        ("@F.udf('int')\ndef f(x):\n    return eval(x)", "PY_FORBIDDEN_BUILTIN"),
        ("def broken(:", "PY_SYNTAX_ERROR"),
    ],
)
def test_pyspark_blocked(code: str, rule_id: str) -> None:
    assert blocked_rule(code, "pyspark") == rule_id


def test_pyspark_error_details() -> None:
    with pytest.raises(SandboxSecurityException) as e:
        check("x = 1\ny = row._1", "pyspark")
    assert (e.value.language, e.value.snippet, e.value.hint) == ("pyspark", "row._1", 'for a field, use row["_1"]')
    assert e.value.location is not None and (e.value.location.line, e.value.location.col) == (2, 4)


def test_sql_in_pyspark_points_at_the_call() -> None:
    report = check('x = 1\nspark.sql("SELECT 1; DROP TABLE prod_catalog.gold.t")', "pyspark")
    assert report.writes[0].location.model_dump() == {"statement": 1, "line": 2, "col": 0}
    with pytest.raises(SandboxSecurityException) as e:
        check('x = 1\nspark.sql("SELECT 1; SET a = 1")', "pyspark")
    assert e.value.language == "sql"
    assert e.value.location is not None and e.value.location.model_dump() == {"statement": 1, "line": 2, "col": 0}


def test_check_has_no_mode() -> None:
    assert list(inspect.signature(check).parameters) == ["code", "language"]
