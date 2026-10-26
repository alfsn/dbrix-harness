"""The §11.3 red-team cases, as data. The policy must block or flag each one (test_policy_blocks.py); with the
policy disabled, the container must still contain it (tests/integration/test_containment.py)."""

from dbrix.policy.report import Language

# Blocked by the policy in every mode.
BLOCKED: list[tuple[Language, str]] = [
    ("pyspark", 'getattr(df, "wr" + "ite")'),
    ("pyspark", '__import__("os")'),
    ("pyspark", "().__class__.__bases__[0].__subclasses__()"),
    ("pyspark", 'exec(base64.b64decode("cHJpbnQoMSk="))'),
    ("pyspark", "spark.sparkContext._jvm"),
    ("pyspark", "spark._jsparkSession"),
    ("pyspark", 'df.rdd.pipe("sh")'),
    ("pyspark", 'T.os.system("sh")'),
    ("pyspark", 'F.sys.modules["os"].system("sh")'),
    ("pyspark", 'df.select(F.reflect(F.lit("java.lang.Runtime"), F.lit("getRuntime")))'),
    ("pyspark", 'df.select(F.call_function("reflect", F.lit("java.lang.Runtime"), F.lit("getRuntime")))'),
    ("pyspark", "df.select(F.expr(\"reflect('java.lang.Runtime', 'getRuntime')\"))"),
    ("pyspark", 'df.write.option("path", "/prod_catalog/silver/posts").saveAsTable("prod_catalog.gold.t")'),
    ("pyspark", 'df.write.saveAsTable("prod_catalog.gold.t", path="/prod_catalog/silver/posts")'),
    ("pyspark", 'df.write.options(**{"path": "/prod_catalog/silver/posts"}).saveAsTable("prod_catalog.gold.t")'),
    ("pyspark", "spark.catalog.listTables()"),
    ("pyspark", 'spark.read.parquet("/prod_catalog/silver/posts")'),
    ("pyspark", 'import socket\nsocket.create_connection(("example.com", 80))'),
    ("sql", "SELECT * FROM delta.`/prod_catalog/silver/posts`"),
    ("sql", "SELECT reflect('java.lang.Runtime', 'getRuntime')"),
    ("sql", "EXECUTE IMMEDIATE 'DROP TABLE prod_catalog.gold.t'"),
    ("sql", "ADD JAR /tmp/x.jar"),
    ("sql", "SET spark.sql.warehouse.dir = /prod_catalog"),
    ("sql", "CREATE FUNCTION f AS 'x.Y' USING JAR '/tmp/x.jar'"),
]

# Allowed statically but flagged as dynamic: the runtime wrapper blocks them when `allow_writes` is false.
DYNAMIC: list[str] = [
    't = "prod_catalog.gold.t"\nspark.sql(f"DROP TABLE {t}")',
    'name = "prod_catalog.gold." + "t"\ndf.write.saveAsTable(name)',
]
