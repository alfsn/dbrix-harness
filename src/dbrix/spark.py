"""Spark in dbrix-spark containers: the session (Delta plus a Derby metastore in the catalog dir) and the
catalog steps shared by the generator and the sampler."""

from glob import glob

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from dbrix.config import DELTA_JARS, METASTORE_DIR, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS
from dbrix.manifest import Manifest
from dbrix.rewrite import rewrite_sql


def create_session(catalog_dir: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("dbrix")
        .config("spark.jars", ",".join(sorted(glob(DELTA_JARS))))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.sources.default", "delta")  # as on Databricks
        .config("spark.sql.warehouse.dir", catalog_dir)
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={catalog_dir}/{METASTORE_DIR};create=true",
        )
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def unit(h: Column) -> Column:
    """A 64-bit hash mapped onto [0, 1)."""
    return F.shiftrightunsigned(h, 11) / float(2**53)


def table_path(catalog_dir: str, name: str) -> str:
    """Where `<schema>.<table>` is stored in a catalog dir (§3.3, §4.3)."""
    return f"{catalog_dir}/{name.replace('.', '/')}"


def create_schemas(spark: SparkSession, manifest: Manifest, catalog_dir: str) -> None:
    """One database per manifest schema, stored at `<catalog_dir>/<schema>` (§3.3, §4.3)."""
    for schema in sorted({name.split(".")[0] for name in manifest.tables}):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '{catalog_dir}/{schema}'")


def write_gold(spark: SparkSession, manifest: Manifest) -> None:
    """Run each Gold transform over the Bronze and Silver tables in the metastore and save the result."""
    for name, t in manifest.tables.items():
        if t.transform:
            sql = rewrite_sql(t.transform, manifest.catalog, lambda _: False)
            spark.sql(sql).write.format("delta").saveAsTable(name)
            print(f"computed {name}", flush=True)
