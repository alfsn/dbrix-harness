"""The SparkSession used in dbrix-spark containers: Delta plus a Derby metastore in the catalog dir."""

from glob import glob

from pyspark.sql import SparkSession

from dbrix.config import DELTA_JARS, METASTORE_DIR, SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS


def create_session(catalog_dir: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("dbrix")
        .config("spark.jars", ",".join(sorted(glob(DELTA_JARS))))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
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
