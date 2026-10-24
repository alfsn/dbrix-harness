"""FK and PK integrity check, shared by the generator and the sampler (§3.3, §4.2 step 7)."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dbrix.manifest import Manifest


class IntegrityError(Exception):
    pass


def check_integrity(spark: SparkSession, manifest: Manifest) -> None:
    """Raise IntegrityError when a PK is duplicated or a non-null FK does not resolve."""
    errors = []
    for name, t in manifest.tables.items():
        df = spark.table(name)
        duplicates = df.groupBy(*t.primary_key).count().where("count > 1").count()
        if duplicates:
            errors.append(f"{name}: {duplicates} duplicated primary keys")
        for fk in t.foreign_keys:
            parent = spark.table(fk.ref_table).select(*[F.col(r).alias(c) for c, r in zip(fk.columns, fk.ref_columns)])
            dangling = df.dropna(subset=fk.columns).join(parent, fk.columns, "left_anti").count()
            if dangling:
                errors.append(f"{name}: {dangling} rows with FK {fk.columns} not found in {fk.ref_table}")
    if errors:
        raise IntegrityError("\n".join(errors))
