"""FK and PK integrity check, shared by the generator and the sampler (§3.3, §4.2 step 7)."""

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dbrix.manifest import ForeignKey, Manifest


class IntegrityError(Exception):
    pass


def fk_keys(parent: DataFrame, fk: ForeignKey) -> DataFrame:
    """The parent's referenced columns, renamed to the child's FK columns, ready to join on `fk.columns`."""
    return parent.select(*[F.col(r).alias(c) for c, r in zip(fk.columns, fk.ref_columns)])


def check_integrity(tables: Mapping[str, DataFrame], manifest: Manifest) -> None:
    """Raise IntegrityError when a PK is duplicated or a non-null FK does not resolve, in the given tables."""
    errors = []
    for name, df in tables.items():
        t = manifest.tables[name]
        duplicates = df.groupBy(*t.primary_key).count().where("count > 1").count()
        if duplicates:
            errors.append(f"{name}: {duplicates} duplicated primary keys")
        for fk in t.foreign_keys:
            dangling = df.dropna(subset=fk.columns).join(fk_keys(tables[fk.ref_table], fk), fk.columns, "left_anti")
            if n := dangling.count():
                errors.append(f"{name}: {n} rows with FK {fk.columns} not found in {fk.ref_table}")
    if errors:
        raise IntegrityError("\n".join(errors))
