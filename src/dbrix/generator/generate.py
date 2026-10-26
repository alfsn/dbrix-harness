"""Builds prod_catalog from the manifest with Spark (§3.3). Runs in the jobs container:

    python -m dbrix.generator.generate <manifest> <catalog_dir> <seed> <scale>

Every random value is a hash of (seed, table, column, row key), never rand(), so the output does not
depend on partitioning. A row key `_rk` is the range id, or a hash of the parent's PK and the child index `_k`.
"""

import calendar
import sys
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dbrix.config import CATALOG_META_FILE
from dbrix.integrity import check_integrity
from dbrix.manifest import (
    BetweenGen,
    CatalogMeta,
    ChoiceGen,
    Column,
    ForeignKey,
    Manifest,
    ParentGen,
    RangeGen,
    SqlGen,
    Table,
    load_manifest,
    manifest_hash,
)
from dbrix.spark import create_schemas, create_session, unit, write_gold

ADJECTIVES = [
    "Bright",
    "Bold",
    "Urban",
    "Pure",
    "Golden",
    "Wild",
    "Silver",
    "Blue",
    "Green",
    "Nova",
    "Prime",
    "Lunar",
    "Solar",
    "Swift",
    "True",
    "Happy",
    "Velvet",
    "Iron",
    "Coral",
    "Crystal",
]
NOUNS = [
    "Labs",
    "Co",
    "Studio",
    "Works",
    "Goods",
    "Beauty",
    "Games",
    "Foods",
    "Wear",
    "Travel",
    "Tech",
    "Collective",
    "Brands",
    "House",
    "Supply",
    "Market",
    "Kitchen",
    "Gear",
    "Style",
    "Media",
]
FIRST_NAMES = [
    "Ava",
    "Liam",
    "Mia",
    "Noah",
    "Zoe",
    "Lucas",
    "Emma",
    "Mateo",
    "Sofia",
    "Leo",
    "Yuki",
    "Aria",
    "Kai",
    "Nina",
    "Omar",
    "Lea",
    "Ivan",
    "Sara",
    "Diego",
    "Maya",
]
LAST_NAMES = [
    "Smith",
    "Garcia",
    "Kim",
    "Silva",
    "Rossi",
    "Muller",
    "Martin",
    "Lopez",
    "Tanaka",
    "Brown",
    "Khan",
    "Novak",
    "Costa",
    "Dubois",
    "Park",
    "Jones",
    "Rivera",
    "Sato",
    "Fischer",
    "Moreau",
]
WORDS = [
    "new",
    "drop",
    "today",
    "love",
    "this",
    "look",
    "summer",
    "glow",
    "routine",
    "favorite",
    "launch",
    "try",
    "best",
    "week",
    "vibes",
    "haul",
    "review",
    "honest",
    "link",
    "bio",
    "collab",
    "giveaway",
    "morning",
    "tips",
    "travel",
    "recipe",
    "setup",
    "game",
    "style",
    "fresh",
]

Hash = Callable[..., SparkColumn]


def word(words: list[str], h: SparkColumn) -> SparkColumn:
    return F.element_at(F.array(*map(F.lit, words)), (F.pmod(h, F.lit(len(words))) + 1).cast("int"))


def uuid(h: SparkColumn, h2: SparkColumn) -> SparkColumn:
    x = F.lower(F.concat(F.lpad(F.hex(h), 16, "0"), F.lpad(F.hex(h2), 16, "0")))
    return F.concat_ws("-", *[F.substring(x, pos, n) for pos, n in [(1, 8), (9, 4), (13, 4), (17, 4), (21, 12)]])


def epoch(d: object) -> int:
    return calendar.timegm(d.timetuple())  # type: ignore[attr-defined]


def column_expr(name: str, column: Column, h: Hash) -> SparkColumn:
    """The column's value from its gen, before nulls and cast. `h(salt)` hashes the row key."""
    gen = column.gen
    if gen == "uuid":
        return uuid(h(name), h(name + "#2"))
    if gen == "company":
        return F.concat_ws(" ", word(ADJECTIVES, h(name)), word(NOUNS, h(name + "#2")))
    if gen == "person_name":
        return F.concat_ws(" ", word(FIRST_NAMES, h(name)), word(LAST_NAMES, h(name + "#2")))
    if gen == "text":
        n_words = (F.pmod(h(name), F.lit(6)) + 3).cast("int")
        return F.concat_ws(" ", F.transform(F.sequence(F.lit(1), n_words), lambda i: word(WORDS, h(name, i))))
    if isinstance(gen, RangeGen):
        lo, hi = gen.range
        return F.lit(lo) + F.pow(unit(h(name)), gen.skew) * (hi - lo)
    if isinstance(gen, ChoiceGen):
        weights = gen.weights or [1.0] * len(gen.choice)
        u, total, acc = unit(h(name)), sum(weights), 0.0
        expr = F.when(F.lit(False), None)
        for value, weight in zip(gen.choice[:-1], weights):
            acc += weight / total
            expr = expr.when(u < acc, F.lit(value))
        return expr.otherwise(F.lit(gen.choice[-1]))
    if isinstance(gen, BetweenGen):
        lo, hi = map(epoch, gen.between)
        return F.timestamp_seconds(F.lit(lo) + F.floor(unit(h(name)) * (hi - lo)))
    if isinstance(gen, ParentGen):
        return F.col(f"parent.{gen.parent}")
    if isinstance(gen, SqlGen):
        return F.expr(gen.sql)
    raise ValueError(f"unknown gen {gen!r}")


def finish(expr: SparkColumn, name: str, column: Column, h: Hash) -> SparkColumn:
    """Apply null_fraction, then cast to the column type."""
    if column.null_fraction:
        expr = F.when(unit(h(name + "#null")) < column.null_fraction, None).otherwise(expr)
    return expr.cast(column.type)


def base_rows(spark: SparkSession, manifest: Manifest, name: str, seed: int, scale: float) -> DataFrame:
    """One row per output row: `_rk`, plus `parent` (struct) and `_k` for rows_per_parent tables."""
    t = manifest.tables[name]
    if t.rows_per_parent is None:
        return spark.range(round(t.rows * scale)).select(F.col("id").alias("_rk"))  # type: ignore[operator]
    rpp = t.rows_per_parent
    parent = spark.table(rpp.parent)
    parent_key = F.xxhash64(*manifest.tables[rpp.parent].primary_key)
    count = F.lit(rpp.min) + F.floor(
        unit(F.xxhash64(F.lit(seed), F.lit(name), F.lit("#count"), parent_key)) * (rpp.max - rpp.min + 1)
    )
    return (
        parent.select(F.struct(*parent.columns).alias("parent"), parent_key.alias("_pk"), count.cast("int").alias("_n"))
        .select("parent", "_pk", F.posexplode(F.array_repeat(F.lit(0), F.col("_n"))).alias("_k", "_zero"))
        .select("parent", "_k", F.xxhash64("_pk", "_k").alias("_rk"))
    )


def pick_parent(df: DataFrame, spark: SparkSession, fk: ForeignKey, h: Hash) -> DataFrame:
    """Fill the FK columns from a hash-picked row of the referenced table (within the `match` group)."""
    groups = [f"_m{i}" for i in range(max(len(fk.match), 1))]
    ref_keys = [F.col(c) for c in fk.match] or [F.lit(0)]
    child_keys = [F.expr(e) for e in fk.match.values()] or [F.lit(0)]
    by_group = Window.partitionBy(*groups)
    indexed = (
        spark.table(fk.ref_table)
        .select(
            *[k.alias(g) for k, g in zip(ref_keys, groups)],
            *[F.col(r).alias(c) for c, r in zip(fk.columns, fk.ref_columns)],
        )
        .withColumn("_idx", F.row_number().over(by_group.orderBy(*fk.columns)) - 1)
        .withColumn("_size", F.count(F.lit(1)).over(by_group))
    )
    sizes = indexed.select(*groups, "_size").distinct()
    salt = "#fk:" + ",".join(fk.columns)
    return (
        df.select("*", *[k.alias(g) for k, g in zip(child_keys, groups)])
        .join(F.broadcast(sizes), groups, "left")
        .withColumn("_idx", F.floor(F.pow(unit(h(salt)), fk.skew) * F.col("_size")).cast("int"))
        .join(F.broadcast(indexed.drop("_size")), [*groups, "_idx"], "left")
        .drop(*groups, "_idx", "_size")
    )


def generate_table(spark: SparkSession, manifest: Manifest, name: str, seed: int, scale: float) -> DataFrame:
    t: Table = manifest.tables[name]

    def h(salt: str, *extra: SparkColumn) -> SparkColumn:
        return F.xxhash64(F.lit(seed), F.lit(name), F.lit(salt), F.col("_rk"), *extra)

    df = base_rows(spark, manifest, name, seed, scale)
    # Order: plain gens, then FK picks (may match on them), then SQL gens (may use everything before).
    for c, col in t.columns.items():
        if col.gen is not None and not isinstance(col.gen, SqlGen):
            df = df.withColumn(c, finish(column_expr(c, col, h), c, col, h))
    for fk in t.foreign_keys:
        if t.columns[fk.columns[0]].gen is None:
            df = pick_parent(df, spark, fk, h)
            for c in fk.columns:
                df = df.withColumn(c, finish(F.col(c), c, t.columns[c], h))
    for c, col in t.columns.items():
        if isinstance(col.gen, SqlGen):
            df = df.withColumn(c, finish(column_expr(c, col, h), c, col, h))

    if not any(t.columns[c].gen == "uuid" for c in t.primary_key):
        # picks can repeat a PK; keep the row with the smallest row key
        df = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*t.primary_key).orderBy("_rk"))).where(
            "_rn = 1"
        )
    if t.duplicate_fraction:
        # Bronze duplicates: same content under a new event id
        pk = t.primary_key[0]
        dups = df.where(unit(h("#dup")) < t.duplicate_fraction).withColumn("_rk", h("#dup"))
        df = df.unionByName(dups.withColumn(pk, uuid(h(pk), h(pk + "#2"))))
    return df.select(*t.columns)


def generate(spark: SparkSession, manifest: Manifest, catalog_dir: str, seed: int, scale: float) -> None:
    create_schemas(spark, manifest, catalog_dir)
    for name in manifest.generation_order():
        generate_table(spark, manifest, name, seed, scale).write.format("delta").saveAsTable(name)
        print(f"generated {name}", flush=True)
    write_gold(spark, manifest)


def main(manifest_path: Path, catalog_dir: str, seed: int, scale: float) -> None:
    manifest = load_manifest(manifest_path)
    spark = create_session(catalog_dir)
    generate(spark, manifest, catalog_dir, seed, scale)
    check_integrity({n: spark.table(n) for n in manifest.tables}, manifest)
    meta = CatalogMeta(
        manifest_hash=manifest_hash(manifest_path),
        seed=seed,
        scale=scale,
        table_versions={n: spark.sql(f"DESCRIBE HISTORY {n} LIMIT 1").collect()[0]["version"] for n in manifest.tables},
    )
    Path(catalog_dir, CATALOG_META_FILE).write_text(meta.model_dump_json(indent=2))
    spark.stop()


if __name__ == "__main__":
    main(Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), float(sys.argv[4]))
