"""Builds a sandbox from the prod catalog (§4.2). Runs in the jobs container:

    python -m dbrix.sampler.sample <manifest> <agent_id> <session_id> <SamplingConfig json>

The last stdout line is `{"seed": ..., "row_counts": {...}}`. Every random choice is a hash of
(seed, table, salt, primary key), never rand(), so the sandbox does not depend on partitioning.
"""

import json
import operator
import sys
from collections.abc import Mapping
from functools import reduce
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dbrix.config import PROD_CATALOG_MOUNT, SANDBOX_MOUNT, SamplingConfig
from dbrix.integrity import check_integrity, fk_keys
from dbrix.manifest import Anchor, ForeignKey, Manifest, anchor_of, load_manifest
from dbrix.spark import create_schemas, create_session, table_path, unit, write_gold


def is_null(fk: ForeignKey) -> Column:
    """A composite FK is null when any of its columns is, as in check_integrity."""
    return reduce(operator.or_, [F.col(c).isNull() for c in fk.columns])


def union(dfs: list[DataFrame], pk: list[str]) -> DataFrame:
    """Rows of all `dfs`, once per PK, materialized so later joins don't recompute the whole lineage."""
    return reduce(DataFrame.unionByName, dfs).dropDuplicates(pk).localCheckpoint()


def up_pass(selected: dict[str, DataFrame], prod: Mapping[str, DataFrame], manifest: Manifest) -> None:
    """Add every missing parent of every selected row (step 4). Children first, so one walk is enough on a DAG.
    A table is checked when the walk reaches it, so parents added earlier in the walk are included."""
    for name in reversed(manifest.generation_order()):
        if name not in selected:
            continue
        for fk in manifest.tables[name].foreign_keys:
            refs = selected[name].select(*[F.col(c).alias(r) for c, r in zip(fk.columns, fk.ref_columns)])
            parents = prod[fk.ref_table].join(refs, fk.ref_columns, "left_semi")
            if fk.ref_table in selected:
                parents = parents.unionByName(selected[fk.ref_table])
            selected[fk.ref_table] = union([parents], manifest.tables[fk.ref_table].primary_key)


def select(
    prod: Mapping[str, DataFrame],
    manifest: Manifest,
    anchor: Anchor,
    sample_size: int,
    fraction: float,
    seed: int,
) -> dict[str, DataFrame]:
    """The sandbox rows of every non-Gold table (§4.2 steps 1-5)."""
    order = manifest.generation_order()

    def sample(name: str, df: DataFrame, salt: str) -> DataFrame:
        h = F.xxhash64(F.lit(seed), F.lit(name), F.lit(salt), *manifest.tables[name].primary_key)
        return df.where(unit(h) < fraction)

    # 1. Seed: the `sample_size` anchor keys with the smallest hash.
    keys = (
        prod[anchor.table]
        .where(F.col(anchor.key).isNotNull())
        .select(anchor.key)
        .distinct()
        .orderBy(F.xxhash64(F.lit(seed), F.lit(anchor.table), anchor.key), anchor.key)
        .limit(sample_size)
    )
    selected = {
        anchor.table: union(
            [prod[anchor.table].join(keys, anchor.key, "left_semi")], manifest.tables[anchor.table].primary_key
        )
    }
    if (found := selected[anchor.table].select(anchor.key).distinct().count()) < sample_size:
        print(
            f"warning: {anchor.table} has {found} {anchor.key} values, fewer than {sample_size}; took all",
            file=sys.stderr,
        )

    # 2-3. Down pass, plus a sample of the rows whose reaching FK is null.
    for name in order:
        fks = [fk for fk in manifest.tables[name].foreign_keys if fk.ref_table in selected]
        if name in selected or not fks:
            continue
        rows = prod[name]
        children = [rows.join(fk_keys(selected[fk.ref_table], fk), fk.columns, "left_semi") for fk in fks]
        null_fk = sample(name, rows.where(reduce(operator.or_, map(is_null, fks))), "#null")
        selected[name] = union([*children, null_fk], manifest.tables[name].primary_key)

    # 4. Up pass.
    up_pass(selected, prod, manifest)

    # 5. Fill and islands, parents first, so every parent of `name` is already in `selected`.
    for name in order:
        if name in selected:
            continue
        t = manifest.tables[name]
        if any(not selected[fk.ref_table].isEmpty() for fk in t.foreign_keys):
            # Fill: every non-null FK points at a selected row, and at least one FK is non-null.
            rows = prod[name].where(~reduce(operator.and_, map(is_null, t.foreign_keys)))
            for fk in t.foreign_keys:
                hits = fk_keys(selected[fk.ref_table], fk).withColumn("_hit", F.lit(True))
                rows = rows.join(hits, fk.columns, "left").where(is_null(fk) | F.col("_hit")).drop("_hit")
            selected[name] = union([rows], t.primary_key)
        else:
            # Island: a sample, plus its parents.
            selected[name] = union([sample(name, prod[name], "#island")], t.primary_key)
            up_pass(selected, prod, manifest)
    return selected


def main(manifest_path: Path, agent_id: str, session_id: str, sampling: SamplingConfig) -> None:
    manifest = load_manifest(manifest_path)
    spark = create_session(SANDBOX_MOUNT)
    seed = sampling.seed
    if seed is None:
        seed = int(spark.range(1).select(F.xxhash64(F.lit(agent_id), F.lit(session_id))).collect()[0][0])
    order = manifest.generation_order()
    prod = {n: spark.read.format("delta").load(table_path(PROD_CATALOG_MOUNT, n)) for n in order}
    selected = select(
        prod, manifest, anchor_of(manifest, sampling), sampling.sample_size, sampling.unrelated_sample_fraction, seed
    )

    create_schemas(spark, manifest, SANDBOX_MOUNT)
    for name in order:
        selected[name].write.format("delta").saveAsTable(name)
        print(f"sampled {name}", flush=True)
    write_gold(spark, manifest)
    tables = {n: spark.table(n) for n in manifest.tables}
    check_integrity(tables, manifest)
    row_counts = {n: df.count() for n, df in tables.items()}
    spark.stop()
    print(json.dumps({"seed": seed, "row_counts": row_counts}), flush=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]), sys.argv[2], sys.argv[3], SamplingConfig.model_validate_json(sys.argv[4]))
