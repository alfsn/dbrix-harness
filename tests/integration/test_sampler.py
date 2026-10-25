import time
from pathlib import Path
from typing import Any

import docker
import pytest

from dbrix.config import SamplingConfig
from dbrix.sampler import SampleResult, run_sampler

from .conftest import CONFIG, REPO, TABLES, spark_json

pytestmark = pytest.mark.docker


def sample(client: docker.DockerClient, prod: Path, sandbox: Path, session_id: str = "s1") -> SampleResult:
    return run_sampler(
        client, CONFIG.executor.image, prod, sandbox, CONFIG.manifest, "agent", session_id, SamplingConfig()
    )


# A small manifest with the cases the default domain never hits. Only keys matter to `select`.
#   brands <- campaigns <- posts (campaign_id nullable) -> accounts <- follows (two FKs to accounts)
#   campaigns, accounts <- contracts <- payments (composite FK, may be partly null)
#   agencies <- agency_staff        (unreachable from brands: an island and its child)
EDGE_CASES = """
import json
from dbrix.integrity import check_integrity
from dbrix.manifest import Anchor, Manifest
from dbrix.sampler.sample import select

def fk(cols, ref):
    return {"columns": cols, "references": ref}

TABLES = {
    "silver.brands": ([], "brand_id string", [("B1",)]),
    "silver.campaigns": ([fk(["brand_id"], "silver.brands(brand_id)")], "campaign_id string, brand_id string",
                         [("C1", "B1")]),
    "silver.accounts": ([], "account_id string", [("X",), ("Y",), ("Z",)]),
    "silver.posts": ([fk(["campaign_id"], "silver.campaigns(campaign_id)"),
                      fk(["account_id"], "silver.accounts(account_id)")],
                     "post_id string, campaign_id string, account_id string",
                     [("P1", "C1", "X"), ("P2", "C1", "Y"), ("P3", None, "Z"), ("P4", None, "Z")]),
    "silver.follows": ([fk(["follower_id"], "silver.accounts(account_id)"),
                        fk(["followed_id"], "silver.accounts(account_id)")],
                       "follower_id string, followed_id string", [("X", "Y"), ("X", "Z"), ("Z", "X")]),
    "silver.contracts": ([fk(["campaign_id"], "silver.campaigns(campaign_id)"),
                          fk(["account_id"], "silver.accounts(account_id)")],
                         "campaign_id string, account_id string", [("C1", "X"), ("C1", "Y")]),
    "silver.payments": ([fk(["campaign_id", "account_id"], "silver.contracts(campaign_id, account_id)")],
                        "payment_id string, campaign_id string, account_id string",
                        [("PAY1", "C1", "X"), ("PAY2", "C1", "Y"), ("PAY3", "C1", None)]),
    "silver.agencies": ([], "agency_id string", [(f"A{i}",) for i in range(10)]),
    "silver.agency_staff": ([fk(["agency_id"], "silver.agencies(agency_id)")], "staff_id string, agency_id string",
                            [(f"S{i}{j}", f"A{i}") for i in range(10) for j in range(2)]),
}
PK = {"silver.follows": ["follower_id", "followed_id"], "silver.contracts": ["campaign_id", "account_id"]}
manifest = Manifest.model_validate({
    "catalog": "prod_catalog",
    "default_anchor": {"table": "silver.brands", "key": "brand_id"},
    "tables": {
        name: {"layer": "silver", "primary_key": PK.get(name, [schema.split()[0]]), "foreign_keys": fks}
        for name, (fks, schema, _) in TABLES.items()
    },
})
prod = {name: spark.createDataFrame(rows, schema) for name, (_, schema, rows) in TABLES.items()}

def run(anchor, sample_size, fraction):
    selected = select(prod, manifest, Anchor(table=anchor[0], key=anchor[1]), sample_size, fraction, seed=7)
    check_integrity(selected, manifest)
    return {
        name: sorted("|".join(str(v) for v in row) for row in df.select(*manifest.tables[name].primary_key).collect())
        for name, df in selected.items()
    }

print(json.dumps({
    "none": run(("silver.brands", "brand_id"), 5, 0.0),
    "all": run(("silver.brands", "brand_id"), 1, 1.0),
    "half": run(("silver.contracts", "account_id"), 1, 0.5),
}))
"""


@pytest.fixture(scope="module")
def edge(client: docker.DockerClient) -> dict[str, dict[str, list[str]]]:
    result: dict[str, dict[str, list[str]]] = spark_json(client, EDGE_CASES, {})
    return result


def test_edge_no_random_rows(edge: dict[str, Any]) -> None:
    """Fraction 0: only rows linked to the anchor. sample_size 5 with one brand takes it (and warns)."""
    assert edge["none"] == {
        "silver.brands": ["B1"],
        "silver.campaigns": ["C1"],
        "silver.posts": ["P1", "P2"],  # no organic posts
        "silver.accounts": ["X", "Y"],  # Z only comes with organic posts
        "silver.follows": ["X|Y"],  # X->Z is left out: Z is not in the sandbox
        "silver.contracts": ["C1|X", "C1|Y"],
        "silver.payments": ["PAY1", "PAY2"],  # PAY3's composite FK is partly null
        "silver.agencies": [],
        "silver.agency_staff": [],
    }


def test_edge_all_random_rows(edge: dict[str, Any]) -> None:
    """Fraction 1: organic posts bring Z up, so its follows are filled; the island is taken whole."""
    s = edge["all"]
    assert s["silver.posts"] == ["P1", "P2", "P3", "P4"]
    assert s["silver.accounts"] == ["X", "Y", "Z"]
    assert s["silver.follows"] == ["X|Y", "X|Z", "Z|X"]
    assert s["silver.payments"] == ["PAY1", "PAY2", "PAY3"]
    assert len(s["silver.agencies"]) == 10 and len(s["silver.agency_staff"]) == 20


def test_edge_anchor_with_parents_and_island_children(edge: dict[str, Any]) -> None:
    """Anchor on one column of a composite PK: its parents come up. The island's children follow the island."""
    s = edge["half"]
    accounts = {c.split("|")[1] for c in s["silver.contracts"]}
    assert len(accounts) == 1 and len(s["silver.contracts"]) == 1
    assert s["silver.campaigns"] == ["C1"] and s["silver.brands"] == ["B1"]
    assert accounts <= set(s["silver.accounts"])
    agencies = s["silver.agencies"]
    assert 0 < len(agencies) < 10
    assert s["silver.agency_staff"] == sorted(f"S{a[1:]}{j}" for a in agencies for j in range(2))


# For 5 seeds on the default domain, in one container: integrity (raises), and the §11.1 properties.
# Determinism is checked here too (seed 0 selected twice), so no second sandbox build is needed.
SEEDS = """
import json
from pathlib import Path
from pyspark.sql import functions as F
from dbrix.integrity import check_integrity
from dbrix.manifest import load_manifest
from dbrix.sampler.sample import select
from dbrix.spark import table_path

m = load_manifest(Path("/repo/manifests/influencer_marketing.yaml"))
prod = {n: spark.read.format("delta").load(table_path("/prod", n)) for n in m.generation_order()}

def rows_hash(s):
    # order-independent content hash per table: sum of row hashes
    return {n: str(df.select(F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)"))).first()[0]) for n, df in s.items()}

def props(seed):
    s = select(prod, m, m.default_anchor, 5, 0.05, seed)
    check_integrity(s, m)
    posts = s["silver.posts"]
    organic = posts.where("campaign_id IS NULL")
    return {
        "rows": rows_hash(s),
        "brands": s["silver.brands"].count(),
        "campaign_posts": posts.where("campaign_id IS NOT NULL").count(),
        "posts_of_selected_campaigns": prod["silver.posts"].join(s["silver.campaigns"], "campaign_id", "left_semi").count(),
        "organic_posts": organic.count(),
        "organic_metrics": s["silver.post_metrics_daily"].join(organic, "post_id", "left_semi").count(),
        "profiles": s["bronze.raw_creator_profiles"].count(),
        "profiles_of_selected_creators":
            prod["bronze.raw_creator_profiles"].join(s["silver.creators"], "creator_id", "left_semi").count(),
    }

print(json.dumps({
    "seeds": [props(seed) for seed in range(5)],
    "seed_0_again": rows_hash(select(prod, m, m.default_anchor, 5, 0.05, 0)),
}))
"""


def test_five_seeds(client: docker.DockerClient, prod_small: Path) -> None:
    result = spark_json(client, SEEDS, {"/prod": prod_small, "/repo/manifests": REPO / "manifests"})
    seeds = result["seeds"]
    assert seeds[0]["rows"] == result["seed_0_again"]  # the same seed gives the same rows
    assert len({p["rows"]["silver.brands"] for p in seeds}) == 5  # other seeds give other brands
    for p in seeds:
        assert p["brands"] == 5  # the up pass added no brand, so no campaign of another brand came in
        assert p["campaign_posts"] == p["posts_of_selected_campaigns"]  # nothing more, nothing less
        assert p["organic_posts"] > 0 and p["organic_metrics"] > 0
        assert p["profiles"] == p["profiles_of_selected_creators"] > 0


SANDBOX_CHECKS = """
import json
from pyspark.sql import functions as F
from dbrix.manifest import load_manifest
from dbrix.rewrite import rewrite_sql
from pathlib import Path
m = load_manifest(Path("/repo/manifests/influencer_marketing.yaml"))
def gold_diff(name, t):
    expected, actual = spark.sql(rewrite_sql(t.transform, m.catalog, lambda _: False)), spark.table(name)
    return expected.exceptAll(actual).count() + actual.exceptAll(expected).count()
print(json.dumps({
    "seed_of_ids": spark.range(1).select(F.xxhash64(F.lit("agent"), F.lit("s1"))).first()[0],
    "row_counts": {n: spark.table(n).count() for n in m.tables},
    "gold_diff": {n: gold_diff(n, t) for n, t in m.tables.items() if t.transform},
}))
"""


def test_sandbox_end_to_end(client: docker.DockerClient, prod_small: Path, tmp_path: Path) -> None:
    """One real build. Determinism of the rows is checked in memory by test_five_seeds."""
    result = sample(client, prod_small, tmp_path / ".creating-a")

    # The meta block is complete.
    assert result.sampling.anchor_table == "silver.brands" and result.sampling.sample_size == 5
    assert result.sampling.prod_table_versions and set(result.row_counts) == set(TABLES)

    # After a rename, a new Spark session opens the sandbox metastore at /sandbox and Gold matches its transform.
    (tmp_path / ".creating-a").rename(tmp_path / "agent_agent_s1")
    mounts = {"/sandbox": tmp_path / "agent_agent_s1", "/repo/manifests": REPO / "manifests"}
    checks = spark_json(client, SANDBOX_CHECKS, mounts, catalog_dir="/sandbox")
    assert checks["seed_of_ids"] == result.sampling.seed  # the seed comes from the ids, so a rebuild matches
    assert checks["row_counts"] == result.row_counts
    assert set(checks["gold_diff"].values()) == {0}


@pytest.mark.slow
def test_scale_1_sandbox_speed(client: docker.DockerClient, prod_scale_1: tuple[Path, float], tmp_path: Path) -> None:
    """Guards against slowdowns. About 120 s measured; the §11.4 (2) target of 30 s is not met yet (§4.5)."""
    start = time.monotonic()
    sample(client, prod_scale_1[0], tmp_path / "sandbox")
    elapsed = time.monotonic() - start
    print(f"sampler at scale 1: {elapsed:.1f} s")
    assert elapsed < 150
