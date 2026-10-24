import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from dbrix.config import Config
from dbrix.manifest import Manifest, ManifestError, load_manifest, validate

RAW: dict[str, Any] = yaml.safe_load(Config().manifest.read_text())


def errors_after(change: Callable[[dict[str, Any]], None]) -> str:
    raw = copy.deepcopy(RAW)
    change(raw)
    return "\n".join(validate(Manifest.model_validate(raw)))


def test_default_manifest_is_valid() -> None:
    assert validate(load_manifest(Config().manifest)) == []


def fk(raw: dict[str, Any], table: str, i: int = 0) -> dict[str, Any]:
    fks: list[dict[str, Any]] = raw["tables"][table]["foreign_keys"]
    return fks[i]


def set_(path: list[str], value: Any) -> Callable[[dict[str, Any]], None]:
    def change(raw: dict[str, Any]) -> None:
        node = raw
        for key in path[:-1]:
            node = node[key]
        node[path[-1]] = value

    return change


def add_table(name: str, table: dict[str, Any]) -> Callable[[dict[str, Any]], None]:
    return set_(["tables", name], table)


COMPOSITE_CHILD = {
    "layer": "silver",
    "primary_key": ["id"],
    "rows": 1,
    "columns": {
        "id": {"type": "string", "gen": "uuid"},
        "campaign_id": {"type": "string"},
        "creator_id": {"type": "string"},
    },
    "foreign_keys": [
        {"columns": ["campaign_id", "creator_id"], "references": "silver.campaign_creators(campaign_id, creator_id)"}
    ],
}


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda r: fk(r, "silver.campaigns").update(references="silver.nope(brand_id)"),
            "'silver.nope' is not a non-Gold table",
        ),
        (
            lambda r: fk(r, "silver.campaigns").update(references="gold.campaign_performance(campaign_id)"),
            "is not a non-Gold table",
        ),
        (
            lambda r: fk(r, "silver.campaigns").update(references="silver.brands(name)"),
            "must reference the full PK ['brand_id']",
        ),
        (lambda r: fk(r, "silver.campaigns").update(columns=["nope"]), "'nope' is not a column"),
        (
            set_(["tables", "silver.campaigns", "columns", "brand_id", "type"], "int"),
            "type of 'brand_id' (int) differs",
        ),
        (add_table("silver.composite_ok", COMPOSITE_CHILD), None),
        (
            add_table(
                "silver.composite_bad",
                {
                    **COMPOSITE_CHILD,
                    "foreign_keys": [
                        {
                            "columns": ["campaign_id", "creator_id"],
                            "references": "silver.campaign_creators(creator_id, campaign_id)",
                        }
                    ],
                },
            ),
            "must reference the full PK ['campaign_id', 'creator_id'], in order",
        ),
        (
            add_table(
                "silver.composite_short",
                {
                    **COMPOSITE_CHILD,
                    "foreign_keys": [
                        {"columns": ["campaign_id"], "references": "silver.campaign_creators(campaign_id)"}
                    ],
                },
            ),
            "must reference the full PK",
        ),
        (
            lambda r: fk(r, "silver.campaigns").update(references="silver.campaigns(campaign_id)"),
            "may not reference itself",
        ),
        (
            lambda r: r["tables"]["silver.brands"].update(
                foreign_keys=[{"columns": ["brand_id"], "references": "silver.campaigns(campaign_id)"}]
            ),
            "FK graph has a cycle",
        ),
        (set_(["tables", "gold.campaign_performance", "rows"], 10), "a Gold table has only"),
        (set_(["tables", "gold.campaign_performance", "transform"], None), "a Gold table needs a transform"),
        (set_(["tables", "silver.brands", "transform"], "SELECT 1"), "only Gold tables have a transform"),
        (
            set_(["tables", "gold.campaign_performance", "transform"], "SELECT FROM WHERE ((("),
            "transform does not parse",
        ),
        (
            set_(["tables", "gold.brand_daily_engagement", "transform"], "SELECT * FROM silver.posts"),
            "reads 'silver.posts'",
        ),
        (
            set_(["tables", "gold.brand_daily_engagement", "transform"], "SELECT * FROM prod_catalog.silver.nope"),
            "reads 'prod_catalog.silver.nope'",
        ),
        (
            set_(["tables", "gold.brand_daily_engagement", "transform"], "SELECT * FROM other.silver.posts"),
            "reads 'other.silver.posts'",
        ),
        (
            set_(["tables", "silver.brands", "rows_per_parent"], {"parent": "silver.creators", "min": 1, "max": 2}),
            "exactly one of rows",
        ),
        (set_(["tables", "silver.brands", "primary_key"], ["nope"]), "PK column 'nope' is not a column"),
        (set_(["tables", "silver.brands", "layer"], "bronze"), "does not match its schema"),
        (set_(["default_anchor"], {"table": "gold.campaign_performance", "key": "campaign_id"}), "default_anchor"),
        (set_(["default_anchor"], {"table": "silver.brands", "key": "name"}), "is not a PK column"),
        (set_(["tables", "silver.brands", "columns", "name", "gen"], None), "silver.brands.name: needs a gen"),
        (
            set_(["tables", "silver.brands", "columns", "name", "gen"], {"parent": "x"}),
            "parent gen needs rows_per_parent",
        ),
        (set_(["tables", "silver.posts", "columns", "account_id", "gen"], "uuid"), "an FK column has no gen"),
        (set_(["tables", "silver.creators", "columns", "tier", "gen", "weights"], [1, 2]), "weights and choice differ"),
        (set_(["tables", "silver.brands", "duplicate_fraction"], 0.1), None),
        (set_(["tables", "silver.campaign_creators", "duplicate_fraction"], 0.1), "needs a single-column uuid PK"),
        (lambda r: fk(r, "silver.posts", 1)["match"].update(nope="parent.creator_id"), "match column 'nope'"),
    ],
)
def test_validation_errors(change: Callable[[dict[str, Any]], None], message: str | None) -> None:
    errors = errors_after(change)
    if message is None:
        assert errors == ""
    else:
        assert message in errors


@pytest.mark.parametrize(
    "change",
    [
        set_(["tables", "silver.brands", "columns", "name", "gen"], "nope"),
        set_(["tables", "silver.brands", "columns", "name", "gen"], {"range": [1, 2], "nope": 1}),
        set_(["tables", "silver.brands", "colums"], {}),
        lambda r: fk(r, "silver.campaigns").update(references="silver.brands"),
    ],
)
def test_schema_errors(change: Callable[[dict[str, Any]], None]) -> None:
    raw = copy.deepcopy(RAW)
    change(raw)
    with pytest.raises(ValidationError):
        Manifest.model_validate(raw)


def test_load_reports_all_errors(tmp_path: Path) -> None:
    raw = copy.deepcopy(RAW)
    raw["tables"]["silver.brands"]["primary_key"] = ["nope"]
    raw["default_anchor"]["key"] = "name"
    path = tmp_path / "m.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ManifestError) as e:
        load_manifest(path)
    assert "PK column 'nope'" in str(e.value) and "default_anchor" in str(e.value)


def test_generation_order_puts_parents_first() -> None:
    m = load_manifest(Config().manifest)
    order = m.generation_order()
    assert "gold.campaign_performance" not in order
    for name in order:
        t = m.tables[name]
        parents = {f.ref_table for f in t.foreign_keys} | ({t.rows_per_parent.parent} if t.rows_per_parent else set())
        assert all(order.index(p) < order.index(name) for p in parents)
