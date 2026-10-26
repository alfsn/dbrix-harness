"""Manifest models, loading, and validation (§3.1)."""

import graphlib
import hashlib
import re
from datetime import date
from functools import cached_property
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from dbrix.config import CATALOG_META_FILE, Layer, SamplingConfig
from dbrix.policy.exceptions import SandboxSecurityException
from dbrix.policy.sql import check_sql


class ManifestError(ValueError):
    pass


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RangeGen(_Model):
    range: tuple[float, float]
    skew: float = 1.0


class ChoiceGen(_Model):
    choice: list[str | int | float]
    weights: list[float] | None = None


class BetweenGen(_Model):
    between: tuple[date, date]


class ParentGen(_Model):
    parent: str


class SqlGen(_Model):
    sql: str


NamedGen = Literal["uuid", "company", "person_name", "text"]
Gen = NamedGen | RangeGen | ChoiceGen | BetweenGen | ParentGen | SqlGen


class Column(_Model):
    type: str
    gen: Gen | None = None
    null_fraction: float = 0.0
    description: str | None = None


_REFERENCE = re.compile(r"(\w+\.\w+)\((\w+(?:\s*,\s*\w+)*)\)")


class ForeignKey(_Model):
    columns: list[str]
    references: str  # "<schema>.<table>(<col>, ...)"
    skew: float = 1.0  # when picked: > 1 favours the first parent rows
    match: dict[str, str] = {}  # when picked: parent column -> SQL expression on the child row

    @field_validator("references")
    @classmethod
    def _parse(cls, value: str) -> str:
        if not _REFERENCE.fullmatch(value):
            raise ValueError(f"expected '<schema>.<table>(<col>, ...)', got {value!r}")
        return value

    @cached_property
    def ref_table(self) -> str:
        return _REFERENCE.fullmatch(self.references).group(1)  # type: ignore[union-attr]

    @cached_property
    def ref_columns(self) -> list[str]:
        return [c.strip() for c in _REFERENCE.fullmatch(self.references).group(2).split(",")]  # type: ignore[union-attr]


class RowsPerParent(_Model):
    parent: str
    min: int
    max: int


class Table(_Model):
    layer: Layer
    description: str | None = None
    primary_key: list[str]
    foreign_keys: list[ForeignKey] = []
    columns: dict[str, Column] = {}
    rows: int | None = None
    rows_per_parent: RowsPerParent | None = None
    duplicate_fraction: float = 0.0
    transform: str | None = None


class Anchor(_Model):
    table: str
    key: str


class Manifest(_Model):
    catalog: str
    default_anchor: Anchor
    tables: dict[str, Table]

    def generation_order(self) -> list[str]:
        """Non-Gold tables, parents first (FK and `rows_per_parent` edges). Raises graphlib.CycleError."""
        graph = {
            name: {fk.ref_table for fk in t.foreign_keys} | ({t.rows_per_parent.parent} if t.rows_per_parent else set())
            for name, t in self.tables.items()
            if t.layer != "gold"
        }
        for name, parents in graph.items():
            parents.discard(name)  # self-references are reported by validate()
        return list(graphlib.TopologicalSorter(graph).static_order())


def load_manifest(path: Path) -> Manifest:
    try:
        manifest = Manifest.model_validate(yaml.safe_load(path.read_text()))
    except ValidationError as e:
        raise ManifestError(f"{path}: {e}") from None
    if errors := validate(manifest):
        raise ManifestError(f"{path}:\n" + "\n".join(f"- {e}" for e in errors))
    return manifest


def manifest_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


class CatalogMeta(_Model):
    """`_catalog_meta.json`: binds a prod catalog to the manifest it was generated from (§3.3)."""

    manifest_hash: str
    seed: int
    scale: float
    table_versions: dict[str, int]


def read_catalog_meta(catalog_dir: Path, manifest_path: Path) -> CatalogMeta:
    """Raises ManifestError when the catalog has no meta or was generated from another manifest."""
    path = catalog_dir / CATALOG_META_FILE
    if not path.exists():
        raise ManifestError(f"{path} not found; generate the prod catalog first (python -m dbrix.generator)")
    meta = CatalogMeta.model_validate_json(path.read_text())
    if meta.manifest_hash != manifest_hash(manifest_path):
        raise ManifestError(f"{catalog_dir} was generated from another manifest than {manifest_path}; regenerate it")
    return meta


def validate(m: Manifest) -> list[str]:
    """Every problem found in the manifest, as messages. Empty when it is valid."""
    errors: list[str] = []
    for name, t in m.tables.items():
        if not re.fullmatch(r"\w+\.\w+", name):
            errors.append(f"{name}: table key must be '<schema>.<table>'")
            continue
        if name.split(".")[0] != t.layer:
            errors.append(f"{name}: layer '{t.layer}' does not match its schema")
        if t.layer == "gold":
            errors += _validate_gold(m, name, t)
        else:
            errors += _validate_generated(m, name, t)

    errors += [f"default_anchor: {e}" for e in validate_anchor(m, m.default_anchor)]

    try:
        m.generation_order()
    except graphlib.CycleError as e:
        errors.append(f"FK graph has a cycle: {' -> '.join(e.args[1])}")
    return errors


def anchor_of(m: Manifest, sampling: SamplingConfig) -> Anchor:
    """The anchor set in `sampling`, or the manifest's default_anchor (§4.1)."""
    if sampling.anchor_table is None or sampling.anchor_key is None:
        return m.default_anchor
    return Anchor(table=sampling.anchor_table, key=sampling.anchor_key)


def validate_anchor(m: Manifest, anchor: Anchor) -> list[str]:
    """Problems with `anchor` as the sampling anchor (§4.1). Empty when it is valid."""
    t = m.tables.get(anchor.table)
    if t is None or t.layer == "gold":
        return [f"'{anchor.table}' is not a non-Gold table"]
    if anchor.key not in t.primary_key:
        return [f"'{anchor.key}' is not a PK column of {anchor.table}"]
    return []


def _validate_gold(m: Manifest, name: str, t: Table) -> list[str]:
    if t.rows is not None or t.rows_per_parent or t.columns or t.foreign_keys or t.duplicate_fraction:
        return [f"{name}: a Gold table has only description, primary_key, and transform"]
    if t.transform is None:
        return [f"{name}: a Gold table needs a transform"]
    try:
        report = check_sql(t.transform)
    except SandboxSecurityException as e:
        return [f"{name}: transform {'does not parse' if e.rule_id == 'SQL_PARSE_ERROR' else 'is not allowed'}: {e}"]
    errors = [f"{name}: transform writes ({w.kind}); it may only read" for w in report.writes]
    for ref in sorted(report.reads):
        catalog, _, table = ref.partition(".")
        source = m.tables.get(table)
        if catalog != m.catalog or source is None or source.layer == "gold":
            errors.append(
                f"{name}: transform reads '{ref}'; "
                f"use '{m.catalog}.<schema>.<table>' for a Bronze or Silver table of this manifest"
            )
    return errors


def _validate_generated(m: Manifest, name: str, t: Table) -> list[str]:
    errors = []
    if t.transform is not None:
        errors.append(f"{name}: only Gold tables have a transform")
    if (t.rows is None) == (t.rows_per_parent is None):
        errors.append(f"{name}: needs exactly one of rows, rows_per_parent")
    errors += [f"{name}: PK column '{c}' is not a column" for c in t.primary_key if c not in t.columns]

    parent = None
    if t.rows_per_parent:
        parent = m.tables.get(t.rows_per_parent.parent)
        if parent is None or parent.layer == "gold":
            errors.append(f"{name}: rows_per_parent.parent '{t.rows_per_parent.parent}' is not a non-Gold table")
        if not 0 <= t.rows_per_parent.min <= t.rows_per_parent.max:
            errors.append(f"{name}: rows_per_parent needs 0 <= min <= max")

    fk_columns: set[str] = set()
    for fk in t.foreign_keys:
        where = f"{name}: FK {fk.columns} -> {fk.references}"
        ref = m.tables.get(fk.ref_table)
        if fk.ref_table == name:
            errors.append(f"{where}: a table may not reference itself")
            continue
        if ref is None or ref.layer == "gold":
            errors.append(f"{where}: '{fk.ref_table}' is not a non-Gold table")
            continue
        if fk.ref_columns != ref.primary_key:
            errors.append(f"{where}: must reference the full PK {ref.primary_key}, in order")
        if len(fk.columns) != len(fk.ref_columns):
            errors.append(f"{where}: column count differs from the referenced columns")
        for c, rc in zip(fk.columns, fk.ref_columns):
            if c not in t.columns:
                errors.append(f"{where}: '{c}' is not a column")
            elif rc in ref.columns and t.columns[c].type != ref.columns[rc].type:
                errors.append(
                    f"{where}: type of '{c}' ({t.columns[c].type}) differs from '{rc}' ({ref.columns[rc].type})"
                )
        errors += [
            f"{where}: match column '{c}' is not a column of {fk.ref_table}" for c in fk.match if c not in ref.columns
        ]
        gens = {t.columns[c].gen is None for c in fk.columns if c in t.columns}
        if len(gens) > 1:
            errors.append(f"{where}: either all FK columns copy from the parent or none has a gen (picked)")
        fk_columns |= set(fk.columns)

    for c, col in t.columns.items():
        if col.gen is None and c not in fk_columns:
            errors.append(f"{name}.{c}: needs a gen")
        if c in fk_columns and col.gen is not None and not isinstance(col.gen, ParentGen):
            errors.append(f"{name}.{c}: an FK column has no gen (picked) or a parent gen")
        if isinstance(col.gen, ParentGen) and (parent is None or col.gen.parent not in parent.columns):
            errors.append(f"{name}.{c}: parent gen needs rows_per_parent and a column of that parent")
        if isinstance(col.gen, ChoiceGen) and col.gen.weights and len(col.gen.weights) != len(col.gen.choice):
            errors.append(f"{name}.{c}: weights and choice differ in length")

    if t.duplicate_fraction and not (
        len(t.primary_key) == 1 and t.columns.get(t.primary_key[0], Column(type="")).gen == "uuid"
    ):
        errors.append(f"{name}: duplicate_fraction needs a single-column uuid PK")
    return errors
