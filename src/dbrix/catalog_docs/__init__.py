"""The agent-facing catalog docs (§8.4), always with prod names. Written by the sampler job into
`<sandbox>/_catalog/`, shown to the agent as /catalog."""

from pathlib import Path

from pydantic import BaseModel

from dbrix.config import CATALOG_PATH, RESULTS_PATH, OperatingMode
from dbrix.manifest import Manifest

# The sampling limitation of §4.2, in the agent's words.
SAMPLING_NOTE = """This is a sample of production: a few brands with everything that belongs to them. Rows pulled in
only because a sampled row points at them (for example a creator who posted for a sampled brand) are incomplete:
counts such as "accounts per creator" or "posts per creator across all brands" come out smaller than in the full
catalog."""


class TableSample(BaseModel):
    """What only Spark knows about a sandbox table."""

    columns: list[tuple[str, str]]  # (name, Spark type)
    row_count: int
    sample_markdown: str


def write_docs(out_dir: Path, manifest: Manifest, samples: dict[str, TableSample]) -> None:
    """README.md, plus `<schema>/<table>.md` for every manifest table."""
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "README.md").write_text(readme(manifest, samples))
    for name in manifest.tables:
        path = out_dir / f"{name.replace('.', '/')}.md"
        path.parent.mkdir(exist_ok=True)
        path.write_text(table_doc(manifest, name, samples[name]))


def full_name(manifest: Manifest, name: str) -> str:
    return f"{manifest.catalog}.{name}"


def readme(manifest: Manifest, samples: dict[str, TableSample]) -> str:
    lines = [f"# {manifest.catalog}", "", "Medallion layers: bronze (raw events), silver (clean entities), gold", ""]
    lines += ["| Table | Rows | Description |", "| --- | --- | --- |"]
    for name, t in manifest.tables.items():
        lines.append(f"| {full_name(manifest, name)} | {samples[name].row_count} | {t.description or ''} |")
    lines += ["", "## Relationships", ""]
    for name, t in manifest.tables.items():
        for fk in t.foreign_keys:
            ref = f"{full_name(manifest, fk.ref_table)}({', '.join(fk.ref_columns)})"
            lines.append(f"- {full_name(manifest, name)}({', '.join(fk.columns)}) -> {ref}")
    lines += [
        "",
        "## Notes",
        "",
        SAMPLING_NOTE,
        "",
        f"One file per table: {CATALOG_PATH}/<schema>/<table>.md. Full query results: {RESULTS_PATH}/<run>.csv.",
        "",
    ]
    return "\n".join(lines)


def table_doc(manifest: Manifest, name: str, sample: TableSample) -> str:
    t = manifest.tables[name]
    lines = [f"# {full_name(manifest, name)}", ""]
    if t.description:
        lines += [t.description, ""]
    lines += [f"Layer: {t.layer}. Rows: {sample.row_count}. Primary key: {', '.join(t.primary_key)}.", ""]
    if t.foreign_keys:
        lines += ["Foreign keys:", ""]
        for fk in t.foreign_keys:
            ref = f"{full_name(manifest, fk.ref_table)}({', '.join(fk.ref_columns)})"
            lines.append(f"- {', '.join(fk.columns)} -> {ref}")
        lines.append("")
    lines += ["## Columns", "", "| Column | Type | Description |", "| --- | --- | --- |"]
    for column, type_ in sample.columns:
        declared = t.columns.get(column)
        lines.append(f"| {column} | {type_} | {declared.description if declared and declared.description else ''} |")
    lines += ["", "## Sample rows", "", sample.sample_markdown, ""]
    return "\n".join(lines)


def agent_instructions(manifest: Manifest, mode: OperatingMode) -> str:
    """The generated AGENTS.md text, passed as the system prompt (§8.4)."""
    schemas = sorted({name.split(".")[0] for name in manifest.tables})
    writes = (
        "You may write (CREATE TABLE, INSERT, saveAsTable, ...). Each call that writes is paused until the user "
        "approves it; the user may also edit or reject it."
        if mode == OperatingMode.READ_WRITE
        else "This session is read-only: calls that create, change, or drop tables are rejected."
    )
    return f"""You are a data analyst working in a Databricks workspace.

## Catalog
- Catalog `{manifest.catalog}`, schemas {", ".join(f"`{s}`" for s in schemas)} (medallion layers: bronze holds raw
  events, silver clean entities, gold aggregates).
- Always use 3-part table names: `{manifest.catalog}.<schema>.<table>`. 1- and 2-part names are rejected.
- Table docs (columns, keys, relationships, sample rows): read `{CATALOG_PATH}/README.md` and
  `{CATALOG_PATH}/<schema>/<table>.md` before writing queries.

## Tools
- `run_sql(query)`: Spark SQL. Several statements may be separated by `;`; you get the last one's result.
- `run_pyspark(code)`: PySpark with `spark`, `F`, and `T` defined. Put the DataFrame to return in `result`.
- Each result shows a preview; the full result is a CSV under `{RESULTS_PATH}/`.
- Errors come back with a hint. Fix the code and try again.

## Mode
{writes}
"""
