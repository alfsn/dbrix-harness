from pathlib import Path

from dbrix.catalog_docs import SAMPLING_NOTE, TableSample, agent_instructions, write_docs
from dbrix.config import Config, OperatingMode
from dbrix.manifest import load_manifest

MANIFEST = load_manifest(Config().manifest)


def test_docs_use_prod_names_only(tmp_path: Path) -> None:
    samples = {
        name: TableSample(columns=[("brand_id", "string")], row_count=7, sample_markdown="| brand_id |\n| --- |")
        for name in MANIFEST.tables
    }
    write_docs(tmp_path, MANIFEST, samples)
    readme = (tmp_path / "README.md").read_text()
    assert "prod_catalog.silver.campaigns(brand_id) -> prod_catalog.silver.brands(brand_id)" in readme
    assert SAMPLING_NOTE in readme
    brands = (tmp_path / "silver" / "brands.md").read_text()
    assert brands.startswith("# prod_catalog.silver.brands")
    assert "| brand_id | string | Brand identifier |" in brands and "Rows: 7." in brands
    docs = [p.read_text() for p in tmp_path.rglob("*.md")]
    assert len(docs) == len(MANIFEST.tables) + 1
    assert not any("/sandbox" in d or "agent_" in d for d in docs)


def test_instructions_state_the_mode() -> None:
    assert "read-only" in agent_instructions(MANIFEST, OperatingMode.READ_ONLY)
    assert "approves" in agent_instructions(MANIFEST, OperatingMode.READ_WRITE)
