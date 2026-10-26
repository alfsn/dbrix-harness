import shutil
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from dbrix.config import CATALOG_META_FILE, Config, SamplingConfig
from dbrix.ids import InvalidSandboxIdError, sandbox_id
from dbrix.manifest import CatalogMeta, ManifestError, manifest_hash
from dbrix.sampler import run_sampler

CONFIG = Config()


@pytest.mark.parametrize("agent_id", ["analyst", "a-1", "A" * 64])
def test_valid_ids(agent_id: str) -> None:
    assert (
        sandbox_id(agent_id, "0b7e9a52-8f3c-4d0e-9c1a-2f6d5e4b3a21")
        == f"agent_{agent_id}_0b7e9a52-8f3c-4d0e-9c1a-2f6d5e4b3a21"
    )


@pytest.mark.parametrize("bad", ["", "a_b", "a.b", "a/b", "..", "A" * 65, "abc\n", "a b", "é", "٣"])
def test_invalid_ids(bad: str) -> None:
    with pytest.raises(InvalidSandboxIdError):
        sandbox_id(bad, "s1")
    with pytest.raises(InvalidSandboxIdError):
        sandbox_id("agent", bad)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"sample_size": 0},
        {"unrelated_sample_fraction": -0.1},
        {"unrelated_sample_fraction": 1.1},
        {"anchor_table": "silver.brands"},
        {"anchor_key": "brand_id"},
    ],
)
def test_sampling_config_errors(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        SamplingConfig(**kwargs)


@pytest.fixture
def prod(tmp_path: Path) -> Path:
    """A prod catalog dir with only its meta file: enough for the checks made before the job starts."""
    meta = CatalogMeta(manifest_hash=manifest_hash(CONFIG.manifest), seed=1, scale=1, table_versions={})
    (tmp_path / "prod").mkdir()
    (tmp_path / "prod" / CATALOG_META_FILE).write_text(meta.model_dump_json())
    return tmp_path / "prod"


def sample(prod: Path, sandbox: Path, **kwargs: Any) -> None:
    """Call run_sampler without Docker: every case here must fail before the job starts."""
    args: dict[str, Any] = {
        "manifest_path": CONFIG.manifest,
        "agent_id": "agent",
        "session_id": "s1",
        "sampling": SamplingConfig(),
    }
    run_sampler(None, "unused", prod, sandbox, **(args | kwargs))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"agent_id": "a_b"}, InvalidSandboxIdError, "agent_id"),
        ({"session_id": "../x"}, InvalidSandboxIdError, "session_id"),
        (
            {"sampling": SamplingConfig(anchor_table="gold.campaign_performance", anchor_key="campaign_id")},
            ValueError,
            "not a non-Gold table",
        ),
        ({"sampling": SamplingConfig(anchor_table="silver.brands", anchor_key="name")}, ValueError, "not a PK column"),
    ],
)
def test_run_sampler_refuses_bad_input(
    prod: Path, tmp_path: Path, kwargs: dict[str, Any], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        sample(prod, tmp_path / "sandbox", **kwargs)


def test_run_sampler_refuses_missing_catalog(prod: Path, tmp_path: Path) -> None:
    shutil.rmtree(prod)
    with pytest.raises(ManifestError, match="generate the prod catalog first"):
        sample(prod, tmp_path / "sandbox")


def test_run_sampler_refuses_other_manifest(prod: Path, tmp_path: Path) -> None:
    other = tmp_path / "other.yaml"
    other.write_text(CONFIG.manifest.read_text() + "\n# changed\n")
    with pytest.raises(ManifestError, match="another manifest"):
        sample(prod, tmp_path / "sandbox", manifest_path=other)


def test_run_sampler_refuses_non_empty_dir(prod: Path, tmp_path: Path) -> None:
    (tmp_path / "sandbox").mkdir()
    (tmp_path / "sandbox" / "old").write_text("x")
    with pytest.raises(FileExistsError):
        sample(prod, tmp_path / "sandbox")
