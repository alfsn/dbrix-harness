"""Phase 2: build one sandbox from the prod catalog in a jobs container (§4)."""

import json
from pathlib import Path
from typing import Any

import docker
from pydantic import BaseModel

from dbrix.config import MANIFEST_MOUNT, PROD_CATALOG_MOUNT, SANDBOX_MOUNT, SamplingConfig
from dbrix.ids import sandbox_id
from dbrix.jobs import require_empty_dir, run_job
from dbrix.manifest import Anchor, Manifest, load_manifest, read_catalog_meta, validate_anchor


class Sampling(BaseModel):
    """The `sampling` block of `_sandbox_meta.json` (§9.1)."""

    anchor_table: str
    anchor_key: str
    sample_size: int
    seed: int
    unrelated_sample_fraction: float
    manifest_hash: str
    prod_table_versions: dict[str, int]


class SampleResult(BaseModel):
    sampling: Sampling
    row_counts: dict[str, int]


def anchor_of(manifest: Manifest, sampling: SamplingConfig) -> Anchor:
    if sampling.anchor_table is None or sampling.anchor_key is None:
        return manifest.default_anchor
    return Anchor(table=sampling.anchor_table, key=sampling.anchor_key)


def run_sampler(
    client: docker.DockerClient,
    image: str,
    prod_catalog_dir: Path,
    sandbox_dir: Path,
    manifest_path: Path,
    agent_id: str,
    session_id: str,
    sampling: SamplingConfig,
    **docker_kwargs: Any,
) -> SampleResult:
    """Fill the empty `sandbox_dir` with a sample of the prod catalog. Everything is checked before Spark starts."""
    sandbox_id(agent_id, session_id)
    manifest = load_manifest(manifest_path)
    anchor = anchor_of(manifest, sampling)
    if errors := validate_anchor(manifest, anchor):
        raise ValueError(f"anchor: {errors[0]}")
    catalog = read_catalog_meta(prod_catalog_dir, manifest_path)
    require_empty_dir(sandbox_dir, "a sandbox is built into an empty dir")
    out = run_job(
        client,
        image,
        ["python", "-m", "dbrix.sampler.sample", MANIFEST_MOUNT, agent_id, session_id, sampling.model_dump_json()],
        {
            str(prod_catalog_dir.resolve()): {"bind": PROD_CATALOG_MOUNT, "mode": "ro"},
            str(sandbox_dir.resolve()): {"bind": SANDBOX_MOUNT, "mode": "rw"},
            str(manifest_path.resolve()): {"bind": MANIFEST_MOUNT, "mode": "ro"},
        },
        **docker_kwargs,
    )
    job = json.loads(out.strip().splitlines()[-1])
    return SampleResult(
        sampling=Sampling(
            anchor_table=anchor.table,
            anchor_key=anchor.key,
            sample_size=sampling.sample_size,
            seed=job["seed"],
            unrelated_sample_fraction=sampling.unrelated_sample_fraction,
            manifest_hash=catalog.manifest_hash,
            prod_table_versions=catalog.table_versions,
        ),
        row_counts=job["row_counts"],
    )
