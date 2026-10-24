"""Phase 1: build the immutable prod catalog in a jobs container (§3)."""

from pathlib import Path
from typing import Any

import docker

from dbrix.config import MANIFEST_MOUNT, PROD_CATALOG_MOUNT
from dbrix.jobs import run_job
from dbrix.manifest import load_manifest


def run_generator(
    client: docker.DockerClient,
    image: str,
    prod_catalog_dir: Path,
    manifest: Path,
    seed: int,
    scale: float,
    **docker_kwargs: Any,
) -> None:
    load_manifest(manifest)  # fail fast, before starting Spark
    prod_catalog_dir.mkdir(parents=True, exist_ok=True)
    if any(prod_catalog_dir.iterdir()):
        raise FileExistsError(
            f"{prod_catalog_dir} is not empty; the prod catalog is immutable, delete it to regenerate"
        )
    run_job(
        client,
        image,
        ["python", "-m", "dbrix.generator.generate", MANIFEST_MOUNT, PROD_CATALOG_MOUNT, str(seed), str(scale)],
        {
            str(prod_catalog_dir.resolve()): {"bind": PROD_CATALOG_MOUNT, "mode": "rw"},
            str(manifest.resolve()): {"bind": MANIFEST_MOUNT, "mode": "ro"},
        },
        **docker_kwargs,
    )
