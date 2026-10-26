import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import docker
import pytest
import requests

from dbrix.config import Config, SamplingConfig
from dbrix.executor.build_image import build_image
from dbrix.generator import run_generator
from dbrix.jobs import run_job
from dbrix.manifest import load_manifest
from dbrix.sampler import SampleResult, run_sampler

REPO = Path(__file__).parents[2]
CONFIG = Config()
TABLES = list(load_manifest(CONFIG.manifest).tables)


@pytest.fixture(scope="session")
def client() -> Iterator[docker.DockerClient]:
    # Fail fast when the Docker engine hangs (it has: `docker ps` never returned). Short timeout for this
    # check only; the real client keeps the default, since jobs wait on containers for minutes.
    try:
        docker.from_env(timeout=10).ping()
    except (docker.errors.DockerException, requests.RequestException) as e:
        pytest.exit(f"Docker does not answer ({e}); restart Docker Desktop", returncode=1)
    c = docker.from_env()
    build_image(c, CONFIG.executor.image, REPO)
    yield c
    c.close()


@pytest.fixture(scope="session")
def prod_small(client: docker.DockerClient, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A prod catalog with about 20 brands."""
    path = tmp_path_factory.mktemp("prod_small")
    run_generator(client, CONFIG.executor.image, path, CONFIG.manifest, seed=3, scale=0.2)
    return path


@pytest.fixture(scope="session")
def sandbox_small(
    client: docker.DockerClient, prod_small: Path, tmp_path_factory: pytest.TempPathFactory
) -> tuple[Path, SampleResult]:
    """The one real sandbox build of the suite, built in `.creating-a` and renamed as the lifecycle layer does.
    Tests that change it work on a copy."""
    root = tmp_path_factory.mktemp("sandbox_small")
    result = run_sampler(
        client,
        CONFIG.executor.image,
        prod_small,
        root / ".creating-a",
        CONFIG.manifest,
        "agent",
        "s1",
        SamplingConfig(),
    )
    return (root / ".creating-a").rename(root / "agent_agent_s1"), result


@pytest.fixture(scope="session")
def prod_scale_1(client: docker.DockerClient, tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, float]:
    """The default prod catalog, and how many seconds the generator took."""
    path = tmp_path_factory.mktemp("prod_scale_1")
    start = time.monotonic()
    run_generator(client, CONFIG.executor.image, path, CONFIG.manifest, CONFIG.generator.seed, CONFIG.generator.scale)
    return path, time.monotonic() - start


def spark_json(
    client: docker.DockerClient, code: str, mounts: dict[str, Path], catalog_dir: str = "/tmp/scratch"
) -> Any:
    """Run `code` in the image with `spark` defined; it must print one JSON line last."""
    prelude = f"from dbrix.spark import create_session\nspark = create_session({catalog_dir!r})\n"
    volumes = {str(p.resolve()): {"bind": dst, "mode": "rw"} for dst, p in mounts.items()}
    out = run_job(client, CONFIG.executor.image, ["python", "-c", prelude + code], volumes)
    return json.loads(out.strip().splitlines()[-1])


# Order-independent content hash per table: sum of row hashes.
CONTENT_HASHES = """
import json
from pyspark.sql import functions as F
from dbrix.spark import table_path
def content(root, name):
    df = spark.read.format("delta").load(table_path(root, name))
    return str(df.select(F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)"))).first()[0])
print(json.dumps({root: {n: content(root, n) for n in %r} for root in %r}))
"""


def content_hashes(client: docker.DockerClient, dirs: dict[str, Path]) -> dict[str, dict[str, str]]:
    """Per mount point in `dirs`, the content hash of every manifest table."""
    result: dict[str, dict[str, str]] = spark_json(client, CONTENT_HASHES % (TABLES, list(dirs)), dirs)
    return result
