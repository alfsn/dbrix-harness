import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import docker
import pytest

from dbrix.config import Config
from dbrix.executor.build_image import build_image
from dbrix.jobs import run_job

REPO = Path(__file__).parents[2]


@pytest.fixture(scope="session")
def client() -> Iterator[docker.DockerClient]:
    c = docker.from_env()
    build_image(c, Config().executor.image, REPO)
    yield c
    c.close()


def spark_json(client: docker.DockerClient, code: str, mounts: dict[str, Path]) -> Any:
    """Run `code` in the image with `spark` defined; it must print one JSON line last."""
    prelude = "from dbrix.spark import create_session\nspark = create_session('/tmp/scratch')\n"
    volumes = {str(p.resolve()): {"bind": dst, "mode": "rw"} for dst, p in mounts.items()}
    out = run_job(client, Config().executor.image, ["python", "-c", prelude + code], volumes)
    return json.loads(out.strip().splitlines()[-1])
