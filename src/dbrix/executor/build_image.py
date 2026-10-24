"""`python -m dbrix.executor.build_image`: build the dbrix-spark image from the repo root."""

import sys
from pathlib import Path

import docker

from dbrix.config import DOCKERFILE, load_config


def build_image(client: docker.DockerClient, tag: str, context: Path) -> None:
    for line in client.api.build(path=str(context), dockerfile=DOCKERFILE, tag=tag, rm=True, decode=True):
        if "error" in line:
            raise docker.errors.BuildError(line["error"], iter([]))
        sys.stderr.write(line.get("stream", ""))


if __name__ == "__main__":
    build_image(docker.from_env(), load_config().executor.image, Path.cwd())
