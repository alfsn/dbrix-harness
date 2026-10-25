"""One-off jobs containers for trusted dbrix code (§7)."""

import sys
from pathlib import Path
from typing import Any

import docker

from dbrix.config import JOB_ERROR_LINES

# Every dbrix container runs with the network off. With no network, the default container hostname does not resolve.
NO_NETWORK: dict[str, Any] = {"network_mode": "none", "hostname": "localhost"}


class JobError(Exception):
    pass


def require_empty_dir(path: Path, why: str) -> None:
    """Create `path` if needed. Raises FileExistsError when it holds anything."""
    path.mkdir(parents=True, exist_ok=True)
    if any(path.iterdir()):
        raise FileExistsError(f"{path} is not empty; {why}")


def run_job(
    client: docker.DockerClient,
    image: str,
    command: list[str],
    volumes: dict[str, dict[str, str]],
    **docker_kwargs: Any,
) -> str:
    """Run `command` with the network off. Streams its output to stderr; returns its stdout."""
    container = client.containers.create(
        image,
        command,
        volumes=volumes,
        **NO_NETWORK,
        **docker_kwargs,
    )
    try:
        container.start()
        for chunk in container.logs(stream=True, follow=True):
            sys.stderr.buffer.write(chunk)
        status = container.wait()["StatusCode"]
        if status:
            tail = container.logs(stdout=False, stderr=True, tail=JOB_ERROR_LINES).decode(errors="replace")
            raise JobError(f"job {command} exited with status {status}:\n{tail}")
        return str(container.logs(stdout=True, stderr=False).decode())
    finally:
        container.remove(force=True)
