"""One-off jobs containers for trusted dbrix code (§7)."""

import sys
from typing import Any

import docker


class JobError(Exception):
    pass


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
        network_mode="none",
        hostname="localhost",  # with no network, the default container hostname does not resolve
        **docker_kwargs,
    )
    try:
        container.start()
        for chunk in container.logs(stream=True, follow=True):
            sys.stderr.buffer.write(chunk)
        status = container.wait()["StatusCode"]
        if status:
            raise JobError(f"job {command} exited with status {status}")
        return str(container.logs(stdout=True, stderr=False).decode())
    finally:
        container.remove(force=True)
