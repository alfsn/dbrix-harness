"""Host side of the executor (§6.3, §7): start the container, send one request, read one response. Everything the
container sends is untrusted: lines are size-limited and validated."""

import json
import select
import socket
import struct
import time
from dataclasses import dataclass
from pathlib import Path

import docker
from docker.models.containers import Container

from dbrix.config import (
    EXECUTOR_START_TIMEOUT,
    MAX_RESPONSE_BYTES,
    RESULTS_PATH,
    SANDBOX_LABEL,
    SANDBOX_MOUNT,
    ExecutorConfig,
)
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.jobs import NO_NETWORK


class ExecutorError(Exception):
    pass


class ExecutorTimeout(ExecutorError):
    pass


@dataclass
class Executor:
    container: Container
    sock: socket.socket  # attached to the container's stdin and stdout
    pending: bytes = b""  # stdout after the last full line


def _recv(sock: socket.socket, n: int, deadline: float) -> bytes:
    data = b""
    while len(data) < n:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([sock], [], [], remaining)[0]:
            raise ExecutorTimeout("the executor did not answer in time")
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ExecutorError("the executor closed its output")
        data += chunk
    return data


def _read_line(executor: Executor, timeout: float) -> bytes:
    """One stdout line. Without a tty, Docker sends stdout in frames: an 8-byte header, then the payload."""
    deadline = time.monotonic() + timeout
    while b"\n" not in executor.pending:
        _, size = struct.unpack(">BxxxL", _recv(executor.sock, 8, deadline))
        if len(executor.pending) + size > MAX_RESPONSE_BYTES:
            raise ExecutorError(f"executor response larger than {MAX_RESPONSE_BYTES} bytes")
        executor.pending += _recv(executor.sock, size, deadline)
    line, _, executor.pending = executor.pending.partition(b"\n")
    return line


def start_executor(
    client: docker.DockerClient, image: str, sandbox_dir: Path, sandbox_id: str, catalog: str, cfg: ExecutorConfig
) -> Executor:
    """Start the runtime with the §7 isolation and wait until Spark is ready."""
    container = client.containers.create(
        image,
        ["python", "-m", "dbrix.executor.runtime", catalog, str(cfg.preview_rows), str(cfg.result_file_max_rows)],
        stdin_open=True,
        read_only=True,
        tmpfs={"/tmp": "rw,exec,nosuid,size=1g"},  # exec: Spark loads native codecs (snappy, lz4) from /tmp
        volumes={str(sandbox_dir.resolve()): {"bind": SANDBOX_MOUNT, "mode": "rw"}},
        user="10001:10001",
        cap_drop=["ALL"],
        security_opt=["no-new-privileges"],
        pids_limit=512,
        mem_limit=cfg.memory,
        nano_cpus=int(cfg.cpus * 1e9),
        labels={SANDBOX_LABEL: sandbox_id},
        **NO_NETWORK,
    )
    executor = Executor(container, _attach(client, container))
    _wait_ready(executor)
    return executor


def restart_executor(client: docker.DockerClient, executor: Executor) -> None:
    """Start the stopped container again (§6.3). Tables survive in the metastore; temp views do not."""
    executor.sock.close()
    executor.container.stop(timeout=0)  # no-op when already stopped
    executor.sock, executor.pending = _attach(client, executor.container), b""
    _wait_ready(executor)


def _attach(client: docker.DockerClient, container: Container) -> socket.socket:
    """Attach, then start, so no line is lost (§6.3)."""
    sock = client.api.attach_socket(str(container.id), params={"stdin": 1, "stdout": 1, "stream": 1})
    container.start()
    raw: socket.socket = getattr(sock, "_sock", sock)
    return raw


def _wait_ready(executor: Executor) -> None:
    try:
        ready = json.loads(_read_line(executor, EXECUTOR_START_TIMEOUT))
    except ExecutorError as e:
        logs = executor.container.logs(stdout=False, stderr=True, tail=20).decode(errors="replace")
        raise ExecutorError(f"executor did not start: {e}\n{logs}") from e
    if ready != {"ready": True}:
        raise ExecutorError(f"unexpected first line from the executor: {ready!r}")


def run(executor: Executor, request: RunRequest, timeout: float) -> RunResult:
    """Send one request. When no answer comes within `timeout` seconds (ExecutorTimeout) or the answer is broken
    (ExecutorError), the container is stopped: its output can't be trusted to line up any more. The caller
    restarts it with `restart_executor`."""
    executor.sock.sendall(request.model_dump_json().encode() + b"\n")
    try:
        line = _read_line(executor, timeout)
    except ExecutorError:
        executor.container.stop(timeout=0)
        raise
    result = RunResult.model_validate_json(line)
    if result.full_result_path not in (None, f"{RESULTS_PATH}/{request.run_id}.csv"):
        raise ExecutorError(f"unexpected result path from the executor: {result.full_result_path!r}")
    return result
