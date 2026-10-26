"""SandboxManager against real executors (§9.5, §6.3). Sandboxes are copies of the one real build, renamed."""

import shutil
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import docker
import pytest

from dbrix.config import RUNS_FILE, SANDBOX_LABEL, OperatingMode
from dbrix.executor.client import remove_executors
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.lifecycle.locks import lock
from dbrix.lifecycle.manager import ExecutorBusy, Sandbox, SandboxManager
from dbrix.lifecycle.meta import read_meta, write_meta
from dbrix.lifecycle.runlog import RunRecord
from dbrix.policy.report import Language
from dbrix.sampler import SampleResult

from .conftest import CONFIG

pytestmark = pytest.mark.docker

RUN_TIMEOUT = 15  # short, so the timeout and busy cases end soon; enough for a small query on a warm Spark


@pytest.fixture
def root(client: docker.DockerClient, tmp_path: Path) -> Iterator[Path]:
    yield tmp_path
    for d in tmp_path.glob("agent_*"):
        remove_executors(client, d.name)


def manager(client: docker.DockerClient, root: Path, max_containers: int = 2) -> SandboxManager:
    executor = CONFIG.executor.model_copy(update={"run_timeout": RUN_TIMEOUT, "max_containers": max_containers})
    return SandboxManager(CONFIG.model_copy(update={"sandbox_root": root, "executor": executor}), client)


def seed(root: Path, sandbox_small: tuple[Path, SampleResult], session_id: str) -> Sandbox:
    d = Path(shutil.copytree(sandbox_small[0], root / f"agent_agent_{session_id}"))
    meta = read_meta(d)
    write_meta(d, meta.model_copy(update={"sandbox_id": d.name, "session_id": session_id}))
    return Sandbox("agent", session_id, d, meta.created_at)


def call(m: SandboxManager, sandbox: Sandbox, code: str, run_id: str, language: Language = "sql") -> RunResult:
    request = RunRequest(run_id=run_id, language=language, code=code, allow_writes=False)
    return m.run(sandbox, request, f"run_{language}", OperatingMode.READ_ONLY)[1]


def running(client: docker.DockerClient) -> set[str]:
    return {c.labels[SANDBOX_LABEL] for c in client.containers.list(filters={"label": SANDBOX_LABEL})}


def test_runs_are_logged_and_set_the_status(
    client: docker.DockerClient, root: Path, sandbox_small: tuple[Path, SampleResult]
) -> None:
    m, sandbox = manager(client, root), seed(root, sandbox_small, "lc1")

    assert call(m, sandbox, "SELECT 1", "ok1").ok
    meta = read_meta(sandbox.dir)
    assert (meta.status, meta.run_count, meta.active_run) == ("IDLE", 1, None)
    assert meta.container_id and running(client) == {sandbox.id}

    assert not call(m, sandbox, "SELECT * FROM prod_catalog.silver.nope", "err1").ok
    assert read_meta(sandbox.dir).status == "FAILED"

    r = call(m, sandbox, "import time\ntime.sleep(600)", "slow1", "pyspark")
    assert r.error and r.error.type == "TimeoutError" and "/sandbox" not in r.error.message
    assert running(client) == set()  # stopped; the next run restarts it

    # Two runs at once wait for each other, and the restart happens once.
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda i: call(m, sandbox, "SELECT 1", f"par{i}"), range(2)))
    assert all(r.ok for r in results)
    meta = read_meta(sandbox.dir)
    assert (meta.status, meta.run_count, meta.last_error) == ("IDLE", 5, None)

    lines = (sandbox.dir / RUNS_FILE).read_text().splitlines()
    records = [RunRecord.model_validate_json(line) for line in lines]
    assert [r.outcome for r in records[:3]] == ["OK", "RUNTIME_ERROR", "TIMEOUT"]
    a, b = sorted(records[3:], key=lambda r: r.started_at)
    assert a.started_at + timedelta(milliseconds=a.duration_ms) <= b.started_at

    # A repeated run_id (a resumed step, §8.3) is not logged twice.
    call(m, sandbox, "SELECT 1", "ok1")
    assert len((sandbox.dir / RUNS_FILE).read_text().splitlines()) == 5


def test_container_limit_evicts_least_recently_used(
    client: docker.DockerClient, root: Path, sandbox_small: tuple[Path, SampleResult]
) -> None:
    m = manager(client, root)
    x, y, z = (seed(root, sandbox_small, s) for s in ["lcx", "lcy", "lcz"])
    call(m, x, "SELECT 1", "x1")
    call(m, y, "SELECT 1", "y1")
    call(m, z, "SELECT 1", "z1")
    assert running(client) == {y.id, z.id}
    assert call(m, x, "SELECT 1", "x2").ok  # the evicted sandbox works again
    assert running(client) == {z.id, x.id}


def test_busy_executors_make_a_call_wait(
    client: docker.DockerClient, root: Path, sandbox_small: tuple[Path, SampleResult]
) -> None:
    m = manager(client, root, max_containers=1)
    x, y = seed(root, sandbox_small, "lcbx"), seed(root, sandbox_small, "lcby")
    call(m, x, "SELECT 1", "x1")

    # x is running a run, so its container can't be evicted.
    with lock(root, x.id), pytest.raises(ExecutorBusy):
        call(m, y, "SELECT 1", "y1")
    assert running(client) == {x.id}

    held = lock(root, x.id)
    held.__enter__()
    threading.Timer(3, held.__exit__, (None, None, None)).start()  # x's run ends after 3 s
    assert call(m, y, "SELECT 1", "y2").ok
    assert running(client) == {y.id}
