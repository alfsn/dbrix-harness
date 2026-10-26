"""§11.3 containment: the red-team cases run on raw Spark, without the wrapper (§6.1). Only the container (§7)
stands between them and prod data or the network."""

import shutil
from collections.abc import Iterator
from pathlib import Path

import docker
import pytest

from dbrix.executor.client import Executor, ExecutorTimeout, restart_executor, run, start_executor
from dbrix.executor.protocol import RunRequest, RunResult
from dbrix.policy.report import Language
from dbrix.sampler import SampleResult
from tests.redteam.cases import BLOCKED, DYNAMIC

from .conftest import CONFIG

pytestmark = pytest.mark.docker

SERVE_RAW = """
from dbrix.executor.runtime import serve
from dbrix.executor.runtime.wrap import RunState
serve("prod_catalog", 1, 10, install=lambda spark, catalog: RunState())
"""
PRELUDE = "import base64\ndf = spark.range(1)\n"  # names the cases use
TIMEOUT = 90  # the first table read on a fresh Spark takes about 45 s; a case that starts a shell hangs until killed


@pytest.fixture(scope="module")
def raw_executor(
    client: docker.DockerClient, sandbox_small: tuple[Path, SampleResult], tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Executor]:
    sandbox = Path(shutil.copytree(sandbox_small[0], tmp_path_factory.mktemp("containment") / "agent_agent_s1"))
    e = start_executor(
        client,
        CONFIG.executor.image,
        sandbox,
        "agent_agent_s1",
        "prod_catalog",
        CONFIG.executor,
        ["python", "-c", SERVE_RAW],
    )
    yield e
    e.container.remove(force=True)


def call(executor: Executor, language: Language, code: str) -> RunResult:
    return run(executor, RunRequest(run_id="rt", language=language, code=code, allow_writes=True), TIMEOUT)


def snapshot(path: Path) -> dict[Path, tuple[int, int]]:
    return {p: (p.stat().st_size, p.stat().st_mtime_ns) for p in path.rglob("*")}


def test_runs_without_wrapper(raw_executor: Executor) -> None:
    """The control: a 2-part name, which the wrapper blocks, reaches raw Spark."""
    assert call(raw_executor, "sql", "SELECT * FROM silver.brands").ok


def test_no_network(raw_executor: Executor) -> None:
    r = call(raw_executor, "pyspark", 'import socket\nsocket.create_connection(("1.1.1.1", 53), timeout=5)')
    assert r.error and r.error.type == "OSError", r


@pytest.mark.parametrize(("language", "code"), BLOCKED + [("pyspark", code) for code in DYNAMIC])
def test_contained(
    client: docker.DockerClient, raw_executor: Executor, prod_small: Path, language: Language, code: str
) -> None:
    before = snapshot(prod_small)
    try:
        call(raw_executor, language, PRELUDE + code if language == "pyspark" else code)
    except ExecutorTimeout:
        restart_executor(client, raw_executor)
    assert snapshot(prod_small) == before
