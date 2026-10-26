import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from dbrix.config import RUNS_FILE, SANDBOX_META_FILE, Config, OperatingMode
from dbrix.lifecycle import gc
from dbrix.lifecycle.locks import LockBusy, lock
from dbrix.lifecycle.manager import SandboxManager
from dbrix.lifecycle.meta import ActiveRun, SandboxMeta, read_meta, write_meta
from dbrix.lifecycle.runlog import RunRecord, append_run
from dbrix.sampler import SampleResult, Sampling

NOW = datetime(2026, 10, 26, 12, tzinfo=UTC)
TTL = 3600
RUN_TIMEOUT = 120


def meta(sid: str, last_used_at: datetime, active_run: ActiveRun | None = None) -> SandboxMeta:
    agent_id, session_id = sid.split("_")[1:]
    return SandboxMeta(
        sandbox_id=sid,
        agent_id=agent_id,
        session_id=session_id,
        status="IDLE",
        created_at=last_used_at,
        last_used_at=last_used_at,
        active_run=active_run,
    )


def record(run_id: str) -> RunRecord:
    return RunRecord(
        run_id=run_id,
        started_at=NOW,
        duration_ms=5,
        tool="run_sql",
        language="sql",
        mode=OperatingMode.READ_ONLY,
        code="SELECT 1",
        outcome="OK",
    )


def test_lock_excludes_second_holder(tmp_path: Path) -> None:
    with lock(tmp_path, "agent_a_s1"):
        with pytest.raises(LockBusy), lock(tmp_path, "agent_a_s1", wait=False):
            pass
        with lock(tmp_path, "agent_a_s2", wait=False):  # other names are independent
            pass
    with lock(tmp_path, "agent_a_s1", wait=False):  # released
        pass


def test_meta_round_trip_leaves_no_temp_file(tmp_path: Path) -> None:
    m = meta("agent_a_s1", NOW, ActiveRun(run_id="r1", started_at=NOW))
    write_meta(tmp_path, m)
    assert read_meta(tmp_path) == m
    assert [p.name for p in tmp_path.iterdir()] == [SANDBOX_META_FILE]


def test_run_log_skips_repeated_run_id(tmp_path: Path) -> None:
    assert append_run(tmp_path, record("r1"))
    assert append_run(tmp_path, record("r2"))
    assert not append_run(tmp_path, record("r1"))
    lines = (tmp_path / RUNS_FILE).read_text().splitlines()
    assert [RunRecord.model_validate_json(line).run_id for line in lines] == ["r1", "r2"]


class FakeContainer:
    def __init__(self, docker: "FakeDocker", sid: str) -> None:
        self.docker, self.sid = docker, sid

    def remove(self, force: bool) -> None:
        self.docker.removed.append(self.sid)


class FakeDocker:
    """Only what GC uses: containers.list(all=True, filters={"label": ...}) and Container.remove."""

    def __init__(self) -> None:
        self.removed: list[str] = []
        self.containers = self

    def list(self, all: bool, filters: dict[str, Any]) -> list[FakeContainer]:
        return [FakeContainer(self, filters["label"].split("=", 1)[1])]


def sandbox(root: Path, sid: str, m: SandboxMeta | None = None, mtime: datetime = NOW) -> Path:
    d = root / sid
    d.mkdir()
    if m:
        write_meta(d, m)
    os.utime(d, (mtime.timestamp(), mtime.timestamp()))
    return d


def collect(root: Path, dry_run: bool = False) -> tuple[gc.GcReport, FakeDocker]:
    docker = FakeDocker()
    report = gc.run(root, TTL, RUN_TIMEOUT, docker, now=NOW, dry_run=dry_run)  # type: ignore[arg-type]
    return report, docker


def test_gc_ttl_from_last_activity(tmp_path: Path) -> None:
    old = NOW - timedelta(seconds=TTL + 1)
    sandbox(tmp_path, "agent_a_old", meta("agent_a_old", old))
    sandbox(tmp_path, "agent_a_new", meta("agent_a_new", NOW - timedelta(seconds=TTL - 1)))
    report, docker = collect(tmp_path)
    assert (report.removed, report.kept) == (["agent_a_old"], ["agent_a_new"])
    assert docker.removed == ["agent_a_old"]
    assert not (tmp_path / "agent_a_old").exists() and (tmp_path / "agent_a_new").exists()


def test_gc_dry_run_removes_nothing(tmp_path: Path) -> None:
    sandbox(tmp_path, "agent_a_old", meta("agent_a_old", NOW - timedelta(days=2)))
    report, docker = collect(tmp_path, dry_run=True)
    assert (report.removed, docker.removed) == (["agent_a_old"], [])
    assert (tmp_path / "agent_a_old").exists()


def test_gc_active_run_protects_until_stale(tmp_path: Path) -> None:
    old = NOW - timedelta(days=2)
    fresh = ActiveRun(run_id="r", started_at=NOW - timedelta(seconds=2 * RUN_TIMEOUT - 1))
    stale = ActiveRun(run_id="r", started_at=NOW - timedelta(seconds=2 * RUN_TIMEOUT + 1))
    sandbox(tmp_path, "agent_a_running", meta("agent_a_running", old, fresh))
    sandbox(tmp_path, "agent_a_crashed", meta("agent_a_crashed", old, stale))
    report, _ = collect(tmp_path)
    assert (report.removed, report.kept) == (["agent_a_crashed"], ["agent_a_running"])


def test_gc_skips_locked_sandbox(tmp_path: Path) -> None:
    sandbox(tmp_path, "agent_a_old", meta("agent_a_old", NOW - timedelta(days=2)))
    with lock(tmp_path, "agent_a_old"):
        report, docker = collect(tmp_path)
    assert (report.kept, docker.removed) == (["agent_a_old"], [])


@pytest.mark.parametrize("content", [None, "{not json", '{"spec_version": 1}'])
def test_gc_missing_or_corrupt_meta_uses_mtime(tmp_path: Path, content: str | None) -> None:
    for sid, mtime in [("agent_a_old", NOW - timedelta(days=2)), ("agent_a_new", NOW)]:
        d = tmp_path / sid
        d.mkdir()
        if content is not None:
            (d / SANDBOX_META_FILE).write_text(content)
        os.utime(d, (mtime.timestamp(), mtime.timestamp()))
    report, _ = collect(tmp_path)
    assert (report.removed, report.kept) == (["agent_a_old"], ["agent_a_new"])


def test_gc_leftover_creating_dirs(tmp_path: Path) -> None:
    sandbox(tmp_path, ".creating-agent_a_crashed", mtime=NOW - timedelta(days=2))
    sandbox(tmp_path, ".creating-agent_a_building", mtime=NOW - timedelta(days=2))
    sandbox(tmp_path, ".creating-agent_a_recent")
    with lock(tmp_path, "agent_a_building"):  # still being built by another caller
        report, _ = collect(tmp_path)
    assert report.removed == [".creating-agent_a_crashed"]
    assert sorted(report.kept) == [".creating-agent_a_building", ".creating-agent_a_recent"]
    assert (tmp_path / ".locks").is_dir()  # lock files are left in place


# Creation (§9.5). The sampler is replaced by a fake: creation logic needs no Spark.


class FakeSampler:
    def __init__(self, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    def __call__(self, client: Any, image: str, prod: Path, sandbox_dir: Path, *args: Any) -> SampleResult:
        self.calls += 1
        time.sleep(0.2)  # long enough for a second caller to arrive
        assert not any(sandbox_dir.iterdir())
        if self.fail:
            raise RuntimeError("sampler broke")
        (sandbox_dir / "silver").mkdir()
        return SAMPLE


SAMPLE = SampleResult(
    sampling=Sampling(
        anchor_table="silver.brands",
        anchor_key="brand_id",
        sample_size=5,
        seed=1,
        unrelated_sample_fraction=0.05,
        manifest_hash="sha256:x",
        prod_table_versions={},
    ),
    row_counts={"silver.brands": 5},
)


def manager(root: Path, monkeypatch: pytest.MonkeyPatch, sampler: FakeSampler) -> SandboxManager:
    monkeypatch.setattr("dbrix.lifecycle.manager.run_sampler", sampler)
    return SandboxManager(Config(sandbox_root=root), docker=None)  # type: ignore[arg-type]


def test_parallel_first_calls_create_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sampler = FakeSampler()
    m = manager(tmp_path, monkeypatch, sampler)
    with ThreadPoolExecutor(2) as pool:
        a, b = pool.map(lambda _: m.get_or_create("a", "s1"), range(2))
    assert a == b and sampler.calls == 1
    meta = read_meta(a.dir)
    assert (meta.status, meta.row_counts, meta.created_at) == ("IDLE", SAMPLE.row_counts, a.created_at)
    assert sorted(p.name for p in tmp_path.iterdir()) == [".locks", "agent_a_s1"]


def test_failed_creation_is_kept_then_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(RuntimeError):
        manager(tmp_path, monkeypatch, FakeSampler(fail=True)).get_or_create("a", "s1")
    meta = read_meta(tmp_path / ".creating-agent_a_s1")
    assert (meta.status, meta.last_error) == ("FAILED", "sampler broke")

    sampler = FakeSampler()
    sandbox = manager(tmp_path, monkeypatch, sampler).get_or_create("a", "s1")
    assert sampler.calls == 1 and read_meta(sandbox.dir).status == "IDLE"
    assert not (tmp_path / ".creating-agent_a_s1").exists()


def test_log_run_counts_activity_not_a_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    m = manager(tmp_path, monkeypatch, FakeSampler())
    sandbox = m.get_or_create("a", "s1")
    before = read_meta(sandbox.dir)
    m.log_run(sandbox, record("r1").model_copy(update={"outcome": "POLICY_BLOCKED"}))
    after = read_meta(sandbox.dir)
    assert (after.status, after.run_count) == ("IDLE", 0) and after.last_used_at > before.last_used_at
    assert (sandbox.dir / RUNS_FILE).exists()
