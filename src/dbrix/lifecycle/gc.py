"""Garbage collection of expired sandboxes (§9.4). Idempotent. Run hourly: python -m dbrix.lifecycle.gc"""

import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from docker import DockerClient, from_env
from pydantic import BaseModel, ValidationError

from dbrix.config import CREATING_PREFIX, load_config
from dbrix.executor.client import remove_executors
from dbrix.lifecycle.locks import LockBusy, lock
from dbrix.lifecycle.meta import read_meta


class GcReport(BaseModel):
    removed: list[str] = []
    kept: list[str] = []


def _mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, UTC)


def _expired(sandbox_dir: Path, ttl: timedelta, run_timeout: int, now: datetime) -> bool:
    """A missing or corrupt meta counts as last used at the dir's mtime. A run younger than 2 × run_timeout
    protects the sandbox; an older one is a crashed run."""
    try:
        meta = read_meta(sandbox_dir)
    except (OSError, ValidationError):
        return now - _mtime(sandbox_dir) > ttl
    if meta.active_run and now - meta.active_run.started_at < timedelta(seconds=2 * run_timeout):
        return False
    return now - meta.last_used_at > ttl


def run(
    sandbox_root: Path,
    ttl_seconds: int,
    run_timeout: int,
    docker: DockerClient,
    now: datetime | None = None,
    dry_run: bool = False,
) -> GcReport:
    now = now or datetime.now(UTC)
    ttl = timedelta(seconds=ttl_seconds)
    report = GcReport()
    for d in sorted(sandbox_root.glob("agent_*")) + sorted(sandbox_root.glob(f"{CREATING_PREFIX}*")):
        sid = d.name.removeprefix(CREATING_PREFIX)
        try:
            # Never wait: a busy lock means the sandbox is in use. Check again under the lock, because a run may
            # have finished between the glob and taking it.
            with lock(sandbox_root, sid, wait=False):
                if d.name.startswith(CREATING_PREFIX):
                    expired = now - _mtime(d) > ttl
                else:
                    expired = _expired(d, ttl, run_timeout, now)
                if expired and not dry_run:
                    remove_executors(docker, sid)
                    shutil.rmtree(d)
        except LockBusy:
            expired = False
        (report.removed if expired else report.kept).append(d.name)
    return report


if __name__ == "__main__":
    cfg = load_config()
    report = run(cfg.sandbox_root, cfg.lifecycle.ttl_seconds, cfg.executor.run_timeout, from_env())
    print(report.model_dump_json(indent=2))
