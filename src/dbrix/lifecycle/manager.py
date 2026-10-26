"""Phase 5: create sandboxes once, run code one call at a time, keep at most `max_containers` executors (§9.5, §6.3).

The host keeps one `Executor` handle per sandbox it started. Everything shared between host processes is on disk
(meta, locks) or in Docker (container labels)."""

import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from docker import DockerClient

from dbrix.config import CREATING_PREFIX, EXECUTORS_LOCK, SANDBOX_LABEL, SLOT_RETRY_SECONDS, Config, OperatingMode
from dbrix.executor.client import (
    Executor,
    ExecutorError,
    ExecutorTimeout,
    remove_executors,
    restart_executor,
    start_executor,
)
from dbrix.executor.client import run as run_executor
from dbrix.executor.protocol import AgentError, RunRequest, RunResult
from dbrix.ids import sandbox_id
from dbrix.lifecycle.locks import LockBusy, lock
from dbrix.lifecycle.meta import ActiveRun, SandboxMeta, now, read_meta, write_meta
from dbrix.lifecycle.runlog import Approval, Outcome, RunRecord, append_run
from dbrix.manifest import load_manifest
from dbrix.sampler import run_sampler

# What the agent sees when its run was stopped (§6.3). No host details: they would break the illusion (§6.5).
LOST_STATE = "Temp views and cached data are lost; tables are kept."


class ExecutorBusy(Exception):
    """Every executor slot stayed busy for `run_timeout` seconds."""


@dataclass(frozen=True)
class Sandbox:
    agent_id: str
    session_id: str
    dir: Path
    created_at: datetime  # changes when the sandbox was GC'd and rebuilt (§8.2 recreation notice)

    @property
    def id(self) -> str:
        return self.dir.name


class SandboxManager:
    def __init__(self, cfg: Config, docker: DockerClient) -> None:
        self._cfg = cfg
        self._docker = docker
        self._root = cfg.sandbox_root
        self._catalog = load_manifest(cfg.manifest).catalog
        self._executors: dict[str, Executor] = {}

    def get_or_create(self, agent_id: str, session_id: str) -> Sandbox:
        """Build the sandbox on first use. Two callers at once build it once."""
        sid = sandbox_id(agent_id, session_id)
        with lock(self._root, sid):
            return self._ensure(agent_id, session_id, sid)

    def run(
        self,
        sandbox: Sandbox,
        request: RunRequest,
        tool: str,
        mode: OperatingMode,
        approval: Approval | None = None,
    ) -> tuple[Sandbox, RunResult]:
        """Run one request and log it. Runs on one sandbox wait for each other. The sandbox is rebuilt when GC
        removed it since `get_or_create`; the returned Sandbox is the one the code ran on. Raises ExecutorBusy."""
        with lock(self._root, sandbox.id):
            sandbox = self._ensure(sandbox.agent_id, sandbox.session_id, sandbox.id)
            executor = self._executor(sandbox)
            started = now()
            meta = read_meta(sandbox.dir)
            active = ActiveRun(run_id=request.run_id, started_at=started)
            write_meta(sandbox.dir, meta.model_copy(update={"status": "ACTIVE", "active_run": active}))
            result, outcome = self._call(executor, request)
            record = RunRecord(
                run_id=request.run_id,
                started_at=started,
                duration_ms=result.duration_ms,
                tool=tool,
                language=request.language,
                mode=mode,
                code=request.code,
                rewritten_sql=result.rewritten_sql,
                outcome=outcome,
                rule_id=result.error.rule_id if result.error else None,
                error_message=result.error.message if result.error else None,
                row_count=result.row_count,
                writes=result.writes_applied,
                approval=approval,
            )
            self._record(sandbox.dir, record, ran=True)
            return sandbox, result

    def log_run(self, sandbox: Sandbox, record: RunRecord) -> None:
        """Log a call that did not run (POLICY_BLOCKED on the host, REJECTED). Skipped when the sandbox is gone."""
        with lock(self._root, sandbox.id):
            if sandbox.dir.exists():
                self._record(sandbox.dir, record, ran=False)

    def _ensure(self, agent_id: str, session_id: str, sid: str) -> Sandbox:
        """Under the sandbox lock: the sandbox, built in `.creating-<id>` and renamed when missing (§9.5)."""
        final = self._root / sid
        if not final.exists():
            creating = self._root / f"{CREATING_PREFIX}{sid}"
            shutil.rmtree(creating, ignore_errors=True)  # left from a crash or a failed build
            creating.mkdir(parents=True)
            t = now()
            meta = SandboxMeta(
                sandbox_id=sid,
                agent_id=agent_id,
                session_id=session_id,
                status="CREATING",
                created_at=t,
                last_used_at=t,
            )
            try:
                sample = run_sampler(
                    self._docker,
                    self._cfg.executor.image,
                    self._cfg.prod_catalog_dir,
                    creating,
                    self._cfg.manifest,
                    agent_id,
                    session_id,
                    self._cfg.sampling,
                )
            except Exception as e:
                write_meta(creating, meta.model_copy(update={"status": "FAILED", "last_error": str(e)}))
                raise
            update = {"status": "IDLE", "sampling": sample.sampling, "row_counts": sample.row_counts}
            write_meta(creating, meta.model_copy(update=update))
            creating.rename(final)
        return Sandbox(agent_id, session_id, final, read_meta(final).created_at)

    def _executor(self, sandbox: Sandbox) -> Executor:
        """Under the sandbox lock: a running executor for the sandbox, started or restarted when needed."""
        meta = read_meta(sandbox.dir)
        executor = self._executors.get(sandbox.id)
        if executor and executor.container.id != meta.container_id:  # the sandbox was GC'd and rebuilt
            executor = None
        if executor:
            executor.container.reload()
            if executor.container.status == "running":
                return executor
        else:
            remove_executors(self._docker, sandbox.id)  # one we can't attach to without losing lines
        deadline = time.monotonic() + self._cfg.executor.run_timeout
        while True:
            # Count, evict, and start as one step across host processes.
            with lock(self._root, EXECUTORS_LOCK):
                if self._free_slot(sandbox.id):
                    if executor:
                        restart_executor(self._docker, executor)
                    else:
                        executor = start_executor(
                            self._docker,
                            self._cfg.executor.image,
                            sandbox.dir,
                            sandbox.id,
                            self._catalog,
                            self._cfg.executor,
                        )
                    break
            if time.monotonic() > deadline:
                raise ExecutorBusy("executor busy, try again")
            time.sleep(SLOT_RETRY_SECONDS)
        self._executors[sandbox.id] = executor
        write_meta(sandbox.dir, meta.model_copy(update={"container_id": executor.container.id}))
        return executor

    def _free_slot(self, sid: str) -> bool:
        """Under the executors lock: true when one more executor may run, after stopping the least recently used
        one whose sandbox is not running a run (its lock is free)."""
        running = self._docker.containers.list(filters={"label": SANDBOX_LABEL, "status": "running"})
        others = [c.labels[SANDBOX_LABEL] for c in running if c.labels[SANDBOX_LABEL] != sid]
        if len(others) < self._cfg.executor.max_containers:
            return True
        for other in sorted(others, key=self._last_used):
            try:
                with lock(self._root, other, wait=False):
                    for c in self._docker.containers.list(filters={"label": f"{SANDBOX_LABEL}={other}"}):
                        c.stop(timeout=0)  # kept: it restarts on its sandbox's next run
                    return True
            except LockBusy:
                continue
        return False

    def _last_used(self, sid: str) -> datetime:
        try:
            return read_meta(self._root / sid).last_used_at
        except (OSError, ValueError):
            return datetime.min.replace(tzinfo=UTC)

    def _call(self, executor: Executor, request: RunRequest) -> tuple[RunResult, Outcome]:
        """Send the request. A timeout or a broken answer leaves the container stopped; the next run restarts it."""
        timeout = self._cfg.executor.run_timeout
        start = time.monotonic()
        try:
            result = run_executor(executor, request, timeout)
        except ExecutorTimeout:
            message = f"The run took longer than {timeout} s and was stopped. {LOST_STATE}"
            return self._failed(start, "TimeoutError", message), "TIMEOUT"
        except ExecutorError:
            message = f"Spark failed and was restarted. {LOST_STATE}"
            return self._failed(start, "ExecutorError", message), "RUNTIME_ERROR"
        if result.ok:
            return result, "OK"
        blocked = result.error is not None and result.error.type == "SecurityException"
        return result, "POLICY_BLOCKED" if blocked else "RUNTIME_ERROR"

    @staticmethod
    def _failed(start: float, error_type: str, message: str) -> RunResult:
        duration_ms = int((time.monotonic() - start) * 1000)
        return RunResult(ok=False, duration_ms=duration_ms, error=AgentError(type=error_type, message=message))

    @staticmethod
    def _record(sandbox_dir: Path, record: RunRecord, ran: bool) -> None:
        """Under the sandbox lock: log the call and update the meta. Code that reached the executor counts as a run
        and sets the status (§9.2); a call that never ran only counts as activity."""
        meta = read_meta(sandbox_dir)
        update: dict[str, object] = {"last_used_at": now(), "active_run": None}
        if append_run(sandbox_dir, record) and ran:
            update |= {
                "run_count": meta.run_count + 1,
                "status": "IDLE" if record.outcome == "OK" else "FAILED",
                "last_error": record.error_message,
            }
        write_meta(sandbox_dir, meta.model_copy(update=update))
