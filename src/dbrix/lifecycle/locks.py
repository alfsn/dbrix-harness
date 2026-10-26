"""One exclusive `fcntl` lock per name in `sandbox_root/.locks/`, outside the sandbox dir (§9.5)."""

import fcntl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dbrix.config import LOCKS_DIR


class LockBusy(Exception):
    pass


@contextmanager
def lock(sandbox_root: Path, name: str, wait: bool = True) -> Iterator[None]:
    """Hold the lock `name` for the block. With `wait=False`, raises LockBusy instead of waiting.
    Each call opens its own file, so threads of one process exclude each other too."""
    path = sandbox_root / LOCKS_DIR / f"{name}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError as e:
            raise LockBusy(name) from e
        yield  # closing the file releases the lock
