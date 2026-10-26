"""Every default and constant lives here, nowhere else (§10)."""

import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

# Paths inside the dbrix-spark image and its containers.
DOCKERFILE = "docker/spark.Dockerfile"
DELTA_JARS = "/opt/ivy/jars/*.jar"
PROD_CATALOG_MOUNT = "/prod_catalog"
MANIFEST_MOUNT = "/manifest.yaml"
SANDBOX_MOUNT = "/sandbox"

# The medallion layers; each is one schema in every catalog (§3, §6.1).
Layer = Literal["bronze", "silver", "gold"]
SCHEMAS: tuple[Layer, ...] = get_args(Layer)

# agent_id and session_id: no `_`, so `agent_{agent_id}_{session_id}` splits back into one pair (§4.4).
ID_PATTERN = r"[A-Za-z0-9-]{1,64}"
# run_id names the result file `_results/<run_id>.csv` (§6.3).
RUN_ID_PATTERN = r"[A-Za-z0-9_-]{1,128}"

# Local Spark: data is small, so few shuffle partitions beat Spark's default of 200.
SPARK_DRIVER_MEMORY = "2g"
SPARK_SHUFFLE_PARTITIONS = 8

# Last stderr lines of a failed jobs container kept in its JobError (the Python traceback ends there).
JOB_ERROR_LINES = 20

# File and dir names inside a catalog dir (prod or sandbox).
METASTORE_DIR = "_metastore"
CATALOG_META_FILE = "_catalog_meta.json"
RESULTS_DIR = "_results"  # in a sandbox dir; the agent sees it as RESULTS_PATH (§8.4)
RESULTS_PATH = "/results"
CATALOG_DOCS_DIR = "_catalog"  # in a sandbox dir; the agent sees it as CATALOG_PATH (§8.4)
CATALOG_PATH = "/catalog"
SAMPLE_ROWS = 5  # per table in the catalog docs

# Sandbox root layout (§4.3, §9).
SANDBOX_META_FILE = "_sandbox_meta.json"
RUNS_FILE = "runs.jsonl"
LOCKS_DIR = ".locks"
CREATING_PREFIX = ".creating-"
EXECUTORS_LOCK = "_executors"  # lock name for the container limit; sandbox ids start with `agent_`, so no clash
SLOT_RETRY_SECONDS = 1.0  # how often a call waiting for a free executor slot looks again

# Executor (§6.3).
SANDBOX_LABEL = "dbrix.sandbox_id"
EXECUTOR_START_TIMEOUT = 300  # seconds until the runtime's ready line (Spark start)
MAX_RESPONSE_BYTES = 1_000_000  # one response line from the untrusted executor
OUTPUT_LIMIT = 8192  # characters of stdout and of stderr kept per run


class OperatingMode(StrEnum):
    """Set by the host per tool call, never by the agent (§8.2)."""

    READ_ONLY = "READ_ONLY"
    READ_WRITE = "READ_WRITE"


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GeneratorConfig(_Section):
    seed: int = 42
    scale: float = 1.0


class SamplingConfig(_Section):
    sample_size: PositiveInt = 5
    unrelated_sample_fraction: float = Field(default=0.05, ge=0, le=1)
    anchor_table: str | None = None  # None: the manifest's default_anchor
    anchor_key: str | None = None
    seed: int | None = None  # None: xxhash64(agent_id, session_id), computed by the sampler job

    @model_validator(mode="after")
    def _anchor_pair(self) -> "SamplingConfig":
        if (self.anchor_table is None) != (self.anchor_key is None):
            raise ValueError("set both anchor_table and anchor_key, or neither")
        return self


class ExecutorConfig(_Section):
    image: str = "dbrix-spark:0.1"
    run_timeout: PositiveInt = 120  # seconds of running time per run
    max_containers: PositiveInt = 2  # executor containers running at once, across host processes
    memory: str = "4g"
    cpus: float = Field(default=2, gt=0)
    preview_rows: PositiveInt = 50
    result_file_max_rows: PositiveInt = 10000


class LifecycleConfig(_Section):
    ttl_seconds: PositiveInt = 86400  # from last activity (§9.4)


class Config(_Section):
    prod_catalog_dir: Path = Path("./prod_catalog")
    sandbox_root: Path = Path("./sandbox_catalog")
    manifest: Path = Path("./manifests/influencer_marketing.yaml")
    default_mode: OperatingMode = OperatingMode.READ_ONLY
    generator: GeneratorConfig = GeneratorConfig()
    sampling: SamplingConfig = SamplingConfig()
    executor: ExecutorConfig = ExecutorConfig()
    lifecycle: LifecycleConfig = LifecycleConfig()


def load_config(path: Path = Path("dbrix.toml")) -> Config:
    """Defaults, overridden by `dbrix.toml` when it exists."""
    if not path.exists():
        return Config()
    return Config.model_validate(tomllib.loads(path.read_text()))
