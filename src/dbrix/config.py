"""Every default and constant lives here, nowhere else (§10)."""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

# Paths inside the dbrix-spark image and its containers.
DOCKERFILE = "docker/spark.Dockerfile"
DELTA_JARS = "/opt/ivy/jars/*.jar"
PROD_CATALOG_MOUNT = "/prod_catalog"
MANIFEST_MOUNT = "/manifest.yaml"
SANDBOX_MOUNT = "/sandbox"

# agent_id and session_id: no `_`, so `agent_{agent_id}_{session_id}` splits back into one pair (§4.4).
ID_PATTERN = r"[A-Za-z0-9-]{1,64}"

# Local Spark: data is small, so few shuffle partitions beat Spark's default of 200.
SPARK_DRIVER_MEMORY = "2g"
SPARK_SHUFFLE_PARTITIONS = 8

# Last stderr lines of a failed jobs container kept in its JobError (the Python traceback ends there).
JOB_ERROR_LINES = 20

# File and dir names inside a catalog dir (prod or sandbox).
METASTORE_DIR = "_metastore"
CATALOG_META_FILE = "_catalog_meta.json"


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


class Config(_Section):
    prod_catalog_dir: Path = Path("./prod_catalog")
    manifest: Path = Path("./manifests/influencer_marketing.yaml")
    generator: GeneratorConfig = GeneratorConfig()
    sampling: SamplingConfig = SamplingConfig()
    executor: ExecutorConfig = ExecutorConfig()


def load_config(path: Path = Path("dbrix.toml")) -> Config:
    """Defaults, overridden by `dbrix.toml` when it exists."""
    if not path.exists():
        return Config()
    return Config.model_validate(tomllib.loads(path.read_text()))
