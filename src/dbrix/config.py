"""Every default and constant lives here, nowhere else (§10)."""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict

# Paths inside the dbrix-spark image and its containers.
DOCKERFILE = "docker/spark.Dockerfile"
DELTA_JARS = "/opt/ivy/jars/*.jar"
PROD_CATALOG_MOUNT = "/prod_catalog"
MANIFEST_MOUNT = "/manifest.yaml"

# Local Spark: data is small, so few shuffle partitions beat Spark's default of 200.
SPARK_DRIVER_MEMORY = "2g"
SPARK_SHUFFLE_PARTITIONS = 8

# File and dir names inside a catalog dir (prod or sandbox).
METASTORE_DIR = "_metastore"
CATALOG_META_FILE = "_catalog_meta.json"


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GeneratorConfig(_Section):
    seed: int = 42
    scale: float = 1.0


class ExecutorConfig(_Section):
    image: str = "dbrix-spark:0.1"


class Config(_Section):
    prod_catalog_dir: Path = Path("./prod_catalog")
    manifest: Path = Path("./manifests/influencer_marketing.yaml")
    generator: GeneratorConfig = GeneratorConfig()
    executor: ExecutorConfig = ExecutorConfig()


def load_config(path: Path = Path("dbrix.toml")) -> Config:
    """Defaults, overridden by `dbrix.toml` when it exists."""
    if not path.exists():
        return Config()
    return Config.model_validate(tomllib.loads(path.read_text()))
