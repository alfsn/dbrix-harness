import json
from pathlib import Path

import docker
import pytest

from dbrix.config import CATALOG_META_FILE
from dbrix.generator import run_generator
from dbrix.manifest import manifest_hash

from .conftest import CONFIG, TABLES, content_hashes, spark_json

pytestmark = pytest.mark.docker

DOMAIN_CHECKS = """
import json
q = lambda sql: spark.sql(sql).first()[0]
t =lambda name: f"delta.`/a/{name.replace('.', '/')}`"
print(json.dumps({
    "organic_share": q(f"SELECT avg(CAST(campaign_id IS NULL AS INT)) FROM {t('silver.posts')}"),
    "posts_by_uncontracted_creator": q(f'''
        SELECT count(*) FROM {t('silver.posts')} p
        JOIN {t('silver.social_accounts')} a ON a.account_id = p.account_id
        LEFT ANTI JOIN {t('silver.campaign_creators')} cc ON cc.campaign_id = p.campaign_id AND cc.creator_id = a.creator_id
        WHERE p.campaign_id IS NOT NULL'''),
    "duplicate_post_events": q(f'''
        SELECT count(*) - count(DISTINCT post_id, raw_payload) FROM {t('bronze.raw_post_events')}'''),
    "metric_days_before_post": q(f'''
        SELECT count(*) FROM {t('silver.post_metrics_daily')} m JOIN {t('silver.posts')} p USING (post_id)
        WHERE m.metric_date < to_date(p.posted_at)'''),
}))
"""


def test_generator_is_deterministic_across_core_counts(client: docker.DockerClient, tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    run_generator(client, CONFIG.executor.image, a, CONFIG.manifest, seed=7, scale=0.05, cpuset_cpus="0")
    run_generator(client, CONFIG.executor.image, b, CONFIG.manifest, seed=7, scale=0.05)

    hashes = content_hashes(client, {"/a": a, "/b": b})
    assert hashes["/a"] == hashes["/b"]
    assert all(h is not None for h in hashes["/a"].values())

    meta = json.loads((a / CATALOG_META_FILE).read_text())
    assert meta["manifest_hash"] == manifest_hash(CONFIG.manifest)
    assert (meta["seed"], meta["scale"]) == (7, 0.05)
    assert set(meta["table_versions"]) == set(TABLES)

    checks = spark_json(client, DOMAIN_CHECKS, {"/a": a})
    assert 0.05 < checks["organic_share"] < 0.15
    assert checks["posts_by_uncontracted_creator"] == 0
    assert checks["duplicate_post_events"] > 0
    assert checks["metric_days_before_post"] == 0


def test_generator_refuses_non_empty_catalog(client: docker.DockerClient, tmp_path: Path) -> None:
    (tmp_path / "old").write_text("x")
    with pytest.raises(FileExistsError):
        run_generator(client, CONFIG.executor.image, tmp_path, CONFIG.manifest, seed=1, scale=0.05)


@pytest.mark.slow
def test_scale_1_finishes_under_2_minutes(prod_scale_1: tuple[Path, float]) -> None:
    """Acceptance §11.4 (1). Integrity is checked inside the job; a violation fails it."""
    assert prod_scale_1[1] < 120
