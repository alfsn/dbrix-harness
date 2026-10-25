import docker
import pytest

from dbrix.policy.sql import AUDITED_FUNCTIONS

from .conftest import spark_json

pytestmark = [pytest.mark.docker, pytest.mark.slow]


def test_spark_functions_match_the_audit(client: docker.DockerClient) -> None:
    """A new or removed Spark function means BLOCKED_FUNCTIONS needs a new review (§5.2)."""
    code = 'import json\nprint(json.dumps([r[0] for r in spark.sql("SHOW FUNCTIONS").collect()]))'
    assert sorted(spark_json(client, code, {})) == sorted(AUDITED_FUNCTIONS.read_text().split())
