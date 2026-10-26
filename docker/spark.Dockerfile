# dbrix-spark: one image, two roles (executor and jobs container). Build: python -m dbrix.executor.build_image
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --user-group --create-home dbrix

COPY --from=ghcr.io/astral-sh/uv:0.7.8 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_LINK_MODE=copy PATH=/opt/venv/bin:$PATH

WORKDIR /opt/dbrix
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups --no-install-project

# Resolve the Delta jars for the installed delta-spark once, at build time, into /opt/ivy/jars.
# At runtime spark.jars points there (dbrix.config.DELTA_JARS), so nothing is downloaded.
RUN python -c "from delta import configure_spark_with_delta_pip as c; from pyspark.sql import SparkSession as S; \
c(S.builder.master('local[1]').config('spark.jars.ivy', '/opt/ivy')).getOrCreate().stop()" \
    && chmod -R a+rX /opt/ivy

COPY src ./src
RUN uv sync --frozen --no-default-groups --no-editable

# Hadoop needs a named user; every container runs as this one (§7).
USER 10001:10001
