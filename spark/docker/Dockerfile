# Multi-stage build shared by spark-master, spark-worker, and the
# JupyterHub singleuser notebook image (see docs/team_jupyter_hub.ipynb).
# All three roles come from the `base` stage, so they run the exact same
# Spark distribution + jar set -- no driver/worker classpath skew across the
# standalone cluster. Building all three from one Dockerfile (via `target:`
# in docker-compose.yml) avoids relying on docker-compose's build ordering
# to get a shared image tag in place first.
#
# Base is the official Apache Spark image, not bitnami/spark: Bitnami moved
# free-tier tags off Docker Hub in 2025. Its entrypoint passes non-driver/
# executor commands straight through (`exec "$@"`), which is what lets
# spark-master/spark-worker run `spark-class ...` directly as the container
# command.
ARG SPARK_IMAGE_TAG=3.5.9-python3

FROM apache/spark:${SPARK_IMAGE_TAG} AS base

# Keep in sync with spark/query_orders.py's ICEBERG_VERSION constant.
ARG ICEBERG_VERSION=1.10.0
ARG SPARK_MINOR=3.5
ARG SCALA_VERSION=2.12

USER root
RUN curl -fL -o "${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
 && curl -fL -o "${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
 && chown spark:spark "${SPARK_HOME}"/jars/iceberg-*.jar
USER spark

# ─────────────────────── notebook: JupyterHub singleuser image ───────────────────────
FROM base AS notebook

USER root
# uv itself, copied from astral's distroless image (same pattern this repo
# already uses for the bootstrap/reconciler one-shot jobs in docker-compose.yml).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /opt/notebook-app
COPY docker/notebook/pyproject.toml docker/notebook/uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/notebook-app/.venv
# uv's default python-install dir lives under $HOME (/root during this
# build), which the spark user can't read at runtime -- point it somewhere
# world-readable instead so the venv's interpreter symlink stays usable
# after USER switches to spark below.
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python
RUN uv sync --frozen --no-dev

# Shared login/session-setup helper (spark/query_orders.py) so the notebook
# image and the host-run flow share one implementation, not two.
COPY query_orders.py /opt/notebook-app/query_orders.py

COPY docker/notebook/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh && chown -R spark:spark /opt/notebook-app /opt/uv-python
# Base image sets spark's home dir to /nonexistent; give it a real,
# writable one for Jupyter's runtime/config dirs and notebook storage. The
# ENV is required too, not just the directory: the `spark` user's /etc/passwd
# entry still says /nonexistent, and without $HOME set explicitly, Jupyter's
# home-relative defaults (runtime_dir, config/data dirs) fall back to that
# passwd entry and fail with PermissionError trying to create it.
RUN mkdir -p /home/spark && chown spark:spark /home/spark
ENV HOME=/home/spark

ENV PATH="/opt/notebook-app/.venv/bin:${PATH}"
# In-cluster defaults for query_orders.py's env-var overrides -- see
# docs/team_jupyter_hub.ipynb "Extending spark/query_orders.py".
ENV SPARK_MASTER_URL="spark://spark-master:7077" \
    KEYCLOAK_TOKEN_URL="http://keycloak:8080/realms/lakehouse/protocol/openid-connect/token" \
    CATALOG_URL="http://lakekeeper:8181/catalog"

USER spark
WORKDIR /home/spark
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["jupyterhub-singleuser"]
