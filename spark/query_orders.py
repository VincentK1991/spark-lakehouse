"""
Read sales.orders from the lakehouse with PySpark, as a specific Keycloak user.

    uv run python query_orders.py [username] [password]

Defaults to alice/test1234, or SPARK_USERNAME/SPARK_PASSWORD from spark/.env if
set (copy .env.example to get started) — see `get_credentials`. Access is
enforced by Lakekeeper/OpenFGA exactly as declared in reconciler/grants.yaml —
e.g. carol can read but not write, and erin (no grant on this table) cannot see
it at all.

Runs on the host against the docker-compose stack. Requires the one-time DNS
setup described in docs/CORS_issues.md:

    echo "127.0.0.1 minio.localhost" | sudo tee -a /etc/hosts

That is the local stand-in for the DNS record a real deployment would have:
Lakekeeper advertises the storage endpoint to every client, so the hostname it
advertises has to resolve for them.

The helpers here (`get_credentials`, `get_token`, `build_spark_conf`,
`get_spark_session`) are shared with the notebooks in this folder, so there is
one implementation of the login/session-setup logic, not several. They are
also reused, unmodified, by the JupyterHub singleuser image (see
spark/docker/Dockerfile and docs/team_jupyter_hub.ipynb): KEYCLOAK_TOKEN_URL/
CATALOG_URL/SPARK_MASTER_URL are env-overridable specifically so that
in-cluster path can point at Docker-network hostnames and a real Spark
cluster instead of localhost/local[*], without forking this file.
"""

from __future__ import annotations

import getpass
import os
import socket
import sys

import requests
from dotenv import load_dotenv

KEYCLOAK_TOKEN_URL = os.environ.get(
    "KEYCLOAK_TOKEN_URL", "http://localhost:8080/realms/lakehouse/protocol/openid-connect/token"
)
CATALOG_URL = os.environ.get("CATALOG_URL", "http://localhost:8181/catalog")
WAREHOUSE = "local"
SPARK_CLIENT_ID = "spark"
ICEBERG_VERSION = "1.10.0"

load_dotenv()  # loads spark/.env if present; a no-op otherwise


def get_credentials(username: str | None = None, password: str | None = None) -> tuple[str, str]:
    """Resolve (username, password), preferring, in order: explicit args,
    SPARK_USERNAME/SPARK_PASSWORD (from the environment or spark/.env), then an
    interactive prompt. Lets scripts, notebooks, and `.env` all share one
    login path without forcing anyone to retype a password every run.
    """
    username = username or os.environ.get("SPARK_USERNAME")
    password = password or os.environ.get("SPARK_PASSWORD")
    if username and password:
        return username, password
    username = username or input("Keycloak username (e.g. alice): ")
    password = password or getpass.getpass("Password: ")
    return username, password


def get_token(username: str, password: str) -> str:
    """Log in to Keycloak as a human user (ROPC — dev-only, see docs)."""
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "password",
            "client_id": SPARK_CLIENT_ID,
            "scope": "lakekeeper",
            "username": username,
            "password": password,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def build_spark_conf(token: str, app_name: str):
    """Spark config for the Lakekeeper Iceberg REST catalog, as `token`'s user.

    Note there is deliberately no `s3.endpoint` here: Lakekeeper returns the
    storage endpoint in every LoadTable response and Iceberg gives that
    server-provided table config precedence, so setting it client-side would be
    silently ignored.

    `master` defaults to the host-run `local[*]` flow. Set SPARK_MASTER_URL
    (e.g. `spark://spark-master:7077`, as the JupyterHub notebook image does)
    to run against the shared standalone cluster instead -- see
    docs/team_jupyter_hub.ipynb.
    """
    import pyspark
    from pyspark.conf import SparkConf

    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    in_cluster = master != "local[*]"

    conf = SparkConf().setMaster(master).setAppName(app_name)
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
        "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
        "spark.sql.catalog.lakekeeper.token": token,
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
        # Lakekeeper's vended per-table config reliably carries the S3
        # endpoint (see the module docstring / docs/CORS_issues.md) but not
        # the region, so the AWS SDK's region-provider chain finds nothing
        # and refuses to guess — same reason trino/catalog/lakekeeper.properties
        # sets s3.region explicitly. Value matches the warehouse's storage
        # profile in lakekeeper/bootstrap.py.
        "spark.sql.catalog.lakekeeper.client.region": "us-east-1",
        # Disabled for the host-run local[*] flow: with several sessions
        # started one after another on the same machine, each would try to
        # bind port 4040 and increment past it, which isn't worth the churn
        # for a UI nobody was reaching anyway. In-cluster this is safe (one
        # container == its own network namespace) and reachable through
        # JupyterHub's proxy, so it's turned back on below.
        "spark.ui.enabled": "false",
    }

    if in_cluster:
        # Jars are baked into the shared spark-master/spark-worker/notebook
        # image (spark/docker/Dockerfile) instead of resolved here, so every
        # role runs an identical classpath -- no spark.jars.packages.
        cores_max = os.environ.get("SPARK_CORES_MAX")
        if cores_max:
            settings["spark.cores.max"] = cores_max
            settings["spark.executor.cores"] = "1"
        settings["spark.driver.memory"] = "2g"
        settings["spark.executor.memory"] = "2g"
        # Reachable at /user/<name>/proxy/4040/ via jupyter-server-proxy,
        # which tunnels it through the Hub -- only Jupyter's own port is
        # published from a spawned singleuser container otherwise.
        settings["spark.ui.enabled"] = "true"
        settings["spark.ui.port"] = "4040"
        # #1 client-mode failure: executors connect back to the driver, and
        # Spark won't reliably auto-detect a reachable address for a
        # container on a Docker network, so it's set explicitly here rather
        # than left to autodetection.
        settings["spark.driver.host"] = socket.gethostbyname(socket.gethostname())
        settings["spark.driver.bindAddress"] = "0.0.0.0"
    else:
        spark_minor = ".".join(pyspark.__version__.split(".")[:2])
        settings["spark.jars.packages"] = (
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor}_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
        )

    for key, value in settings.items():
        conf.set(key, value)
    return conf


def get_spark_session(username: str, password: str, app_name: str):
    """Log in as `username` and return a ready-to-use SparkSession."""
    from pyspark.sql import SparkSession

    token = get_token(username, password)
    conf = build_spark_conf(token, app_name)
    # pyspark stubs type `builder` as a classproperty, so attribute access on
    # it is flagged spuriously — suppress just that.
    builder = SparkSession.builder
    spark = builder.config(conf=conf).getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE lakekeeper")
    return spark


def main() -> None:
    username = sys.argv[1] if len(sys.argv) > 1 else None
    password = sys.argv[2] if len(sys.argv) > 2 else None
    username, password = get_credentials(username, password)

    spark = get_spark_session(username, password, "lakehouse-query-orders")
    print(f"Logged in as {username}.")
    spark.sql("SELECT * FROM sales.orders").show()
    spark.stop()


if __name__ == "__main__":
    main()
