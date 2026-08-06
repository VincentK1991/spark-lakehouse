"""
Applies table_definitions/tables/*.py as Iceberg schema contracts against the
lakekeeper catalog, via Spark's native DataFrameWriterV2 API.

Each module under tables/ is one table: a pyspark.sql.types.StructType for
its columns (so ETL code can import that exact object to validate against,
rather than trusting a re-parsed copy of it), plus optional partitioning and
table properties. Applying is CREATE TABLE IF NOT EXISTS only — a table that
already exists is left untouched, printed as a skip, never altered. Schema
changes to an existing table are a separate concern (migration / ALTER
TABLE), intentionally out of scope here, so this script can't silently
rewrite a table underneath running ETL.

This does not grant any new capability: the identity running it still needs
a `create` grant on the target namespace/warehouse (reconciler/grants.yaml,
table-definitions-writer group) — same as reconciler/grants.yaml governs for
anyone hand-writing `CREATE TABLE` or `.writeTo(...).create()` from a
notebook. A data scientist who already holds that grant (e.g.
sales-table-creators) can still create tables directly from their own ETL
code — this is an opt-in front door for schema a team wants defined and
reviewed in a PR up front, not a lock on table creation.

Runs as the `table-definitions` Keycloak service-account client
(client_credentials grant), not a logged-in human — this is meant to run
unattended (CI/deploy), so unlike query_orders.py it never prompts for a
username/password. Needs TABLE_DEFINITIONS_CLIENT_SECRET, read from the
repo-root .env (see docs/initial_stack.md for the dev-only secret value).

Run from the spark/ directory (reuses its pyspark environment):

    cd spark
    uv run python ../table_definitions/apply.py
"""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
from pathlib import Path
from types import ModuleType

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "spark"))

from query_orders import KEYCLOAK_TOKEN_URL, build_spark_conf

from table_definitions import tables as tables_pkg

CLIENT_ID = "table-definitions"

load_dotenv(REPO_ROOT / ".env")


def get_service_account_token() -> str:
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": os.environ["TABLE_DEFINITIONS_CLIENT_SECRET"],
            "scope": "lakekeeper",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_spark_session(token: str, app_name: str):
    from pyspark.sql import SparkSession

    conf = build_spark_conf(token, app_name)
    builder = SparkSession.builder
    spark = builder.config(conf=conf).getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE lakekeeper")
    return spark


def discover_table_modules() -> list[ModuleType]:
    modules = []
    for info in pkgutil.iter_modules(tables_pkg.__path__):
        if info.name.startswith("_"):
            continue
        modules.append(importlib.import_module(f"{tables_pkg.__name__}.{info.name}"))
    return modules


def apply_table(spark, module: ModuleType) -> None:
    full_name = f"lakekeeper.{module.TABLE}"
    namespace = module.TABLE.rsplit(".", 1)[0]

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakekeeper.{namespace}")

    if spark.catalog.tableExists(full_name):
        print(
            f"  {full_name}: already exists, contract not re-applied "
            "(schema changes need a migration)"
        )
        return

    writer = (
        spark.createDataFrame([], schema=module.SCHEMA)
        .writeTo(full_name)
        .using("iceberg")
    )

    description = getattr(module, "DESCRIPTION", None)
    if description:
        writer = writer.tableProperty("comment", description)

    for key, value in getattr(module, "PROPERTIES", {}).items():
        writer = writer.tableProperty(key, str(value))

    partition_by = getattr(module, "partition_by", None)
    if partition_by is not None:
        writer = writer.partitionedBy(*partition_by())

    writer.create()
    print(f"  {full_name}: created")


def main() -> None:
    token = get_service_account_token()
    spark = get_spark_session(token, "lakehouse-table-definitions")
    print(f"Authenticated as service account '{CLIENT_ID}'.")

    modules = discover_table_modules()
    if not modules:
        print(f"No table definitions found in {Path(tables_pkg.__file__).parent}")
        return

    print("Applying table definitions...")
    for module in modules:
        apply_table(spark, module)

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
