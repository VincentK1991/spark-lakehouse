"""
One-shot bootstrap for the lakehouse stack. Idempotent — safe to re-run.

- Bootstraps Lakekeeper (first caller becomes the technical admin).
- Grants the real human admin (ADMIN_EMAIL) admin rights.
- Creates the demo warehouse "local" backed by the MinIO bucket.
- Creates demo namespaces/tables used by reconciler/grants.yaml.
"""

import os
import sys
import time

import pyarrow as pa
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

LAKEKEEPER_URL = os.environ["LAKEKEEPER_URL"]
MANAGEMENT_URL = f"{LAKEKEEPER_URL}/management"
CATALOG_URL = f"{LAKEKEEPER_URL}/catalog"
KEYCLOAK_TOKEN_URL = os.environ["KEYCLOAK_TOKEN_URL"]
KEYCLOAK_ADMIN_API_URL = os.environ["KEYCLOAK_ADMIN_API_URL"]
RECONCILER_CLIENT_ID = os.environ["RECONCILER_CLIENT_ID"]
RECONCILER_CLIENT_SECRET = os.environ["RECONCILER_CLIENT_SECRET"]
ADMIN_EMAIL = os.environ["ADMIN_EMAIL"]
WAREHOUSE_NAME = os.environ["WAREHOUSE_NAME"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_LAKEKEEPER_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_LAKEKEEPER_SECRET_KEY"]


def get_token() -> str:
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": RECONCILER_CLIENT_ID,
            "client_secret": RECONCILER_CLIENT_SECRET,
            "scope": "lakekeeper",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def wait_for_lakekeeper(token: str, attempts: int = 30) -> None:
    for _ in range(attempts):
        try:
            resp = requests.get(
                f"{MANAGEMENT_URL}/v1/info",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise SystemExit("Lakekeeper never became ready")


def ensure_bootstrapped(token: str) -> None:
    info = requests.get(f"{MANAGEMENT_URL}/v1/info", headers={"Authorization": f"Bearer {token}"})
    info.raise_for_status()
    if info.json().get("bootstrapped"):
        print("Lakekeeper already bootstrapped.")
        return
    resp = requests.post(
        f"{MANAGEMENT_URL}/v1/bootstrap",
        headers={"Authorization": f"Bearer {token}"},
        json={"accept-terms-of-use": True},
    )
    resp.raise_for_status()
    print("Lakekeeper bootstrapped.")


def resolve_keycloak_subject(token: str, email: str) -> str:
    resp = requests.get(
        f"{KEYCLOAK_ADMIN_API_URL}/users",
        headers={"Authorization": f"Bearer {token}"},
        params={"email": email, "exact": "true"},
    )
    resp.raise_for_status()
    users = resp.json()
    if not users:
        raise SystemExit(f"No Keycloak user found with email {email}")
    return users[0]["id"]


def resolve_keycloak_service_account_subject(token: str, client_id: str) -> str:
    username = f"service-account-{client_id}"
    resp = requests.get(
        f"{KEYCLOAK_ADMIN_API_URL}/users",
        headers={"Authorization": f"Bearer {token}"},
        params={"username": username, "exact": "true"},
    )
    resp.raise_for_status()
    users = resp.json()
    if not users:
        raise SystemExit(f"No Keycloak service-account user found for client {client_id}")
    return users[0]["id"]


def ensure_user_permission(
    token: str, assignments_path: str, permission_type: str, principal: str
) -> None:
    full_path = f"{MANAGEMENT_URL}{assignments_path}"
    current = requests.get(full_path, headers={"Authorization": f"Bearer {token}"})
    current.raise_for_status()
    already_granted = any(
        a.get("type") == permission_type and a.get("user") == principal
        for a in current.json().get("assignments", [])
    )
    if already_granted:
        return
    # Re-writing an already-existing tuple is rejected with 409
    # (TupleAlreadyExistsError), so the GET-based check above is required for
    # idempotency, not just an optimization.
    resp = requests.post(
        full_path,
        headers={"Authorization": f"Bearer {token}"},
        json={"writes": [{"type": permission_type, "user": principal}]},
    )
    resp.raise_for_status()


def ensure_admin_granted(token: str, subject_id: str) -> None:
    principal = f"oidc~{subject_id}"
    ensure_user_permission(token, "/v1/permissions/server/assignments", "admin", principal)
    ensure_user_permission(token, "/v1/permissions/project/assignments", "project_admin", principal)
    print(f"Granted admin rights to {principal} ({ADMIN_EMAIL}).")


def ensure_trino_read_access(token: str, warehouse_id: str) -> None:
    """Trino/SQLPad run as one shared, trusted identity for observability
    (see docs/initial_stack.md) — grant it broad read access on the whole
    warehouse, inherited by every namespace/table underneath."""
    subject_id = resolve_keycloak_service_account_subject(token, "trino")
    principal = f"oidc~{subject_id}"
    ensure_user_permission(
        token, f"/v1/permissions/warehouse/{warehouse_id}/assignments", "select", principal
    )
    print(f"Granted read access on warehouse to {principal} (trino service account).")


def ensure_warehouse(token: str) -> str:
    resp = requests.get(
        f"{MANAGEMENT_URL}/v1/warehouse", headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    existing = {w["name"]: w["warehouse-id"] for w in resp.json().get("warehouses", [])}
    if WAREHOUSE_NAME in existing:
        print(f"Warehouse '{WAREHOUSE_NAME}' already exists.")
        return existing[WAREHOUSE_NAME]

    resp = requests.post(
        f"{MANAGEMENT_URL}/v1/warehouse",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "warehouse-name": WAREHOUSE_NAME,
            "storage-profile": {
                "type": "s3",
                "bucket": MINIO_BUCKET,
                "key-prefix": "warehouse",
                "endpoint": MINIO_ENDPOINT,
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "access-key-id": MINIO_ACCESS_KEY,
                "secret-access-key": MINIO_SECRET_KEY,
            },
        },
    )
    resp.raise_for_status()
    warehouse_id = resp.json()["warehouse-id"]
    print(f"Created warehouse '{WAREHOUSE_NAME}'.")
    return warehouse_id


ORDERS_SCHEMA = Schema(
    NestedField(1, "order_id", LongType(), required=False),
    NestedField(2, "customer", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
)

TRANSLATED_DOCS_SCHEMA = Schema(
    NestedField(1, "document_id", LongType(), required=False),
    NestedField(2, "source_language", StringType(), required=False),
    NestedField(3, "translated_text", StringType(), required=False),
)

DEMO_TABLES = {
    ("sales", "orders"): ORDERS_SCHEMA,
    ("content", "translated_documents_sensitive"): TRANSLATED_DOCS_SCHEMA,
}
DEMO_NAMESPACES = ["sales", "sales_reporting", "content"]

ORDERS_ROWS = pa.table(
    {
        "order_id": [1, 2, 3, 4, 5],
        "customer": ["acme-corp", "globex-inc", "initech", "umbrella-corp", "stark-industries"],
        "amount": [1250.00, 430.50, 89.99, 2100.75, 560.20],
    }
)
DEMO_ROWS = {
    ("sales", "orders"): ORDERS_ROWS,
}


def seed_demo_data(token: str) -> None:
    catalog = RestCatalog(
        name="lakekeeper",
        uri=CATALOG_URL,
        warehouse=WAREHOUSE_NAME,
        token=token,
    )

    for ns in DEMO_NAMESPACES:
        try:
            catalog.create_namespace(ns)
            print(f"Created namespace '{ns}'.")
        except NamespaceAlreadyExistsError:
            print(f"Namespace '{ns}' already exists.")

    for (ns, table), schema in DEMO_TABLES.items():
        identifier = f"{ns}.{table}"
        try:
            iceberg_table = catalog.create_table(identifier, schema=schema)
            print(f"Created table '{identifier}'.")
        except TableAlreadyExistsError:
            iceberg_table = catalog.load_table(identifier)
            print(f"Table '{identifier}' already exists.")

        rows = DEMO_ROWS.get((ns, table))
        if rows is not None:
            if iceberg_table.scan().to_arrow().num_rows == 0:
                iceberg_table.append(rows)
                print(f"Inserted {rows.num_rows} dummy rows into '{identifier}'.")
            else:
                print(f"'{identifier}' already has data, leaving it as-is.")


def main() -> None:
    token = get_token()
    wait_for_lakekeeper(token)
    ensure_bootstrapped(token)

    # Bootstrapping may rotate/require a fresh token for subsequent calls in
    # some Lakekeeper versions; re-fetch to be safe.
    token = get_token()

    subject_id = resolve_keycloak_subject(token, ADMIN_EMAIL)
    ensure_admin_granted(token, subject_id)
    warehouse_id = ensure_warehouse(token)
    ensure_trino_read_access(token, warehouse_id)
    seed_demo_data(token)
    print("Bootstrap complete.")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTP error: {e.response.status_code} {e.response.text}", file=sys.stderr)
        raise
