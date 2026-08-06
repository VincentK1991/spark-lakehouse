"""
Reconciles reconciler/groups.yaml and reconciler/grants.yaml against Keycloak
(groups/members) and Lakekeeper (roles/role-members/permission-grants).
Additive and idempotent: re-running only adds what's missing, it never
removes a group/role member or grant that these files no longer list
(removal reconciliation is a possible future enhancement, out of scope for
this first pass).

Run with:
    docker compose run --rm reconciler
"""

import os
import sys

import requests
import yaml

from enums import Permission, ResourceType

LAKEKEEPER_URL = os.environ["LAKEKEEPER_URL"]
MANAGEMENT_URL = f"{LAKEKEEPER_URL}/management"
CATALOG_URL = f"{LAKEKEEPER_URL}/catalog"
KEYCLOAK_TOKEN_URL = os.environ["KEYCLOAK_TOKEN_URL"]
KEYCLOAK_ADMIN_API_URL = os.environ["KEYCLOAK_ADMIN_API_URL"]
CLIENT_ID = os.environ["RECONCILER_CLIENT_ID"]
CLIENT_SECRET = os.environ["RECONCILER_CLIENT_SECRET"]

GROUPS_FILE = os.path.join(os.path.dirname(__file__), "groups.yaml")
GRANTS_FILE = os.path.join(os.path.dirname(__file__), "grants.yaml")


def get_token() -> str:
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "lakekeeper",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def kc_get(token: str, path: str, **params):
    resp = requests.get(
        f"{KEYCLOAK_ADMIN_API_URL}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
    )
    resp.raise_for_status()
    return resp.json()


def kc_post(token: str, path: str, json_body=None):
    resp = requests.post(
        f"{KEYCLOAK_ADMIN_API_URL}{path}",
        headers={"Authorization": f"Bearer {token}"},
        json=json_body or {},
    )
    if resp.status_code not in (200, 201, 204, 409):
        resp.raise_for_status()
    return resp


def kc_put(token: str, path: str):
    resp = requests.put(
        f"{KEYCLOAK_ADMIN_API_URL}{path}", headers={"Authorization": f"Bearer {token}"}
    )
    if resp.status_code not in (200, 204):
        resp.raise_for_status()


def ensure_keycloak_group(token: str, name: str) -> str:
    found = kc_get(token, "/groups", search=name, exact="true")
    for g in found:
        if g["name"] == name:
            return g["id"]
    kc_post(token, "/groups", {"name": name})
    found = kc_get(token, "/groups", search=name, exact="true")
    return next(g["id"] for g in found if g["name"] == name)


def resolve_user_subject_by_email(token: str, email: str) -> str:
    users = kc_get(token, "/users", email=email, exact="true")
    if not users:
        raise SystemExit(f"No Keycloak user found with email {email}")
    return users[0]["id"]


def resolve_service_account_subject(token: str, client_id: str) -> str:
    username = f"service-account-{client_id}"
    users = kc_get(token, "/users", username=username, exact="true")
    if not users:
        raise SystemExit(
            f"No Keycloak service-account user found for client_id {client_id} "
            f"(expected username '{username}')"
        )
    return users[0]["id"]


def add_user_to_keycloak_group(token: str, user_id: str, group_id: str) -> None:
    kc_put(token, f"/users/{user_id}/groups/{group_id}")


def lk_get(token: str, path: str, base: str = MANAGEMENT_URL):
    resp = requests.get(f"{base}{path}", headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()


def lk_post(token: str, path: str, json_body: dict, base: str = MANAGEMENT_URL):
    resp = requests.post(
        f"{base}{path}", headers={"Authorization": f"Bearer {token}"}, json=json_body
    )
    resp.raise_for_status()
    return resp


def ensure_lakekeeper_role(token: str, name: str, description: str) -> str:
    roles = lk_get(token, "/v1/role").get("roles", [])
    for r in roles:
        if r["name"] == name:
            return r["id"]
    resp = lk_post(token, "/v1/role", {"name": name, "description": description})
    return resp.json()["id"]


def ensure_role_member(token: str, role_id: str, subject_id: str) -> None:
    members = lk_get(token, f"/v1/role/{role_id}/members").get("members", [])
    principal = f"oidc~{subject_id}"
    for m in members:
        if m.get("type") == "user" and m.get("id") == principal:
            return
    lk_post(
        token,
        f"/v1/role/{role_id}/members",
        {"members": [{"type": "user", "id": principal}]},
    )


def sync_groups(token: str, groups: dict) -> dict:
    """Returns {group_name: lakekeeper_role_id}."""
    role_ids = {}
    for group_name, spec in groups.items():
        kc_group_id = ensure_keycloak_group(token, group_name)
        role_id = ensure_lakekeeper_role(token, group_name, spec.get("description", ""))
        role_ids[group_name] = role_id

        for email in spec.get("members", []):
            subject_id = resolve_user_subject_by_email(token, email)
            add_user_to_keycloak_group(token, subject_id, kc_group_id)
            ensure_role_member(token, role_id, subject_id)
            print(f"  {group_name}: ensured member {email}")

        for client_id in spec.get("service_accounts", []):
            subject_id = resolve_service_account_subject(token, client_id)
            add_user_to_keycloak_group(token, subject_id, kc_group_id)
            ensure_role_member(token, role_id, subject_id)
            print(f"  {group_name}: ensured service account {client_id}")

    return role_ids


def get_warehouse_id(token: str, name: str) -> str:
    warehouses = lk_get(token, "/v1/warehouse").get("warehouses", [])
    for w in warehouses:
        if w["name"] == name:
            return w["id"]
    raise SystemExit(f"Warehouse '{name}' not found (has bootstrap.py run yet?)")


def get_namespace_id(token: str, warehouse_id: str, namespace: str) -> str:
    resp = requests.get(
        f"{CATALOG_URL}/v1/{warehouse_id}/namespaces/{namespace}",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.json()["properties"]["namespace_id"]


def get_table_id(token: str, warehouse_id: str, namespace: str, table: str) -> str:
    resp = requests.get(
        f"{CATALOG_URL}/v1/{warehouse_id}/namespaces/{namespace}/tables/{table}",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.json()["metadata"]["table-uuid"]


def ensure_grant(
    token: str, resource_type: str, resource_path: str, permission: str, role_id: str
) -> None:
    if permission not in Permission._value2member_map_:
        raise SystemExit(f"Unknown permission '{permission}' for {resource_path}")

    parts = resource_path.split(".")
    warehouse_name = parts[0]
    warehouse_id = get_warehouse_id(token, warehouse_name)

    if resource_type == ResourceType.WAREHOUSE:
        assignments_path = f"/v1/permissions/warehouse/{warehouse_id}/assignments"
    elif resource_type == ResourceType.NAMESPACE:
        namespace = ".".join(parts[1:])
        namespace_id = get_namespace_id(token, warehouse_id, namespace)
        assignments_path = f"/v1/permissions/namespace/{namespace_id}/assignments"
    elif resource_type == ResourceType.TABLE:
        namespace = ".".join(parts[1:-1])
        table = parts[-1]
        table_id = get_table_id(token, warehouse_id, namespace, table)
        assignments_path = f"/v1/permissions/warehouse/{warehouse_id}/table/{table_id}/assignments"
    else:
        raise SystemExit(f"Unsupported resource_type: {resource_type}")

    current = lk_get(token, assignments_path).get("assignments", [])
    already_granted = any(a.get("type") == permission and a.get("role") == role_id for a in current)
    if already_granted:
        print(f"  {resource_path}: '{permission}' already granted to role {role_id}")
        return

    lk_post(
        token,
        assignments_path,
        {"writes": [{"type": permission, "role": role_id}], "deletes": []},
    )
    print(f"  {resource_path}: granted '{permission}' to role {role_id}")


def main() -> None:
    with open(GROUPS_FILE) as f:
        groups_spec = yaml.safe_load(f)
    with open(GRANTS_FILE) as f:
        grants_spec = yaml.safe_load(f)

    token = get_token()

    print("Syncing groups (Keycloak groups + Lakekeeper roles/members)...")
    role_ids = sync_groups(token, groups_spec.get("groups", {}))

    print("Applying grants...")
    for grant in grants_spec.get("grants", []):
        role_id = role_ids.get(grant["group"])
        if role_id is None:
            raise SystemExit(f"Grant references unknown group '{grant['group']}'")
        ensure_grant(
            token,
            grant["resource_type"],
            grant["resource_path"],
            grant["permission"],
            role_id,
        )

    print("Reconciliation complete.")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTP error: {e.response.status_code} {e.response.text}", file=sys.stderr)
        raise
