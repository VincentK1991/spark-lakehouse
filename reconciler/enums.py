"""Enums for grants.yaml validation, mirroring Lakekeeper's permission and
resource-type vocabulary.
"""

from enum import StrEnum


class ResourceType(StrEnum):
    WAREHOUSE = "warehouse"
    NAMESPACE = "namespace"
    TABLE = "table"


class Permission(StrEnum):
    DESCRIBE = "describe"
    SELECT = "select"
    CREATE = "create"
    MODIFY = "modify"
    OWNERSHIP = "ownership"
    PASS_GRANTS = "pass_grants"
    MANAGE_GRANTS = "manage_grants"
