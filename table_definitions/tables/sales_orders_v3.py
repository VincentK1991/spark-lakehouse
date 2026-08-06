"""Schema contract for local.sales.orders_v3.

Unlike sales.orders_v2 (created ad hoc from a notebook before this contract
existed, so it has no timestamp column), this table is defined here first —
it exists to demonstrate the partition_by() hook end-to-end: apply.py can
only express partitioning through a column that's actually in the contract.

Applied by table_definitions/apply.py — CREATE TABLE IF NOT EXISTS only, see
that script's docstring for what that means.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

TABLE = "sales.orders_v3"
DESCRIPTION = "Sales order line items, partitioned by day"

SCHEMA = StructType(
    [
        StructField(
            "order_id", LongType(), nullable=False, metadata={"comment": "Primary key"}
        ),
        StructField("amount", DoubleType(), nullable=False),
        StructField(
            "order_ts",
            TimestampType(),
            nullable=False,
            metadata={"comment": "Order placement time, UTC"},
        ),
    ]
)

PROPERTIES = {"write.format.default": "parquet"}


def partition_by() -> list:
    """Column expressions, not plain strings: pyspark.sql.functions needs an
    active SparkSession to build them, which doesn't exist yet at
    module-import time, so apply.py calls this after starting one."""
    return [F.days("order_ts")]
