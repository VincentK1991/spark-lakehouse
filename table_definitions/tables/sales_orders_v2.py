"""Schema contract for local.sales.orders_v2 (successor to sales.orders).

Applied by table_definitions/apply.py — CREATE TABLE IF NOT EXISTS only, see
that script's docstring for what that means. Import SCHEMA from here in ETL
code that writes to this table, to validate the DataFrame's shape against
the contract instead of trusting it matches by convention:

    from table_definitions.tables.sales_orders_v2 import SCHEMA, TABLE
    assert new_df.schema == SCHEMA
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TABLE = "sales.orders_v2"
DESCRIPTION = "Canonical sales order line items (successor to sales.orders)"

SCHEMA = StructType(
    [
        StructField("order_id", LongType(), nullable=False, metadata={"comment": "Primary key"}),
        StructField("customer", StringType(), nullable=False),
        StructField("amount", DecimalType(10, 2), nullable=False),
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
