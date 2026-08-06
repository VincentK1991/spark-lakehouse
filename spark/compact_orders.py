from query_orders import get_spark_session


def compact_orders():
    username = "alice"
    password = "test1234"
    spark = get_spark_session(username, password, "lakehouse-compact-orders")
    print("Before compaction:\n==================\n")
    before = spark.sql("SELECT file_path, file_size_in_bytes, record_count FROM sales.orders.files")
    before.show(truncate=False)
    print(f"{before.count()} data file(s) before compaction.")
    print("\n\nCompacting...\n==================\n")
    result = spark.sql(
        """
        CALL lakekeeper.system.rewrite_data_files(
            table => 'sales.orders',
            options => map('min-input-files', '2')
        )
    """
    )
    print(f"Compaction result: {result.show(truncate=False)}")
    print("\n\nAfter compaction:\n==================\n")
    after = spark.sql("""SELECT file_path, file_size_in_bytes, record_count
    FROM sales.orders.files
    """)
    after.show(truncate=False)
    print(f"{after.count()} data file(s) after compaction.")

    spark.stop()


if __name__ == "__main__":
    compact_orders()
