"""Shared retail pipeline configuration and source discovery helpers."""

from pathlib import Path

DEFAULT_SOURCE_FOLDER = "/opt/airflow/data"
DEFAULT_SQL_FOLDER = "/opt/airflow/include/sql"

SNOWFLAKE_CONN_ID = "snowflake_conn"

SOURCE_CONFIG = {
    "pos_sales": {
        "prefixes": ["pos_sales"],
        "columns": [
            "transaction_id",
            "line_id",
            "store_id",
            "item_id",
            "transaction_date",
            "sales_qty",
            "unit_price",
            "discount_amt",
        ],
        "transform_sql": "transform_silver_sales.sql",
        "bronze_table": "BRONZE.BRONZE_SALES",
        "bronze_columns": [
            "TRANSACTION_ID",
            "TRANSACTION_LINE_ID",
            "STORE_ID",
            "ITEM_ID",
            "TRANSACTION_DATE",
            "QUANTITY",
            "UNIT_PRICE",
            "DISCOUNT_AMT",
        ],
        "silver_table": "SILVER.SILVER_SALES",
    },
    "item_master": {
        "prefixes": ["item_master"],
        "columns": [
            "item_id",
            "item_name",
            "brand",
            "category",
            "subclass_id",
            "subclass_name",
            "unit_cost",
            "list_price",
        ],
        "transform_sql": "transform_silver_item_master.sql",
        "bronze_table": "BRONZE.BRONZE_ITEM_MASTER",
        "bronze_columns": [
            "ITEM_ID",
            "ITEM_NAME",
            "BRAND",
            "CATEGORY",
            "SUBCLASS_ID",
            "SUBCLASS_NAME",
            "UNIT_COST",
            "LIST_PRICE",
        ],
        "silver_table": "SILVER.SILVER_ITEM_MASTER",
    },
    "inventory": {
        "prefixes": ["inventory"],
        "columns": [
            "snapshot_date",
            "item_id",
            "store_id",
            "on_hand_qty",
            "unit_cost",
        ],
        "transform_sql": "transform_silver_inventory.sql",
        "bronze_table": "BRONZE.BRONZE_INVENTORY",
        "bronze_columns": [
            "SNAPSHOT_DATE",
            "ITEM_ID",
            "STORE_ID",
            "ON_HAND_QTY",
            "UNIT_COST",
        ],
        "silver_table": "SILVER.SILVER_INVENTORY",
    },
    "store_master": {
        "prefixes": ["store_master"],
        "columns": [
            "store_id",
            "store_name",
            "city",
            "district_id",
            "district_name",
            "region",
            "store_type",
            "open_date",
        ],
        "transform_sql": "transform_silver_store_master.sql",
        "bronze_table": "BRONZE.BRONZE_STORE_MASTER",
        "bronze_columns": [
            "STORE_ID",
            "STORE_NAME",
            "CITY",
            "DISTRICT_ID",
            "DISTRICT_NAME",
            "REGION",
            "STORE_TYPE",
            "OPEN_DATE",
        ],
        "silver_table": "SILVER.SILVER_STORE_MASTER",
    },
}

SUPPORTED_SOURCES = tuple(SOURCE_CONFIG.keys())


def get_source_config(filename):
    """Return (source_name, config) based on the filename prefix."""

    for source_name, config in SOURCE_CONFIG.items():
        if any(filename.startswith(prefix) for prefix in config["prefixes"]):
            return source_name, config

    return None, None


def read_csv_headers_and_count(file_path):
    """Return the CSV header list and the number of data rows."""

    import csv

    with open(file_path, "r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames or []
        row_count = 0
        for row_number, row in enumerate(reader, start=1):
            if None in row:
                raise ValueError(
                    f"CSV data row {row_number} has more fields than its header."
                )
            if any(row.get(column) is None for column in headers):
                raise ValueError(
                    f"CSV data row {row_number} has fewer fields than its header."
                )
            row_count += 1

    return headers, row_count


def resolve_source_folder(value=None):
    return Path(value or DEFAULT_SOURCE_FOLDER)


def resolve_sql_folder(value=None):
    return Path(value or DEFAULT_SQL_FOLDER)
