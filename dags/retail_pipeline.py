"""Retail data warehouse pipeline: Bronze -> Quarantine -> Silver -> Gold.

Implements the medallion architecture and reconciliation checkpoints for the
retail warehouse.
"""

import csv
import hashlib
import re
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, BaseSensorOperator, Variable, get_current_context, task
from airflow.sdk.exceptions import AirflowException

from utils.retail_config import (
    DEFAULT_SOURCE_FOLDER,
    SOURCE_CONFIG,
    SUPPORTED_SOURCES,
    get_source_config,
    read_csv_headers_and_count,
)
from utils.sql_utilis import (
    copy_csv_to_bronze,
    execute_sql_file,
    execute_sql_transaction,
    rebuild_legacy_gold_tables,
)


DEFAULT_ARGS = {
    "owner": "retail_data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

WAREHOUSE_DDL = [
    {"layer": "control", "sql_file": "create_control_tables.sql"},
    {"layer": "bronze", "sql_file": "create_bronze_tables.sql"},
    {"layer": "silver", "sql_file": "create_silver_tables.sql"},
    {"layer": "gold", "sql_file": "create_gold_tables.sql"},
]


def get_source_folder():
    return Path(
        Variable.get("source_folder", default=DEFAULT_SOURCE_FOLDER)
    )


def file_sha256(file_path):
    digest = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            digest.update(chunk)

    return digest.hexdigest()


def make_batch_id(filename, source_name, processing_date):
    batch_date = processing_date.strftime("%Y%m%d")
    file_key = hashlib.sha256(filename.encode("utf-8")).hexdigest()[:16]
    return f"{batch_date}_{source_name}_{file_key}"


def source_files_for_date(source_folder, processing_date):
    date_token = processing_date.strftime("%Y%m%d")
    csv_files = [
        path
        for path in source_folder.glob("*.csv")
        if re.search(rf"(?<!\d){date_token}(?!\d)", path.stem)
    ]
    return {
        source_name: [
            path
            for path in csv_files
            if any(
                path.name.startswith(prefix)
                for prefix in config["prefixes"]
            )
        ]
        for source_name, config in SOURCE_CONFIG.items()
    }


class CSVFileSensor(BaseSensorOperator):
    """Wait until at least one supported CSV file is present."""

    def poke(self, context):
        source_folder = get_source_folder()

        if not source_folder.exists():
            return False

        logical_date = context.get("logical_date")
        if logical_date is None:
            raise AirflowException("The file sensor requires a logical date.")

        source_files = source_files_for_date(
            source_folder,
            logical_date.date(),
        )
        date_token = logical_date.strftime("%Y%m%d")
        missing_sources = [
            source_name
            for source_name, files in source_files.items()
            if not files
        ]
        if not missing_sources:
            print(
                f"All required source files found for {date_token}: "
                f"{sum(len(files) for files in source_files.values())}"
            )
            return True

        print(
            f"Waiting for {date_token} source files; "
            f"missing: {', '.join(missing_sources)}"
        )
        return False


@task
def create_warehouse_tables():
    if rebuild_legacy_gold_tables():
        print("Replaced legacy Gold tables with the current dimensional model.")
    execute_sql_transaction(WAREHOUSE_DDL)
    print("Control, bronze, silver and gold objects are ready.")


@task
def discover_files():
    """Validate files, register batches, and quarantine invalid files."""

    logical_date = get_current_context().get("logical_date")
    if logical_date is None:
        raise AirflowException("File discovery requires a DAG logical date.")
    processing_date = logical_date.date()
    date_token = processing_date.strftime("%Y%m%d")

    source_folder = get_source_folder()
    source_folder.mkdir(parents=True, exist_ok=True)

    valid_files = []
    quarantined_files = []

    candidate_files = sorted(
        path
        for path in source_folder.iterdir()
        if path.is_file()
        and not path.name.startswith(".")
        and re.search(rf"(?<!\d){date_token}(?!\d)", path.stem)
    )

    for file_path in candidate_files:
        filename = file_path.name
        is_csv = file_path.suffix.lower() == ".csv"
        source_name, config = (
            get_source_config(filename) if is_csv else (None, None)
        )
        batch_id = make_batch_id(filename, source_name or "unknown", processing_date)

        if is_csv:
            try:
                headers, row_count = read_csv_headers_and_count(file_path)
                file_hash = file_sha256(file_path)
                read_error = None
            except (OSError, UnicodeError, csv.Error, ValueError) as exc:
                headers, row_count = [], 0
                file_hash = None
                read_error = str(exc)
        else:
            headers, row_count, file_hash, read_error = [], 0, None, None

        expected_headers = config["columns"] if config else []
        rejection_reason = None

        if not is_csv:
            rejection_reason = "unsupported file format"
        elif config is None:
            rejection_reason = "unsupported source filename"
        elif read_error is not None:
            rejection_reason = f"unreadable file: {read_error}"
        elif headers != expected_headers:
            rejection_reason = "unexpected or missing CSV headers"
        elif row_count == 0:
            rejection_reason = "file contains no data rows"

        operations = [
            {
                "layer": "control",
                "sql_file": "clear_batch.sql",
                "statement_index": 0,
                "params": (batch_id,),
            },
            {
                "layer": "control",
                "sql_file": "clear_batch.sql",
                "statement_index": 1,
                "params": (batch_id,),
            },
            {
                "layer": "control",
                "sql_file": "register_batch.sql",
                "params": (
                    batch_id,
                    source_name or "unknown",
                    filename,
                    str(file_path),
                    file_hash,
                    row_count,
                ),
            },
        ]

        if rejection_reason is not None:
            operations.append(
                {
                    "layer": "control",
                    "sql_file": "quarantine_file.sql",
                    "params": (
                        batch_id,
                        source_name or "unknown",
                        filename,
                        rejection_reason,
                        ", ".join(expected_headers),
                        ", ".join(headers),
                    ),
                }
            )
            operations.append(
                {
                    "layer": "control",
                    "sql_file": "set_batch_status.sql",
                    "params": ("QUARANTINED", rejection_reason, batch_id),
                }
            )
            quarantined_files.append(filename)

        execute_sql_transaction(operations)

        if rejection_reason is None:
            valid_files.append(
                {
                    "batch_id": batch_id,
                    "source_name": source_name,
                    "file_name": filename,
                    "file_path": str(file_path),
                    "row_count": row_count,
                    "processing_date": processing_date.isoformat(),
                }
            )

    print(f"Valid files: {len(valid_files)}")
    print(f"Quarantined files: {len(quarantined_files)}")

    if quarantined_files:
        print(f"Quarantined: {quarantined_files}")

    if not candidate_files:
        raise FileNotFoundError(
            f"No source files found for logical date {date_token}."
        )

    return valid_files


@task
def load_to_bronze(file_info):
    """Stage one validated CSV and bulk-load it into its Bronze table."""

    filename = file_info["file_name"]
    batch_id = file_info["batch_id"]
    source_name = file_info["source_name"]
    config = SOURCE_CONFIG[source_name]
    file_path = Path(file_info["file_path"])

    rows_loaded = copy_csv_to_bronze(
        file_path=file_path,
        source_columns=config["columns"],
        bronze_columns=config["bronze_columns"],
        bronze_table=config["bronze_table"],
        file_name=filename,
        batch_id=batch_id,
        expected_row_count=file_info["row_count"],
        processing_date=file_info["processing_date"],
    )
    execute_sql_file(
        "control",
        "set_batch_status.sql",
        params=("BRONZE_LOADED", None, batch_id),
    )

    print(f"{source_name}: loaded {rows_loaded} rows from {filename}")

    return {
        "source_name": source_name,
        "batch_id": batch_id,
        "rows_loaded": rows_loaded,
    }


@task
def run_silver_transform(source_name):
    """Validate bronze records and load clean silver records."""

    config = SOURCE_CONFIG[source_name]
    logical_date = get_current_context().get("logical_date")
    if logical_date is None:
        raise AirflowException("Silver transformation requires a DAG logical date.")

    processing_date = logical_date.date().isoformat()
    execute_sql_transaction(
        [
            {
                "layer": "silver",
                "sql_file": config["transform_sql"],
                "statement_index": statement_index,
                "params": (processing_date,)
                if statement_index in (2, 3)
                else None,
            }
            for statement_index in range(4)
        ]
    )

    print(f"Silver transformation completed: {source_name}")


@task
def reconcile_source_to_bronze():
    execute_sql_file("recon", "recon_source_to_bronze.sql")
    print("Source -> Bronze reconciliation completed.")


@task
def reconcile_bronze_to_silver():
    execute_sql_file("recon", "recon_bronze_to_silver.sql")
    print("Bronze -> Silver reconciliation completed.")


@task
def reconcile_silver_to_gold():
    execute_sql_file("recon", "recon_silver_to_gold.sql")
    print("Silver -> Gold reconciliation completed.")


def assert_reconciliation(source_layer, target_layer):
    failures = execute_sql_file(
        "recon",
        "check_reconciliation.sql",
        params=(source_layer, target_layer),
        fetch=True,
    )

    if failures:
        formatted = "; ".join(
            f"{row['SOURCE_NAME']} (batch {row['BATCH_ID']}): "
            f"{row['RECON_MESSAGE']}"
            for row in failures
        )
        raise AirflowException(
            f"{source_layer} -> {target_layer} reconciliation failed: {formatted}"
        )

    print(f"{source_layer} -> {target_layer} reconciliation passed.")


@task
def verify_source_to_bronze():
    assert_reconciliation("SOURCE", "BRONZE")


@task
def verify_bronze_to_silver():
    assert_reconciliation("BRONZE", "SILVER")


@task
def verify_silver_to_gold():
    assert_reconciliation("SILVER", "GOLD")


@task
def load_gold_dimensions():
    execute_sql_transaction(
        [
            {"layer": "gold", "sql_file": "load_dim_category.sql"},
            {"layer": "gold", "sql_file": "load_dim_region.sql"},
            {"layer": "gold", "sql_file": "load_dim_subclass.sql"},
            {"layer": "gold", "sql_file": "load_dim_district.sql"},
            {"layer": "gold", "sql_file": "load_dim_item.sql"},
            {"layer": "gold", "sql_file": "load_dim_locn.sql"},
            {"layer": "gold", "sql_file": "load_dim_day.sql"},
        ]
    )
    print("Gold dimensions loaded.")


@task
def load_gold_facts():
    execute_sql_transaction(
        [
            {"layer": "gold", "sql_file": "load_fact_sales.sql"},
            {"layer": "gold", "sql_file": "load_fact_inventory.sql"},
        ]
    )
    print("Gold facts loaded.")


@task
def create_reporting_views():
    execute_sql_file("reporting", "create_reporting_views.sql")
    print("Reporting and monitoring views created.")


@task
def report_pipeline_summary():
    rows = execute_sql_file("recon", "pipeline_summary.sql", fetch=True)

    for row in rows:
        print(
            f"{row['SOURCE_LAYER']:>6} -> {row['TARGET_LAYER']:<6} "
            f"| {row['SOURCE_NAME']:<12} | {row['RECON_STATUS']:<6} "
            f"| {row['RECON_MESSAGE']}"
        )

    return rows


with DAG(
    dag_id="retail_warehouse_pipeline",
    description="Retail medallion pipeline with quarantine and reconciliation",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 11, 10),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["retail", "medallion", "ingestion", "reconciliation"],
) as dag:

    wait_for_files = CSVFileSensor(
        task_id="wait_for_files",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    tables = create_warehouse_tables()
    discovered = discover_files()
    loaded = load_to_bronze.expand(file_info=discovered)

    recon_source = reconcile_source_to_bronze()
    verified_source = verify_source_to_bronze()

    transformed = run_silver_transform.expand(source_name=list(SUPPORTED_SOURCES))

    recon_bronze = reconcile_bronze_to_silver()
    verified_bronze = verify_bronze_to_silver()

    dimensions = load_gold_dimensions()
    facts = load_gold_facts()

    recon_gold = reconcile_silver_to_gold()
    verified_gold = verify_silver_to_gold()
    reporting = create_reporting_views()
    summary = report_pipeline_summary()

    (
        wait_for_files
        >> tables
        >> discovered
        >> loaded
        >> recon_source
        >> verified_source
        >> transformed
        >> recon_bronze
        >> verified_bronze
        >> dimensions
        >> facts
        >> recon_gold
        >> verified_gold
        >> reporting
        >> summary
    )
