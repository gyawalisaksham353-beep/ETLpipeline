"""Reusable Snowflake execution helpers for the retail pipeline."""

import hashlib
import re
import uuid
from pathlib import Path

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable

from utils.retail_config import (
    DEFAULT_SQL_FOLDER,
    SNOWFLAKE_CONN_ID,
)


def get_sql_folder():
    return Path(Variable.get("sql_folder", default=DEFAULT_SQL_FOLDER))


def get_snowflake_database():
    database = Variable.get("snowflake_database", default="RETAIL_DWH")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", database) is None:
        raise ValueError("Airflow variable 'snowflake_database' must be an identifier.")
    return database.upper()


def get_target_schema():
    schema = Variable.get("target_schema", default="PUBLIC")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", schema) is None:
        raise ValueError("Airflow variable 'target_schema' must be an identifier.")
    return schema.upper()


def get_sql_path(layer, filename):
    """Build the full path to a SQL file."""

    sql_path = get_sql_folder() / layer / filename

    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path


def split_sql_statements(sql_content):
    """Split SQL on semicolons while ignoring strings and comments."""

    statements = []
    current = []
    index = 0
    length = len(sql_content)

    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    while index < length:
        char = sql_content[index]
        next_char = sql_content[index + 1] if index + 1 < length else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                index += 2
                in_block_comment = False
                continue
            index += 1
            continue

        if in_single_quote:
            current.append(char)
            if char == "'":
                if next_char == "'":
                    current.append(next_char)
                    index += 2
                    continue
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            current.append(char)
            if char == '"':
                if next_char == '"':
                    current.append(next_char)
                    index += 2
                    continue
                in_double_quote = False
            index += 1
            continue

        if char == "'":
            in_single_quote = True
            current.append(char)
            index += 1
            continue

        if char == '"':
            in_double_quote = True
            current.append(char)
            index += 1
            continue

        if char == "-" and next_char == "-":
            in_line_comment = True
            current.append(char)
            current.append(next_char)
            index += 2
            continue

        if char == "/" and next_char == "*":
            in_block_comment = True
            current.append(char)
            current.append(next_char)
            index += 2
            continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)

    return statements


def read_sql_file(layer, filename):
    """Read a SQL file and split it into executable statements."""

    sql_path = get_sql_path(layer, filename)

    with open(sql_path, "r", encoding="utf-8") as file:
        sql_content = file.read()

    sql_content = sql_content.replace(
        "{{ snowflake_database }}",
        get_snowflake_database(),
    ).replace(
        "{{ target_schema }}",
        get_target_schema(),
    )
    return split_sql_statements(sql_content)


def _execute_statement(cursor, statement, params=None, many=False):
    if many:
        cursor.executemany(statement, params or [])
    elif params is not None:
        cursor.execute(statement, params)
    else:
        cursor.execute(statement)


def _fetch_rows(cursor):
    if cursor.description is None:
        return []

    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def run_sql_file(
    cursor,
    layer,
    filename,
    params=None,
    many=False,
    statement_index=None,
    fetch=False,
):
    """Execute a SQL file and optionally return rows from its last statement."""

    statements = read_sql_file(layer, filename)

    if statement_index is not None:
        statements = [(statement_index, statements[statement_index])]
    else:
        statements = list(enumerate(statements))

    rows = []

    for execution_index, (index, statement) in enumerate(statements):
        try:
            _execute_statement(cursor, statement, params=params, many=many)
        except Exception as exc:
            raise RuntimeError(
                f"Failed executing statement {index + 1} "
                f"in SQL file '{layer}/{filename}'"
            ) from exc

        if fetch and execution_index == len(statements) - 1:
            rows = _fetch_rows(cursor)

    return rows


def execute_sql_file(
    layer,
    filename,
    params=None,
    many=False,
    statement_index=None,
    fetch=False,
):
    """Open a Snowflake connection, execute a SQL file, and commit."""

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        rows = run_sql_file(
            cursor=cursor,
            layer=layer,
            filename=filename,
            params=params,
            many=many,
            statement_index=statement_index,
            fetch=fetch,
        )
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def execute_sql_transaction(operations):
    """Execute several SQL files in one Snowflake transaction."""

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        for operation in operations:
            run_sql_file(
                cursor=cursor,
                layer=operation["layer"],
                filename=operation["sql_file"],
                params=operation.get("params"),
                many=operation.get("many", False),
                statement_index=operation.get("statement_index"),
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def copy_csv_to_bronze(
    file_path,
    source_columns,
    bronze_columns,
    bronze_table,
    file_name,
    batch_id,
    expected_row_count,
    processing_date,
):
    """Stage one CSV and replace its Bronze rows using COPY INTO."""

    local_path = Path(file_path).resolve()
    if not local_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {local_path}")

    if not source_columns or len(source_columns) != len(bronze_columns):
        raise ValueError(
            "Source and Bronze column lists must have equal non-zero lengths."
        )

    identifier_pattern = r"[A-Za-z_][A-Za-z0-9_$]*"
    table_parts = bronze_table.split(".")
    if len(table_parts) != 2 or any(
        re.fullmatch(identifier_pattern, part) is None for part in table_parts
    ):
        raise ValueError(f"Invalid Bronze schema/table name: {bronze_table}")

    def normalize_column(column):
        if re.fullmatch(identifier_pattern, column) is None:
            raise ValueError(f"Invalid column name: {column}")
        return column.upper()

    source_columns = [normalize_column(column) for column in source_columns]
    bronze_columns = [normalize_column(column) for column in bronze_columns]
    database = get_snowflake_database()
    bronze_table = f"{database}.{bronze_table}"
    if "STAGED_FILE_ROW_NUMBER" in source_columns:
        raise ValueError("Source columns cannot use the staging metadata column name.")

    stage_subpath = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()
    temp_table = (
        f"{database}.BRONZE.BRONZE_COPY_STAGE_"
        + uuid.uuid4().hex.upper()
    )
    temp_columns = ",\n        ".join(
        f"{column} VARCHAR" for column in source_columns
    )
    target_columns = ", ".join(
        bronze_columns
        + ["SRC_FILE_NM", "SRC_ROW_NUM", "BATCH_ID", "INGEST_TS", "INGEST_DT"]
    )
    select_columns = ", ".join(source_columns)

    file_uri = f"file://{local_path.as_posix()}".replace("'", "''")
    put_sql = (
        f"PUT '{file_uri}' "
        f"@{database}.BRONZE.BRONZE_INGEST_STAGE/{stage_subpath} "
        "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    create_temp_sql = f"""
        CREATE TEMP TABLE {temp_table} (
            {temp_columns},
            STAGED_FILE_ROW_NUMBER NUMBER
        )
    """
    copy_sql = f"""
        COPY INTO {temp_table}
        FROM @{database}.BRONZE.BRONZE_INGEST_STAGE/{stage_subpath}
        FILE_FORMAT = (
            FORMAT_NAME = {database}.BRONZE.BRONZE_CSV_FORMAT
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (
            STAGED_FILE_ROW_NUMBER = METADATA$FILE_ROW_NUMBER
        )
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
    """
    insert_sql = f"""
        INSERT INTO {bronze_table} ({target_columns})
        SELECT
            {select_columns},
            %s,
            ROW_NUMBER() OVER (ORDER BY STAGED_FILE_ROW_NUMBER),
            %s,
            CURRENT_TIMESTAMP(),
            TO_DATE(%s)
        FROM {temp_table}
    """

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(put_sql)
        cursor.fetchall()

        cursor.execute(
            f"LIST @{database}.BRONZE.BRONZE_INGEST_STAGE/{stage_subpath}"
        )
        if len(cursor.fetchall()) != 1:
            raise RuntimeError(
                f"Expected exactly one staged file for batch {batch_id}."
            )

        cursor.execute(create_temp_sql)
        cursor.execute(copy_sql)
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
        copied_row_count = cursor.fetchone()[0]
        if copied_row_count != expected_row_count:
            raise RuntimeError(
                f"COPY INTO loaded {copied_row_count} rows for {file_name}; "
                f"expected {expected_row_count}."
            )

        cursor.execute(
            f"DELETE FROM {bronze_table} WHERE SRC_FILE_NM = %s",
            (file_name,),
        )
        cursor.execute(insert_sql, (file_name, batch_id, processing_date))
        cursor.execute(
            f"""
                SELECT COUNT(*)
                FROM {bronze_table}
                WHERE SRC_FILE_NM = %s
                  AND BATCH_ID = %s
            """,
            (file_name, batch_id),
        )
        inserted_row_count = cursor.fetchone()[0]
        if inserted_row_count != expected_row_count:
            raise RuntimeError(
                f"Bronze insert wrote {inserted_row_count} rows for {file_name} "
                f"(batch {batch_id}); expected {expected_row_count}."
            )

        conn.commit()
        return inserted_row_count
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def legacy_gold_schema_present():
    """Return True when the earlier modelling Gold tables are still present."""

    rows = execute_sql_file(
        "gold",
        "detect_legacy_gold_schema.sql",
        fetch=True,
    )
    if not rows:
        return False
    return int(rows[0].get("LEGACY_TABLE_COUNT", 0)) > 0


def rebuild_legacy_gold_tables():
    """Replace only the Gold tables that still use the legacy key model."""

    if not legacy_gold_schema_present():
        return False

    execute_sql_file("gold", "drop_legacy_gold_tables.sql")
    return True
