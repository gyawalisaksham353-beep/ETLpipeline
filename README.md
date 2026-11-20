# Retail Data Warehouse ETL Pipeline

A daily ETL pipeline built with **Apache Airflow and Snowflake** that processes four retail data feeds, applies data quality checks, maintains dimensional history, and reconciles data across the warehouse.

The pipeline follows a medallion architecture (Bronze, Silver, and Gold) and is designed to support reliable reruns, historical backfills, data quality monitoring, and consistent reporting.

## 1. Project Overview

The pipeline processes four retail source files:

- POS Sales
- Item Master
- Inventory
- Store Master

It takes raw CSV files through ingestion, validation, transformation, dimensional modeling, and reconciliation before making the data available for reporting.

### Key Features

- Daily orchestration using Apache Airflow.
- Snowflake-based data warehouse.
- File discovery and logical-date validation.
- Parallel Bronze ingestion using mapped Airflow tasks.
- Silver-layer validation, deduplication, and quarantine.
- Type 1 and Type 2 dimensional history management.
- Sales and inventory fact tables.
- Batch-level and file-level reconciliation.
- Idempotent processing to support safe reruns.
- Historical backfills for previously missed dates.

## 2. Architecture

### Pipeline Flow

```text
                Logical-Date Sensor
                        |
                File Discovery
                        |
                File Validation
                        |
              +---------+---------+
              |         |         |
           Sales    Item Master  Inventory  Store Master
              |         |         |         |
              +---------+---------+---------+
                        |
                Bronze Layer
              (Raw Landing Tables)
                        |
                Silver Layer
          (Validation and Transformation)
                        |
                 Quarantine
           (Rejected Records and Reasons)
                        |
                 Gold Layer
             (Dimensional Warehouse)
                        |
                Reconciliation
                        |
                Reporting Views
```

### Medallion Layers

| Layer | Purpose |
|---|---|
| Bronze | Land source data in Snowflake and maintain ingestion metadata. |
| Silver | Validate, clean, deduplicate, and transform source records. |
| Quarantine | Store rejected records with descriptive rejection reasons. |
| Gold | Build dimensional models, sales facts, and inventory facts. |
| Control | Track ingestion batches and pipeline execution metadata. |
| Reconciliation | Compare records and measures across pipeline layers. |

## 3. Gold Data Model

The Gold layer uses a snowflake schema to represent product and organizational hierarchies.

### Product Hierarchy

```text
DIM_CATEGORY
     |
DIM_SUBCLASS
     |
DIM_ITEM
     |
  FCT_SALES
```

### Organizational Hierarchy

```text
DIM_REGION
     |
DIM_DISTRICT
     |
DIM_LOCN
     |
  FCT_SALES
     |
FCT_INVENTORY
```

`DIM_DAY` connects to both fact tables.

### Dimension Management

**Type 2 history**

- `DIM_ITEM` and `DIM_LOCN` maintain historical versions.
- When a tracked attribute changes, a new dimension version is created.
- Previous versions are retained for historical reporting.

**Type 1 history**

- `DIM_ITEM.BRAND` is managed as a Type 1 attribute.
- Brand changes update all existing item versions.
- No new dimension version is created for a brand-only change.

### Fact Tables

**FCT_SALES**

Stores sales transaction-line records.

- Grain: One row per transaction and transaction line.
- Measures include sales quantity and net sales amount.
- Duplicate transaction lines are identified using `(TRANSACTION_ID, TRANSACTION_LINE_ID)`.

**FCT_INVENTORY**

Stores inventory positions.

- Grain: One row per item, location, and snapshot date.
- Each snapshot represents an inventory observation.
- Earlier positions are closed using `EFFECTIVE_TO` and `IS_CURRENT`.
- Historical inventory observations are retained rather than deleted.

## 4. Data Quality and Quarantine

Data quality checks are performed in the Silver layer before records are loaded into the Gold model.

### Validation Process

1. Validate required fields and source values.
2. Check data types and business rules.
3. Identify duplicate business keys.
4. Check referential integrity against master data.
5. Separate invalid records from valid records.
6. Store rejected records in the quarantine table.

### Quarantine Table

Rejected records are written to:

`QUARANTINE.RECORD_QUARANTINE`

The table preserves:

- Source file
- Load date
- Raw record values
- `REASON_CODE`
- Descriptive rejection reason

This allows invalid records to be investigated without silently discarding source data.

### Designed Sales Defects

Five deliberate sales data defects were introduced to test the validation process:

| Defect | Expected Handling |
|---|---|
| Two NULL item IDs | Quarantine |
| One negative sales quantity | Quarantine |
| One orphan item ID | Quarantine |
| One duplicated transaction line | Count and deduplicate |

The duplicate is handled separately from quarantine because it is a duplicate of an otherwise valid business record.

Verification SQL:

`include/sql/test/verify_five_defects.sql`

## 5. Reconciliation

Reconciliation verifies that records and measures remain consistent as data moves through the pipeline.

It operates at the batch, source-file, and logical-date levels.

The reconciliation process checks:

- Source-to-Bronze row counts.
- Bronze-to-Silver processing counts.
- Accepted and quarantined records.
- Deduplication counts.
- Silver-to-Gold record counts.
- Sales quantities and financial measures.
- Inventory quantities and values.

Reconciliation results are stored and used to identify mismatches.

A failed reconciliation causes the DAG to fail rather than allowing inconsistent data to pass silently into reporting.

SQL files:

- `include/sql/recon/pipeline_summary.sql`

## 6. Idempotency and Processing Reliability

The pipeline is designed so that rerunning the same logical date does not create duplicate records or change previously calculated results.

Key implementation decisions:

**Bronze ingestion**
- Generates a batch ID using logical date, source, and filename hash.
- Uploads files to batch-specific internal-stage paths.
- Uses `COPY INTO` to load data into temporary tables.
- Verifies staged and inserted row counts.
- Replaces existing rows for the corresponding source filename.

**Silver transformation**
- Rebuilds the relevant logical-date partition.
- Merges records using business keys.
- Deduplicates sales transactions.
- Routes invalid records to quarantine.

**Gold transformation**
- Uses deterministic surrogate keys.
- Uses conditional `MERGE` operations.
- Avoids unnecessary timestamp updates when business values have not changed.
- Preserves historical inventory positions.

**Reconciliation**
- Tracks processing results by batch, file, and logical date.
- Fails the pipeline when reconciliation requirements are not met.

## 7. Configuration

The following Airflow connections and variables must be configured before running the pipeline.

### Airflow Connection

| Name | Purpose |
|---|---|
| `snowflake_conn` | Snowflake connection and credentials. |

### Airflow Variables

| Variable | Purpose | Default |
|---|---|---|
| `source_folder` | Mounted directory containing source CSV files. | `/opt/airflow/data` |
| `snowflake_database` | Target Snowflake database. | `RETAIL_DWH` |
| `target_schema` | Reporting-view schema. | `PUBLIC` |
| `sql_folder` | Root directory containing SQL files. | `/opt/airflow/include/sql` |

Database and reporting-schema identifiers are validated before SQL rendering.

The remaining medallion schemas are fixed:

- `CONTROL`
- `BRONZE`
- `SILVER`
- `QUARANTINE`
- `DIMENSIONS`
- `FACTS`

Snowflake credentials are managed through Airflow connections and are not stored in source code.

## 8. Running the Pipeline

### DAG Details

| Property | Value |
|---|---|
| DAG ID | `retail_warehouse_pipeline` |
| Schedule | Daily |
| Catchup | Disabled |
| Orchestrator | Apache Airflow |
| Warehouse | Snowflake |

The logical-date sensor waits until matching files from all four sources are available.

If a required file is missing, the sensor fails after its configured timeout and reports the missing sources.

### Historical Backfill

For the supplied five-day dataset, run the following command from PowerShell:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id retail_warehouse_pipeline --from-date 2026-11-10 --to-date 2026-11-14 --max-active-runs 1
```

This creates one logical-date run for each day.

Each run discovers its source files and creates mapped Bronze tasks, one per file.

### Rerunning a Failed Execution

To retry a partially completed run, clear `discover_files` and its downstream tasks rather than clearing only the final verification task.

To reprocess an already completed date using Airflow 3.3:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id retail_warehouse_pipeline --from-date 2026-11-12 --to-date 2026-11-12 --reprocess-behavior completed --max-active-runs 1
```

## 9. Testing and Validation

### Unit Tests

Run repository tests from the project root:

```powershell
docker compose run --rm --no-deps airflow-worker python -m unittest discover -s /opt/airflow/tests -v
```

### SQL Regression Tests

After a Snowflake backfill, run:

`include/sql/test/verify_five_defects.sql`

This checks the designed data defects, dimensional history, and positional inventory behavior.

### Sales Reporting Check

Compare Gold sales totals by date with Silver:

```sql
SELECT
    DIM_DAY.FULL_DATE,
    COUNT(*) AS SALES_LINES,
    SUM(FCT_SALES.NET_SALES_AMT) AS NET_SALES
FROM RETAIL_DWH.FACTS.FCT_SALES AS FCT_SALES
JOIN RETAIL_DWH.DIMENSIONS.DIM_DAY AS DIM_DAY
    ON DIM_DAY.DATE_SK = FCT_SALES.DATE_SK
GROUP BY DIM_DAY.FULL_DATE
ORDER BY DIM_DAY.FULL_DATE;
```

### Idempotency Acceptance Test

For replay acceptance:

1. Record row counts, quantities, and net sales.
2. Record inventory on-hand quantities and values.
3. Rerun the same logical date.
4. Compare the results before and after the rerun.

All recorded counts and measures should remain identical.

Mocked unit tests alone are not sufficient to establish live Snowflake replay correctness.

## 10. Live Execution Results

The pipeline was executed against the configured Snowflake account on **November 20, 2026**.

### Backfill Results

| Test | Result |
|---|---|
| Five-day backfill (Nov 10–14) | All five runs succeeded |
| Task instances | All 21 task instances green in every run |
| Single-date replay (Nov 12) | Identical counts and totals |
| Full five-day replay | Identical counts and totals |
| Defect/SCD regression checks | 8 of 8 passed |
| Reconciliation FAIL records | 0 |
| Reconciliation WARNING records | 0 |

### Day-Three Sales Validation

For November 12:

| Processing Stage | Records |
|---|---:|
| Bronze sales | 25 |
| Silver sales | 20 |
| Quarantined records | 4 |
| Deduplicated records | 1 |

Quarantine breakdown:

| Reason | Count |
|---|---:|
| `NULL_ITEM_ID` | 2 |
| `NEGATIVE_QTY` | 1 |
| `ORPHAN_ITEM` | 1 |

### Dimensional History Results

- Store `S002` has two `DIM_LOCN` versions:
  - District `D02` until November 11, 2026.
  - District `D04` from November 12, 2026 (current).
- Item `I003` carries the Type 1 brand `DairyBest Select` across every version.
- `FCT_INVENTORY` contains 250 closed/current positions.
- No duplicate open inventory positions were found.

## 11. Repository Structure

```text
.
├── dags/
│   ├── retail_warehouse_pipeline.py
│   └── utils/
├── include/
│   └── sql/
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       ├── recon/
│       └── test/
├── tests/
├── docs/
│   ├── source_contract.md
│   └── silver_contract.md
├── evidence/
│   ├── backfill_grid.svg
│   ├── backfill_grid.html
│   ├── backfill_runs.txt
│   ├── reconciliation_output.txt
│   └── idempotency snapshots
├── data/
├── docker-compose.yaml
└── README.md
```

## 12. Documentation and Evidence

Additional documentation:

| File | Description |
|---|---|
| `docs/source_contract.md` | Source columns, observed types, grains, and business keys. |
| `docs/silver_contract.md` | Silver-layer data types and validation rules. |
| `include/sql/test/verify_five_defects.sql` | Defect, SCD, and inventory verification queries. |
| `include/sql/recon/pipeline_summary.sql` | Latest reconciliation summary. |

### Execution Evidence

The `evidence/` directory contains:

- `backfill_grid.svg` and `backfill_grid.html`: Visual representation of Airflow task execution.
- `backfill_runs.txt`: Raw Airflow run states.
- `reconciliation_output.txt`: Reconciliation and defect verification output.
- Before-and-after idempotency snapshots.

The SVG grid was generated from Airflow task states because the execution environment did not have a browser screenshot tool. Raw run states and Snowflake outputs are included alongside it.

## 13. Known Limitations

The following limitations were identified during implementation:

1. **Existing Airflow DAG issue:** The environment's `example_custom_weight` DAG can break the global `airflow dags list` command because its custom priority-weight strategy is not registered. The retail pipeline and its per-DAG commands are unaffected.

2. **Typed Bronze sales table:** `BRONZE_SALES` uses `DATE` and `NUMBER` columns instead of storing every field as `VARCHAR`. The supplied defects are handled, but future files containing unparseable dates or numeric values would require a Bronze schema rebuild or a text-based staging model.

3. **Quarantine rebuild scope:** Quarantine rebuilding replaces rows per source across all Bronze history rather than only the logical-date partition. It is idempotent but becomes more expensive as historical data grows.

4. **Corrected source-file deletions:** Gold fact tables use `MERGE` on their grain keys. If a corrected source file removes an existing transaction line, the old Gold record is not automatically deleted.

5. **Legacy ingestion records:** Legacy `INGEST_BATCH` rows are ignored during date-scoped reconciliation rather than deleted.

6. **Legacy Gold schema:** Tables from the earlier modeling exercise use different key names (`ITEM_KY`, `LOCN_KY`, `DAY_KY`) and cannot be upgraded additively due to their `NOT NULL` constraints. The pipeline detects these markers and drops only pipeline-owned Gold tables once with `CASCADE`, then recreates them. Silver remains the source of truth for rebuilding Gold.

7. **Missing source files:** All four source prefixes must be present for the logical date. A missing file causes the sensor to time out.

8. **Insert-count mismatch:** A failing mapped loader rolls back its transaction and reports the affected file and batch ID. The failing loader should be investigated before retrying reconciliation.

---

## 14. Demo Walkthrough

The recommended demonstration sequence is:

1. Show the logical-date sensor waiting for all four source files.
2. Show the four mapped Bronze loads, batch IDs, and row-count checks.
3. Demonstrate Silver validation using the day-three sales defects.
4. Query the `DIM_ITEM` history and Type 1 brand update.
5. Query the `DIM_LOCN` district change for store `S002`.
6. Show inventory `EFFECTIVE_TO` and `IS_CURRENT` behavior.
7. Show per-file reconciliation results.
8. Rerun a logical date and compare counts and measures to demonstrate idempotency.

---

**Technologies:** Apache Airflow 3.3 · Snowflake · Python · SQL · Docker · Git
