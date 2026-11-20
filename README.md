# Retail warehouse pipeline

This repository implements a daily Airflow pipeline that lands four retail CSV
feeds in Snowflake, validates and quarantines records, builds normalized
dimensional history, and reconciles each file through Gold.

## Architecture

```text
logical-date sensor
        |
file discovery and validation
        |
mapped PUT -> COPY INTO -> Bronze (one task per file)
        |
Silver validation, deduplication, and record quarantine
        |
SCD dimensions -> sales and positional inventory facts
        |
per-file/per-date reconciliation -> reporting views
```

The Gold model remains snowflaked:

```text
DIM_CATEGORY -> DIM_SUBCLASS -> DIM_ITEM -> FCT_SALES
DIM_REGION   -> DIM_DISTRICT -> DIM_LOCN -> FCT_SALES
                                      \-> FCT_INVENTORY
DIM_DAY --------------------------------> both facts
```

`DIM_ITEM` and `DIM_LOCN` preserve Type 2 history. `DIM_ITEM.BRAND` is Type 1:
brand changes update all versions without creating a new item version.
`FCT_INVENTORY` stores every item/location/snapshot observation; subsequent
observations close earlier positions using `EFFECTIVE_TO` and `IS_CURRENT`.

## Configuration

Set these Airflow connections/variables before running:

| Name | Value / purpose |
|---|---|
| Connection `snowflake_conn` | Snowflake credentials; credentials are not stored in code |
| Variable `source_folder` | Mounted source folder, normally `/opt/airflow/data` |
| Variable `snowflake_database` | Database name, normally `RETAIL_DWH` |
| Variable `target_schema` | Reporting-view schema, normally `PUBLIC` |
| Variable `sql_folder` | Optional SQL root; defaults to `/opt/airflow/include/sql` |

Database and reporting-schema identifiers are validated before SQL rendering.
Other medallion schemas (`CONTROL`, `BRONZE`, `SILVER`, `QUARANTINE`,
`DIMENSIONS`, and `FACTS`) are fixed parts of the model.

## Run and backfill

The DAG is `retail_warehouse_pipeline`, scheduled daily with catchup disabled.
The sensor waits for a matching file from **each** source for the run's logical
date. It fails after its configured timeout and reports which sources are
missing.

For the supplied five-day dataset, run the explicit Airflow backfill from
PowerShell:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id retail_warehouse_pipeline --from-date 2026-11-10 --to-date 2026-11-14 --max-active-runs 1
```

This creates one logical-date run for each day. The mapped Bronze task runs once
per discovered file in that run. To retry a partially completed run, clear
`discover_files` and its downstream tasks, not just the final verification
task.

## Idempotency and data quality

- Bronze uses a logical-date/source/filename-hash batch ID, uploads to a
  per-batch internal-stage path, copies into a temporary table, verifies both
  staged and inserted row counts, then replaces rows for the source filename.
- Silver rebuilds only the logical-date partition and merges by its business
  key. Bad records are written to `QUARANTINE.RECORD_QUARANTINE` with a
  `REASON_CODE`, descriptive reason, source file, load date, and raw values.
- Sales deduplicates by `(TRANSACTION_ID, TRANSACTION_LINE_ID)`.
- Dimensions and facts use deterministic surrogate keys and conditional
  `MERGE`s. Replays do not append duplicate rows or update timestamps when
  business values have not changed.
- Inventory positions are keyed by item/location/snapshot date. Earlier rows
  are closed; they are never deleted.
- Reconciliation records counts and measures per batch/file/date and fails the
  DAG on mismatches.

The five designed sales defects are covered by
[`verify_five_defects.sql`](include/sql/test/verify_five_defects.sql): two null
item IDs, a negative quantity, an orphan item, and one duplicated transaction
line. The duplicated line is counted and deduplicated; the other four rows are
quarantined.

## Contracts and validation

- Source columns, observed types, grains, and keys:
  [`docs/source_contract.md`](docs/source_contract.md)
- Typed Silver columns and validation rules:
  [`docs/silver_contract.md`](docs/silver_contract.md)
- Read-only defect, SCD, and positional-inventory checks:
  [`include/sql/test/verify_five_defects.sql`](include/sql/test/verify_five_defects.sql)
- Latest reconciliation results:
  [`include/sql/recon/pipeline_summary.sql`](include/sql/recon/pipeline_summary.sql)

Run repository tests from the project root:

```powershell
docker compose run --rm --no-deps airflow-worker python -m unittest discover -s /opt/airflow/tests -v
```

After a Snowflake backfill, run the SQL regression query and compare Gold sales
to Silver by date:

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

For replay acceptance, record row counts, quantity, net sales, and inventory
on-hand/value before and after rerunning the same logical date. They should be
identical. A mocked unit test is not a substitute for this live Snowflake
acceptance check. Airflow 3.3 uses `airflow backfill create`; for an already
completed date, explicitly opt in to reprocessing:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id retail_warehouse_pipeline --from-date 2026-11-12 --to-date 2026-11-12 --reprocess-behavior completed --max-active-runs 1
```

## Troubleshooting

- `RequestsDependencyWarning` is a dependency compatibility warning, not a
  Snowflake load error by itself.
- A `COPY INTO` column-count failure is prevented by the Bronze file format's
  metadata-aware mismatch setting and Python row-width preflight. Run the
  updated Bronze DDL before retrying an existing Snowflake environment.
- A source-to-Bronze mismatch with a filename-only batch ID indicates legacy
  control rows. Reconciliation filters to date-scoped batch IDs, so legacy rows
  no longer create false warnings. Legacy `INGEST_BATCH` rows are ignored, not
  deleted.
- Gold tables created by the earlier modelling exercise use different key names
  (`ITEM_KY`, `LOCN_KY`, `DAY_KY`) and cannot be upgraded additively because of
  their `NOT NULL` constraints. `create_warehouse_tables` detects those markers,
  drops only the pipeline-owned Gold tables once with `CASCADE`, and recreates
  them with the current dimensional model. Silver is the source of truth, so the
  next Gold load rebuilds all rows.
- For a missing-file timeout, compare the logical date with the four filenames
  in `source_folder`; the sensor requires all four prefixes for that date.
- For an insert-count mismatch, inspect the failing mapped loader before
  retrying reconciliation. The loader rolls its transaction back and reports
  the file and batch ID.

## Demo outline

1. Show the sensor waiting until all four logical-date files exist.
2. Show four mapped Bronze loads, their batch IDs, and row-count checks.
3. Show Silver rejecting the day-three sales defects and counting the duplicate.
4. Query `DIM_ITEM` history/brand and `DIM_LOCN` history for the store district
   event.
5. Query inventory `EFFECTIVE_TO`/`IS_CURRENT` and per-file reconciliation.
6. Rerun a logical date and compare the documented counts and measures.

## Live verification and evidence

The pipeline was run against the configured Snowflake account on 2026-11-20.

- Five-day backfill `2026-11-10` through `2026-11-14`: all five runs succeeded,
  with all 21 task instances green in every run.
- A fresh rerun of `2026-11-12` produced exactly the same row counts and totals.
- A full rerun of all five dates also produced exactly the same counts and
  totals.
- All eight defect/SCD checks in `verify_five_defects.sql` passed.
- Reconciliation had zero FAIL and zero WARNING rows across all three layers.
- Day-three result: Bronze 25 sales rows, Silver 20, quarantine 4
  (`NULL_ITEM_ID` 2, `NEGATIVE_QTY` 1, `ORPHAN_ITEM` 1), deduplicated 1.
- `S002` has two `DIM_LOCN` versions (`D02` until 2026-11-11, `D04` current from
  2026-11-12). `I003` carries the Type 1 brand `DairyBest Select` on every
  version.
- `FCT_INVENTORY` has 250 closed/current positions and no duplicate open rows.

Evidence is in [`evidence/`](evidence/): the generated backfill grid
(`backfill_grid.svg` / `.html`), raw run states (`backfill_runs.txt`),
reconciliation and defect output (`reconciliation_output.txt`), and the
before/after idempotency snapshots.

The evidence grid is an SVG rendering of the Airflow task grid, created because
the execution environment has no browser screenshot tool. The underlying run
states and Snowflake outputs are included as text alongside it.

Known limitations:

- The environment's existing `example_custom_weight` DAG can break the global
  `airflow dags list` command because its custom priority-weight strategy is not
  registered. The pipeline itself and its per-DAG commands are unaffected.
- Bronze `BRONZE_SALES` is typed (`DATE`, `NUMBER`) rather than all-`VARCHAR`.
  The supplied defects are handled, but a future file with an unparseable date
  or numeric value would need a Bronze schema rebuild or a text staging model.
- The quarantine rebuild replaces rows per source across all Bronze history, not
  only the logical-date partition. It is idempotent but becomes more expensive
  as history grows.
- Gold facts use `MERGE` on the grain key, which satisfies the replay
  requirement. If a corrected source file removes a transaction line, the old
  Gold line is not deleted by the merge.
#   E T L p i p e l i n e  
 # ETLpipeline
# ETLpipeline
