import csv
import sys
import unittest
from collections import Counter
from datetime import date
from pathlib import Path
from unittest import mock

from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowException

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

import retail_pipeline
from utils.retail_config import SOURCE_CONFIG, read_csv_headers_and_count
from utils.sql_utilis import split_sql_statements


class RetailPipelineContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(
            dag_folder=str(ROOT / "dags"),
            safe_mode=False,
        )
        cls.dag = cls.dagbag.get_dag("retail_warehouse_pipeline")

    def test_dag_imports_and_has_no_cycles(self):
        self.assertEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.topological_sort()), len(self.dag.tasks))

    def test_tasks_retry_sensor_times_out_and_ingest_is_mapped(self):
        for task in self.dag.tasks:
            self.assertGreaterEqual(task.retries, 1, task.task_id)

        sensor = self.dag.get_task("wait_for_files")
        timeout = sensor.timeout
        if hasattr(timeout, "total_seconds"):
            timeout = timeout.total_seconds()
        self.assertEqual(timeout, 600)
        self.assertTrue(self.dag.get_task("load_to_bronze").is_mapped)

    def test_dag_does_not_embed_credentials_paths_or_warehouse_objects(self):
        source = (ROOT / "dags" / "retail_pipeline.py").read_text(
            encoding="utf-8"
        )
        for forbidden in (
            "RETAIL_DWH.",
            "/opt/airflow/",
            "password=",
            "private_key=",
        ):
            self.assertNotIn(forbidden.lower(), source.lower())

    def test_logical_date_sensor_requires_one_file_per_source(self):
        with self.subTest("four sources"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as temp_dir:
                folder = Path(temp_dir)
                for filename in (
                    "pos_sales_20261112.csv",
                    "item_master_20261112.csv",
                    "inventory_snapshot_20261112.csv",
                    "store_master_20261112.csv",
                ):
                    (folder / filename).touch()
                files = retail_pipeline.source_files_for_date(
                    folder,
                    date(2026, 11, 12),
                )
                self.assertEqual(set(files), set(SOURCE_CONFIG))
                self.assertTrue(all(files.values()))

                (folder / "store_master_20261112.csv").unlink()
                files = retail_pipeline.source_files_for_date(
                    folder,
                    date(2026, 11, 12),
                )
                self.assertFalse(files["store_master"])

    def test_batch_id_uses_logical_date_and_is_stable(self):
        filename = "pos_sales_20261112.csv"
        batch_id = retail_pipeline.make_batch_id(
            filename,
            "pos_sales",
            date(2026, 11, 12),
        )
        self.assertRegex(batch_id, r"^20261112_pos_sales_[0-9a-f]{16}$")
        self.assertEqual(
            batch_id,
            retail_pipeline.make_batch_id(
                filename,
                "pos_sales",
                date(2026, 11, 12),
            ),
        )

    def test_all_supplied_files_match_source_contracts(self):
        for path in sorted((ROOT / "data").glob("*.csv")):
            source_name, config = retail_pipeline.get_source_config(path.name)
            self.assertIsNotNone(source_name, path.name)
            headers, row_count = read_csv_headers_and_count(path)
            self.assertEqual(headers, config["columns"], path.name)
            self.assertGreater(row_count, 0, path.name)

    def test_day_three_sales_defects_are_present(self):
        path = ROOT / "data" / "pos_sales_20261112.csv"
        with path.open(newline="", encoding="utf-8-sig") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(sum(not row["item_id"].strip() for row in rows), 2)
        self.assertEqual(sum(float(row["sales_qty"]) < 0 for row in rows), 1)
        self.assertEqual(sum(row["item_id"] == "I999" for row in rows), 1)
        key_counts = Counter(
            (row["transaction_id"], row["line_id"]) for row in rows
        )
        self.assertEqual(sum(count > 1 for count in key_counts.values()), 1)

    def test_silver_transforms_rebuild_one_date_partition(self):
        sql_root = ROOT / "include" / "sql" / "silver"
        for config in SOURCE_CONFIG.values():
            statements = split_sql_statements(
                (sql_root / config["transform_sql"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(len(statements), 4, config["transform_sql"])
            self.assertIn("TO_DATE(%s)", statements[2])
            self.assertIn("TO_DATE(%s)", statements[3])

    def test_gold_loaders_are_merges_and_inventory_closes_positions(self):
        gold = ROOT / "include" / "sql" / "gold"
        ddl = (gold / "create_gold_tables.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("ADD COLUMN IF NOT EXISTS ITEM_SK", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS ITEM_NAME", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS BRAND", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS LOCN_SK", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS LOCN_ID", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS DATE_SK", ddl)
        location = (gold / "load_dim_locn.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("TARGET.LOCN_ID = SOURCE.STORE_ID", location)

        for filename in (
            "load_dim_item.sql",
            "load_dim_locn.sql",
            "load_dim_subclass.sql",
            "load_dim_district.sql",
            "load_dim_category.sql",
            "load_dim_region.sql",
            "load_fact_sales.sql",
            "load_fact_inventory.sql",
        ):
            sql = (gold / filename).read_text(encoding="utf-8")
            self.assertIn("MERGE INTO", sql.upper(), filename)
            self.assertNotIn("DELETE FROM", sql.upper(), filename)

        item_sql = (gold / "load_dim_item.sql").read_text(encoding="utf-8")
        self.assertIn("LATEST_BRAND", item_sql)
        self.assertIn("TARGET.BRAND = SOURCE.BRAND", item_sql)
        self.assertNotIn("COALESCE(ITEM_MASTER.BRAND", item_sql)

        inventory_sql = (gold / "load_fact_inventory.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("EFFECTIVE_TO", inventory_sql)
        self.assertIn("IS_CURRENT", inventory_sql)
        self.assertIn("LEAD(SNAPSHOT_DATE)", inventory_sql)

    def test_reconciliation_failure_raises_airflow_exception(self):
        failure = {
            "SOURCE_NAME": "pos_sales",
            "BATCH_ID": "20261112_pos_sales_0123456789abcdef",
            "RECON_MESSAGE": "source=25, gold=24",
        }
        with mock.patch.object(
            retail_pipeline,
            "execute_sql_file",
            return_value=[failure],
        ):
            with self.assertRaisesRegex(AirflowException, "source=25, gold=24"):
                retail_pipeline.assert_reconciliation("SOURCE", "BRONZE")

    def test_bronze_to_silver_reconciliation_excludes_legacy_batches(self):
        sql = (
            ROOT / "include" / "sql" / "recon"
            / "recon_bronze_to_silver.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("REGEXP_LIKE(", sql)
        self.assertIn(
            "^[0-9]{8}_(pos_sales|item_master|inventory|store_master)_[0-9a-f]{16}$",
            sql,
        )

    def test_source_to_bronze_reconciliation_excludes_legacy_batches(self):
        sql = (
            ROOT / "include" / "sql" / "recon"
            / "recon_source_to_bronze.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("REGEXP_LIKE(", sql)
        self.assertIn(
            "^[0-9]{8}_(pos_sales|item_master|inventory|store_master)_[0-9a-f]{16}$",
            sql,
        )

    def test_legacy_gold_schema_migration_is_wired_in(self):
        gold = ROOT / "include" / "sql" / "gold"
        detect = (gold / "detect_legacy_gold_schema.sql").read_text(
            encoding="utf-8"
        )
        drop = (gold / "drop_legacy_gold_tables.sql").read_text(
            encoding="utf-8"
        )
        dag = (ROOT / "dags" / "retail_pipeline.py").read_text(
            encoding="utf-8"
        )
        for marker in ("ITEM_KY", "LOCN_KY", "DAY_KY"):
            self.assertIn(marker, detect)
        for table in (
            "FCT_SALES",
            "FCT_INVENTORY",
            "DIM_ITEM",
            "DIM_LOCN",
            "DIM_DAY",
        ):
            self.assertIn(table, drop)
        self.assertIn("CASCADE", drop)
        self.assertIn("rebuild_legacy_gold_tables", dag)


if __name__ == "__main__":
    unittest.main()
