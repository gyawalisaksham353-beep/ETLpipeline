-- Read-only regression checks for sales quarantine, duplicate handling, and SCD2 history.
-- Run after the complete pipeline has refreshed Silver and Gold.
-- These checks validate defect classes and invariants, not specific IDs or dates.

WITH NULL_ITEM_CASE AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES
            WHERE COALESCE(TRIM(TO_VARCHAR(ITEM_ID)), '') = ''
        ) AS SOURCE_COUNT,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.QUARANTINE.RECORD_QUARANTINE
            WHERE SOURCE_NAME = 'pos_sales'
              AND SOURCE_LAYER = 'BRONZE'
              AND REASON_CODE = 'NULL_ITEM_ID'
        ) AS QUARANTINE_COUNT,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.SILVER.SILVER_SALES
            WHERE COALESCE(TRIM(TO_VARCHAR(ITEM_ID)), '') = ''
        ) AS SILVER_COUNT
),
NEGATIVE_QUANTITY_CASE AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES
            WHERE TRY_TO_NUMBER(TO_VARCHAR(QUANTITY)) < 0
        ) AS SOURCE_COUNT,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.QUARANTINE.RECORD_QUARANTINE
            WHERE SOURCE_NAME = 'pos_sales'
              AND SOURCE_LAYER = 'BRONZE'
              AND REASON_CODE = 'NEGATIVE_QTY'
        ) AS QUARANTINE_COUNT,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.SILVER.SILVER_SALES
            WHERE TRY_TO_NUMBER(TO_VARCHAR(QUANTITY)) < 0
        ) AS SILVER_COUNT
),
MISSING_ITEM_CASE AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES AS SALES
            WHERE COALESCE(TRIM(TO_VARCHAR(SALES.ITEM_ID)), '') <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER AS ITEM
                  WHERE ITEM.ITEM_ID = SALES.ITEM_ID
                    AND COALESCE(TRIM(TO_VARCHAR(ITEM.ITEM_ID)), '') <> ''
                    AND COALESCE(TRIM(TO_VARCHAR(ITEM.ITEM_NAME)), '') <> ''
                    AND TRY_TO_NUMBER(ITEM.UNIT_COST, 38, 6) >= 0
                    AND TRY_TO_NUMBER(ITEM.LIST_PRICE, 38, 6) >= 0
                    AND COALESCE(
                        TRY_TO_DATE(
                            REGEXP_SUBSTR(ITEM.SRC_FILE_NM, '[0-9]{8}'),
                            'YYYYMMDD'
                        ),
                        TRY_TO_DATE(TO_VARCHAR(ITEM.INGEST_DT))
                    ) <= TRY_TO_DATE(TO_VARCHAR(SALES.TRANSACTION_DATE))
              )
        ) AS SOURCE_COUNT,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.QUARANTINE.RECORD_QUARANTINE
            WHERE SOURCE_NAME = 'pos_sales'
              AND SOURCE_LAYER = 'BRONZE'
              AND REASON_CODE = 'ORPHAN_ITEM'
        ) AS QUARANTINE_COUNT,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.SILVER.SILVER_SALES AS SILVER
            WHERE NOT EXISTS (
                SELECT 1
                FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER AS ITEM
                WHERE ITEM.ITEM_ID = SILVER.ITEM_ID
                  AND COALESCE(TRIM(TO_VARCHAR(ITEM.ITEM_ID)), '') <> ''
                  AND COALESCE(TRIM(TO_VARCHAR(ITEM.ITEM_NAME)), '') <> ''
                  AND TRY_TO_NUMBER(ITEM.UNIT_COST, 38, 6) >= 0
                  AND TRY_TO_NUMBER(ITEM.LIST_PRICE, 38, 6) >= 0
                  AND COALESCE(
                      TRY_TO_DATE(
                          REGEXP_SUBSTR(ITEM.SRC_FILE_NM, '[0-9]{8}'),
                          'YYYYMMDD'
                      ),
                      TRY_TO_DATE(TO_VARCHAR(ITEM.INGEST_DT))
                  ) <= SILVER.TRANSACTION_DATE
            )
        ) AS SILVER_COUNT
),
DUPLICATE_LINE_CASE AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT TRANSACTION_ID, TRANSACTION_LINE_ID
                FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES
                GROUP BY TRANSACTION_ID, TRANSACTION_LINE_ID
                HAVING COUNT(*) > 1
            )
        ) AS BRONZE_DUPLICATE_GROUPS,
        (
            SELECT COUNT(*)
            FROM (
                SELECT TRANSACTION_ID, TRANSACTION_LINE_ID
                FROM {{ snowflake_database }}.SILVER.SILVER_SALES
                GROUP BY TRANSACTION_ID, TRANSACTION_LINE_ID
                HAVING COUNT(*) > 1
            )
        ) AS SILVER_DUPLICATE_GROUPS,
        (
            SELECT COALESCE(SUM(DUPLICATE_RECORD_COUNT), 0)
            FROM {{ snowflake_database }}.CONTROL.RECONCILIATION
            WHERE SOURCE_NAME = 'pos_sales'
              AND SOURCE_LAYER = 'BRONZE'
              AND TARGET_LAYER = 'SILVER'
        ) AS RECON_DUPLICATE_COUNT
),
ITEM_BRAND_LATEST AS (
    SELECT ITEM_ID, BRAND
    FROM {{ snowflake_database }}.SILVER.SILVER_ITEM_MASTER
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ITEM_ID
        ORDER BY SNAPSHOT_DATE DESC, SRC_ROW_NUM DESC
    ) = 1
),
ITEM_SCD1_CASE AS (
    SELECT COUNT_IF(DIM_ITEM.BRAND IS DISTINCT FROM ITEM_BRAND_LATEST.BRAND)
        AS BRAND_MISMATCHES
    FROM {{ snowflake_database }}.DIMENSIONS.DIM_ITEM AS DIM_ITEM
    INNER JOIN ITEM_BRAND_LATEST
        ON ITEM_BRAND_LATEST.ITEM_ID = DIM_ITEM.ITEM_ID
),
INVENTORY_SCD_CASE AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT ITEM_ID, STORE_ID, SNAPSHOT_DATE
                FROM {{ snowflake_database }}.FACTS.FCT_INVENTORY
                GROUP BY ITEM_ID, STORE_ID, SNAPSHOT_DATE
                HAVING COUNT(*) > 1
            )
        ) AS DUPLICATE_POSITIONS,
        (
            WITH NEXT_POSITIONS AS (
                SELECT
                    ITEM_ID,
                    STORE_ID,
                    SNAPSHOT_DATE,
                    EFFECTIVE_TO,
                    IS_CURRENT,
                    LEAD(SNAPSHOT_DATE) OVER (
                        PARTITION BY ITEM_ID, STORE_ID
                        ORDER BY SNAPSHOT_DATE
                    ) AS NEXT_SNAPSHOT_DATE
                FROM {{ snowflake_database }}.FACTS.FCT_INVENTORY
            )
            SELECT COUNT(*)
            FROM NEXT_POSITIONS
            WHERE (
                    NEXT_SNAPSHOT_DATE IS NULL
                    AND (NOT IS_CURRENT OR EFFECTIVE_TO IS NOT NULL)
                )
                OR (
                    NEXT_SNAPSHOT_DATE IS NOT NULL
                    AND (
                        IS_CURRENT
                        OR EFFECTIVE_TO <> DATEADD(DAY, -1, NEXT_SNAPSHOT_DATE)
                    )
                )
        ) AS INVALID_CLOSURES,
        (
            SELECT COUNT(*)
            FROM (
                SELECT ITEM_ID, STORE_ID
                FROM {{ snowflake_database }}.FACTS.FCT_INVENTORY
                GROUP BY ITEM_ID, STORE_ID
                HAVING COUNT_IF(IS_CURRENT) <> 1
            )
        ) AS BAD_CURRENT_FLAGS
),
STORE_SCD2_CASE AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT STORE_ID
                FROM {{ snowflake_database }}.DIMENSIONS.DIM_LOCN
                WHERE STORE_ID IS NOT NULL
                GROUP BY STORE_ID
                HAVING COUNT(*) > 1
            )
        ) AS STORES_WITH_HISTORY,
        (
            SELECT COUNT(*)
            FROM (
                SELECT STORE_ID
                FROM {{ snowflake_database }}.DIMENSIONS.DIM_LOCN
                WHERE STORE_ID IS NOT NULL
                GROUP BY STORE_ID
                HAVING COUNT(DISTINCT DISTRICT_SK) > 1
            )
        ) AS STORES_WITH_DISTRICT_CHANGE,
        (
            WITH HISTORY_WITH_NEXT AS (
                SELECT
                    STORE_ID,
                    EFFECTIVE_FROM,
                    EFFECTIVE_TO,
                    IS_CURRENT,
                    LEAD(EFFECTIVE_FROM) OVER (
                        PARTITION BY STORE_ID
                        ORDER BY EFFECTIVE_FROM, LOCN_SK
                    ) AS NEXT_EFFECTIVE_FROM
                FROM {{ snowflake_database }}.DIMENSIONS.DIM_LOCN
            )
            SELECT COUNT(*)
            FROM HISTORY_WITH_NEXT
            WHERE (IS_CURRENT AND EFFECTIVE_TO IS NOT NULL)
               OR (NOT IS_CURRENT AND EFFECTIVE_TO IS NULL)
               OR (
                    NOT IS_CURRENT
                    AND NEXT_EFFECTIVE_FROM IS NOT NULL
                    AND EFFECTIVE_TO <> DATEADD(DAY, -1, NEXT_EFFECTIVE_FROM)
               )
        ) AS INVALID_HISTORY_ROWS,
        (
            SELECT COUNT(*)
            FROM (
                SELECT STORE_ID
                FROM {{ snowflake_database }}.DIMENSIONS.DIM_LOCN
                WHERE STORE_ID IS NOT NULL
                GROUP BY STORE_ID
                HAVING COUNT_IF(IS_CURRENT) <> 1
                   OR (COUNT(*) > 1 AND COUNT_IF(NOT IS_CURRENT) < 1)
            )
        ) AS STORES_WITH_BAD_CURRENT_FLAG,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.DIMENSIONS.DIM_ITEM AS ITEM
            LEFT JOIN {{ snowflake_database }}.DIMENSIONS.DIM_SUBCLASS AS SUBCLASS
                ON SUBCLASS.SUBCLASS_SK = ITEM.SUBCLASS_SK
            LEFT JOIN {{ snowflake_database }}.DIMENSIONS.DIM_CATEGORY AS CATEGORY
                ON CATEGORY.CATEGORY_SK = SUBCLASS.CATEGORY_SK
            WHERE (ITEM.SUBCLASS_SK IS NOT NULL AND SUBCLASS.SUBCLASS_SK IS NULL)
               OR (SUBCLASS.CATEGORY_SK IS NOT NULL AND CATEGORY.CATEGORY_SK IS NULL)
        ) AS BROKEN_ITEM_HIERARCHY_ROWS,
        (
            SELECT COUNT(*)
            FROM {{ snowflake_database }}.DIMENSIONS.DIM_LOCN AS STORE
            LEFT JOIN {{ snowflake_database }}.DIMENSIONS.DIM_DISTRICT AS DISTRICT
                ON DISTRICT.DISTRICT_SK = STORE.DISTRICT_SK
            LEFT JOIN {{ snowflake_database }}.DIMENSIONS.DIM_REGION AS REGION
                ON REGION.REGION_SK = DISTRICT.REGION_SK
            WHERE (STORE.DISTRICT_SK IS NOT NULL AND DISTRICT.DISTRICT_SK IS NULL)
               OR (DISTRICT.REGION_SK IS NOT NULL AND REGION.REGION_SK IS NULL)
        ) AS BROKEN_STORE_HIERARCHY_ROWS
),
BRONZE_BATCHES AS (
    SELECT 'pos_sales' AS SOURCE_NAME, SRC_FILE_NM, BATCH_ID
    FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES

    UNION ALL

    SELECT 'item_master', SRC_FILE_NM, BATCH_ID
    FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER

    UNION ALL

    SELECT 'inventory', SRC_FILE_NM, BATCH_ID
    FROM {{ snowflake_database }}.BRONZE.BRONZE_INVENTORY

    UNION ALL

    SELECT 'store_master', SRC_FILE_NM, BATCH_ID
    FROM {{ snowflake_database }}.BRONZE.BRONZE_STORE_MASTER
),
BRONZE_BATCH_CASE AS (
    SELECT
        COUNT(*) AS ROW_COUNT,
        COUNT_IF(
            BATCH_ID IS NULL
            OR NOT REGEXP_LIKE(
                BATCH_ID,
                '^[0-9]{8}_(pos_sales|item_master|inventory|store_master)_[0-9a-f]{16}$'
            )
        ) AS INVALID_BATCH_IDS,
        (
            SELECT COUNT(*)
            FROM (
                SELECT SOURCE_NAME, SRC_FILE_NM
                FROM BRONZE_BATCHES
                GROUP BY SOURCE_NAME, SRC_FILE_NM
                HAVING COUNT(DISTINCT BATCH_ID) <> 1
            )
        ) AS FILES_WITH_MULTIPLE_BATCH_IDS,
        (
            SELECT COUNT(*)
            FROM (
                SELECT BATCH_ID
                FROM BRONZE_BATCHES
                GROUP BY BATCH_ID
                HAVING COUNT(DISTINCT SOURCE_NAME || '|' || SRC_FILE_NM) > 1
            )
        ) AS BATCH_IDS_SHARED_BY_FILES
    FROM BRONZE_BATCHES
)
SELECT
    'null_item_ids' AS TEST_CASE,
    IFF(
        SOURCE_COUNT > 0
        AND QUARANTINE_COUNT >= SOURCE_COUNT
        AND SILVER_COUNT = 0,
        'PASS',
        'FAIL'
    ) AS TEST_STATUS,
    'source=' || SOURCE_COUNT
        || ', quarantined=' || QUARANTINE_COUNT
        || ', silver=' || SILVER_COUNT AS DETAILS
FROM NULL_ITEM_CASE

UNION ALL

SELECT
    'non_positive_quantity',
    IFF(
        SOURCE_COUNT > 0
        AND QUARANTINE_COUNT >= SOURCE_COUNT
        AND SILVER_COUNT = 0,
        'PASS',
        'FAIL'
    ),
    'source=' || SOURCE_COUNT
        || ', quarantined=' || QUARANTINE_COUNT
        || ', silver=' || SILVER_COUNT
FROM NEGATIVE_QUANTITY_CASE

UNION ALL

SELECT
    'missing_effective_item_master',
    IFF(
        SOURCE_COUNT > 0
        AND QUARANTINE_COUNT >= SOURCE_COUNT
        AND SILVER_COUNT = 0,
        'PASS',
        'FAIL'
    ),
    'source_without_effective_item=' || SOURCE_COUNT
        || ', quarantined=' || QUARANTINE_COUNT
        || ', silver_with_missing_item=' || SILVER_COUNT
FROM MISSING_ITEM_CASE

UNION ALL

SELECT
    'duplicate_transaction_line',
    IFF(
        BRONZE_DUPLICATE_GROUPS > 0
        AND SILVER_DUPLICATE_GROUPS = 0
        AND RECON_DUPLICATE_COUNT > 0,
        'PASS',
        'FAIL'
    ),
    'bronze_duplicate_groups=' || BRONZE_DUPLICATE_GROUPS
        || ', silver_duplicate_groups=' || SILVER_DUPLICATE_GROUPS
        || ', recon_duplicates=' || RECON_DUPLICATE_COUNT
FROM DUPLICATE_LINE_CASE

UNION ALL

SELECT
    'store_district_scd2',
    IFF(
        STORES_WITH_HISTORY > 0
        AND STORES_WITH_DISTRICT_CHANGE > 0
        AND INVALID_HISTORY_ROWS = 0
        AND STORES_WITH_BAD_CURRENT_FLAG = 0
        AND BROKEN_ITEM_HIERARCHY_ROWS = 0
        AND BROKEN_STORE_HIERARCHY_ROWS = 0,
        'PASS',
        'FAIL'
    ),
    'stores_with_history=' || STORES_WITH_HISTORY
        || ', stores_with_district_change=' || STORES_WITH_DISTRICT_CHANGE
        || ', invalid_history_rows=' || INVALID_HISTORY_ROWS
        || ', stores_with_bad_current_flag=' || STORES_WITH_BAD_CURRENT_FLAG
        || ', broken_item_hierarchy_rows=' || BROKEN_ITEM_HIERARCHY_ROWS
        || ', broken_store_hierarchy_rows=' || BROKEN_STORE_HIERARCHY_ROWS
FROM STORE_SCD2_CASE

UNION ALL

SELECT
    'date_scoped_bronze_batch_ids',
    IFF(
        ROW_COUNT > 0
        AND INVALID_BATCH_IDS = 0
        AND FILES_WITH_MULTIPLE_BATCH_IDS = 0
        AND BATCH_IDS_SHARED_BY_FILES = 0,
        'PASS',
        'FAIL'
    ),
    'rows=' || ROW_COUNT
        || ', invalid_batch_ids=' || INVALID_BATCH_IDS
        || ', files_with_multiple_batch_ids=' || FILES_WITH_MULTIPLE_BATCH_IDS
        || ', batch_ids_shared_by_files=' || BATCH_IDS_SHARED_BY_FILES
FROM BRONZE_BATCH_CASE

UNION ALL

SELECT
    'item_brand_scd1',
    IFF(BRAND_MISMATCHES = 0, 'PASS', 'FAIL'),
    'historical_versions_with_stale_brand=' || BRAND_MISMATCHES
FROM ITEM_SCD1_CASE

UNION ALL

SELECT
    'inventory_positions_close',
    IFF(
        DUPLICATE_POSITIONS = 0
        AND INVALID_CLOSURES = 0
        AND BAD_CURRENT_FLAGS = 0,
        'PASS',
        'FAIL'
    ),
    'duplicate_positions=' || DUPLICATE_POSITIONS
        || ', invalid_closures=' || INVALID_CLOSURES
        || ', bad_current_flags=' || BAD_CURRENT_FLAGS
FROM INVENTORY_SCD_CASE;
