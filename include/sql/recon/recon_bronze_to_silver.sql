-- Per-file Bronze = Silver + rejected rows + deduped rows.
DELETE FROM {{ snowflake_database }}.CONTROL.RECONCILIATION
WHERE SOURCE_LAYER = 'BRONZE'
  AND TARGET_LAYER = 'SILVER';

INSERT INTO {{ snowflake_database }}.CONTROL.RECONCILIATION (
    RECON_ID,
    BATCH_ID,
    SOURCE_NAME,
    SOURCE_LAYER,
    TARGET_LAYER,
    SOURCE_RECORD_COUNT,
    TARGET_RECORD_COUNT,
    REJECTED_RECORD_COUNT,
    DUPLICATE_RECORD_COUNT,
    SOURCE_MEASURE_TOTAL,
    TARGET_MEASURE_TOTAL,
    RECON_STATUS,
    RECON_MESSAGE,
    RECON_TS
)
WITH BRONZE_COUNTS AS (
    SELECT 'pos_sales' AS SOURCE_NAME, BATCH_ID, COUNT(*) AS ROW_COUNT
    FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES
    GROUP BY BATCH_ID
    UNION ALL
    SELECT 'item_master', BATCH_ID, COUNT(*)
    FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER
    GROUP BY BATCH_ID
    UNION ALL
    SELECT 'inventory', BATCH_ID, COUNT(*)
    FROM {{ snowflake_database }}.BRONZE.BRONZE_INVENTORY
    GROUP BY BATCH_ID
    UNION ALL
    SELECT 'store_master', BATCH_ID, COUNT(*)
    FROM {{ snowflake_database }}.BRONZE.BRONZE_STORE_MASTER
    GROUP BY BATCH_ID
),
SILVER_COUNTS AS (
    SELECT 'pos_sales' AS SOURCE_NAME, BATCH_ID, COUNT(*) AS ROW_COUNT,
        SUM(NET_SALES_AMT) AS MEASURE_TOTAL
    FROM {{ snowflake_database }}.SILVER.SILVER_SALES
    GROUP BY BATCH_ID
    UNION ALL
    SELECT 'item_master', BATCH_ID, COUNT(*), NULL::NUMBER(38, 6)
    FROM {{ snowflake_database }}.SILVER.SILVER_ITEM_MASTER
    GROUP BY BATCH_ID
    UNION ALL
    SELECT 'inventory', BATCH_ID, COUNT(*), SUM(INVENTORY_VALUE)
    FROM {{ snowflake_database }}.SILVER.SILVER_INVENTORY
    GROUP BY BATCH_ID
    UNION ALL
    SELECT 'store_master', BATCH_ID, COUNT(*), NULL::NUMBER(38, 6)
    FROM {{ snowflake_database }}.SILVER.SILVER_STORE_MASTER
    GROUP BY BATCH_ID
),
QUARANTINE_COUNTS AS (
    SELECT BATCH_ID, COUNT(*) AS ROW_COUNT
    FROM {{ snowflake_database }}.QUARANTINE.RECORD_QUARANTINE
    WHERE SOURCE_LAYER = 'BRONZE'
    GROUP BY BATCH_ID
),
FILE_BATCHES AS (
    SELECT
        BATCH_ID,
        SOURCE_NAME,
        SRC_FILE_NM,
        SRC_ROW_COUNT,
        STATUS
    FROM {{ snowflake_database }}.CONTROL.INGEST_BATCH
    WHERE SOURCE_NAME IN ('pos_sales', 'item_master', 'inventory', 'store_master')
      AND REGEXP_LIKE(
          BATCH_ID,
          '^[0-9]{8}_(pos_sales|item_master|inventory|store_master)_[0-9a-f]{16}$'
      )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY BATCH_ID
        ORDER BY UPDATED_TS DESC
    ) = 1
),
METRICS AS (
    SELECT
        FILE_BATCHES.BATCH_ID,
        FILE_BATCHES.SOURCE_NAME,
        FILE_BATCHES.SRC_FILE_NM,
        COALESCE(FILE_BATCHES.SRC_ROW_COUNT, BRONZE_COUNTS.ROW_COUNT, 0)
            AS SOURCE_COUNT,
        FILE_BATCHES.STATUS,
        COALESCE(BRONZE_COUNTS.ROW_COUNT, 0) AS BRONZE_COUNT,
        COALESCE(SILVER_COUNTS.ROW_COUNT, 0) AS SILVER_COUNT,
        IFF(
            FILE_BATCHES.STATUS = 'QUARANTINED',
            COALESCE(FILE_BATCHES.SRC_ROW_COUNT, 0),
            COALESCE(QUARANTINE_COUNTS.ROW_COUNT, 0)
        ) AS REJECTED_COUNT,
        GREATEST(
            COALESCE(BRONZE_COUNTS.ROW_COUNT, 0)
                - IFF(
                    FILE_BATCHES.STATUS = 'QUARANTINED',
                    0,
                    COALESCE(QUARANTINE_COUNTS.ROW_COUNT, 0)
                )
                - COALESCE(SILVER_COUNTS.ROW_COUNT, 0),
            0
        ) AS DEDUPED_COUNT,
        SILVER_COUNTS.MEASURE_TOTAL
    FROM FILE_BATCHES
    LEFT JOIN BRONZE_COUNTS
        ON BRONZE_COUNTS.BATCH_ID = FILE_BATCHES.BATCH_ID
        AND BRONZE_COUNTS.SOURCE_NAME = FILE_BATCHES.SOURCE_NAME
    LEFT JOIN SILVER_COUNTS
        ON SILVER_COUNTS.BATCH_ID = FILE_BATCHES.BATCH_ID
        AND SILVER_COUNTS.SOURCE_NAME = FILE_BATCHES.SOURCE_NAME
    LEFT JOIN QUARANTINE_COUNTS
        ON QUARANTINE_COUNTS.BATCH_ID = FILE_BATCHES.BATCH_ID
)
SELECT
    UUID_STRING(),
    BATCH_ID,
    SOURCE_NAME,
    'BRONZE',
    'SILVER',
    SOURCE_COUNT,
    SILVER_COUNT,
    REJECTED_COUNT,
    DEDUPED_COUNT,
    NULL,
    MEASURE_TOTAL,
    IFF(
        SOURCE_COUNT = SILVER_COUNT + REJECTED_COUNT + DEDUPED_COUNT
        AND (
            STATUS = 'QUARANTINED'
            OR SOURCE_COUNT = BRONZE_COUNT
        ),
        'PASS',
        'FAIL'
    ),
    'file=' || SRC_FILE_NM
        || ', source=' || SOURCE_COUNT
        || ', bronze=' || BRONZE_COUNT
        || ', silver=' || SILVER_COUNT
        || ', rejected=' || REJECTED_COUNT
        || ', deduped=' || DEDUPED_COUNT,
    CURRENT_TIMESTAMP()
FROM METRICS;
