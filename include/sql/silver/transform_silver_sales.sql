-- =====================================================
-- SILVER SALES TRANSFORMATION
-- Bronze -> validate -> quarantine rejects -> merge valid
-- Business key: TRANSACTION_ID + TRANSACTION_LINE_ID
-- =====================================================

-- Rebuild record-level quarantine for this source.
DELETE FROM {{ snowflake_database }}.QUARANTINE.RECORD_QUARANTINE
WHERE SOURCE_NAME = 'pos_sales'
  AND SOURCE_LAYER = 'BRONZE';

INSERT INTO {{ snowflake_database }}.QUARANTINE.RECORD_QUARANTINE (
    BATCH_ID,
    SOURCE_NAME,
    SOURCE_LAYER,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    RAW_RECORD,
    REASON_CODE,
    REJECTION_REASON,
    LOAD_DATE,
    INGEST_TS,
    QUARANTINE_TS
)
WITH NORMALIZED AS (
    SELECT
        TO_VARCHAR(TRANSACTION_ID) AS TRANSACTION_ID,
        TO_VARCHAR(TRANSACTION_LINE_ID) AS TRANSACTION_LINE_ID,
        TO_VARCHAR(STORE_ID) AS STORE_ID,
        TO_VARCHAR(ITEM_ID) AS ITEM_ID,
        TRY_TO_DATE(TO_VARCHAR(TRANSACTION_DATE)) AS TRANSACTION_DATE,
        TRY_TO_NUMBER(TO_VARCHAR(QUANTITY), 10, 2) AS QUANTITY,
        TRY_TO_NUMBER(TO_VARCHAR(UNIT_PRICE), 12, 2) AS UNIT_PRICE,
        TRY_TO_NUMBER(TO_VARCHAR(DISCOUNT_AMT), 12, 2) AS DISCOUNT_AMT,
        SRC_FILE_NM,
        SRC_ROW_NUM,
        INGEST_TS,
        INGEST_DT,
        BATCH_ID
    FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES
),
VALIDATED AS (
    SELECT
        NORMALIZED.*,
        COALESCE(
            COALESCE(TRIM(TRANSACTION_ID), '') <> ''
            AND COALESCE(TRIM(TRANSACTION_LINE_ID), '') <> ''
            AND COALESCE(TRIM(STORE_ID), '') <> ''
            AND COALESCE(TRIM(ITEM_ID), '') <> ''
            AND TRANSACTION_DATE IS NOT NULL
            AND QUANTITY > 0
            AND UNIT_PRICE >= 0
            AND DISCOUNT_AMT >= 0
            AND DISCOUNT_AMT <= QUANTITY * UNIT_PRICE,
            FALSE
        ) AS IS_VALID
    FROM NORMALIZED
),
QUARANTINED AS (
    SELECT *
    FROM VALIDATED
    WHERE NOT IS_VALID

    UNION ALL

    SELECT *
    FROM VALIDATED
    WHERE IS_VALID
      AND NOT EXISTS (
          SELECT 1
          FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER AS ITEM_MASTER
          WHERE ITEM_MASTER.ITEM_ID = VALIDATED.ITEM_ID
            AND COALESCE(TRIM(ITEM_MASTER.ITEM_ID), '') <> ''
            AND COALESCE(TRIM(ITEM_MASTER.ITEM_NAME), '') <> ''
            AND TRY_TO_NUMBER(ITEM_MASTER.UNIT_COST, 38, 6) >= 0
            AND TRY_TO_NUMBER(ITEM_MASTER.LIST_PRICE, 38, 6) >= 0
            AND COALESCE(
                TRY_TO_DATE(
                    REGEXP_SUBSTR(ITEM_MASTER.SRC_FILE_NM, '[0-9]{8}'),
                    'YYYYMMDD'
                ),
                ITEM_MASTER.INGEST_DT
            ) <= VALIDATED.TRANSACTION_DATE
      )
)
SELECT
    BATCH_ID,
    'pos_sales',
    'BRONZE',
    SRC_FILE_NM,
    SRC_ROW_NUM,
    OBJECT_CONSTRUCT(
        'transaction_id', TRANSACTION_ID,
        'line_id', TRANSACTION_LINE_ID,
        'store_id', STORE_ID,
        'item_id', ITEM_ID,
        'transaction_date', TRANSACTION_DATE,
        'sales_qty', QUANTITY,
        'unit_price', UNIT_PRICE,
        'discount_amt', DISCOUNT_AMT
    ),
    CASE
        WHEN COALESCE(TRIM(ITEM_ID), '') = '' THEN 'NULL_ITEM_ID'
        WHEN QUANTITY < 0 THEN 'NEGATIVE_QTY'
        WHEN QUANTITY = 0 THEN 'ZERO_QTY'
        WHEN IS_VALID
          AND NOT EXISTS (
              SELECT 1
              FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER AS ITEM_MASTER
              WHERE ITEM_MASTER.ITEM_ID = QUARANTINED.ITEM_ID
                AND COALESCE(TRIM(ITEM_MASTER.ITEM_ID), '') <> ''
                AND COALESCE(TRIM(ITEM_MASTER.ITEM_NAME), '') <> ''
                AND TRY_TO_NUMBER(ITEM_MASTER.UNIT_COST, 38, 6) >= 0
                AND TRY_TO_NUMBER(ITEM_MASTER.LIST_PRICE, 38, 6) >= 0
                AND COALESCE(
                    TRY_TO_DATE(
                        REGEXP_SUBSTR(ITEM_MASTER.SRC_FILE_NM, '[0-9]{8}'),
                        'YYYYMMDD'
                    ),
                    ITEM_MASTER.INGEST_DT
                ) <= QUARANTINED.TRANSACTION_DATE
          ) THEN 'ORPHAN_ITEM'
        WHEN TRANSACTION_DATE IS NULL THEN 'INVALID_TRANSACTION_DATE'
        WHEN QUANTITY IS NULL THEN 'INVALID_QTY'
        WHEN UNIT_PRICE IS NULL OR UNIT_PRICE < 0 THEN 'INVALID_UNIT_PRICE'
        WHEN DISCOUNT_AMT IS NULL OR DISCOUNT_AMT < 0 THEN 'INVALID_DISCOUNT'
        WHEN DISCOUNT_AMT > QUANTITY * UNIT_PRICE THEN 'DISCOUNT_EXCEEDS_GROSS'
        ELSE 'INVALID_SALES_ROW'
    END,
    RTRIM(CONCAT(
        IFF(COALESCE(TRIM(TRANSACTION_ID), '') = '', 'missing transaction id; ', ''),
        IFF(COALESCE(TRIM(TRANSACTION_LINE_ID), '') = '', 'missing transaction line id; ', ''),
        IFF(COALESCE(TRIM(STORE_ID), '') = '', 'missing store id; ', ''),
        IFF(COALESCE(TRIM(ITEM_ID), '') = '', 'missing item id; ', ''),
        IFF(TRANSACTION_DATE IS NULL, 'invalid transaction date; ', ''),
        IFF(QUANTITY IS NULL, 'invalid sales quantity; ', ''),
        IFF(QUANTITY <= 0, 'non-positive sales quantity; ', ''),
        IFF(UNIT_PRICE IS NULL, 'invalid unit price; ', ''),
        IFF(UNIT_PRICE < 0, 'negative unit price; ', ''),
        IFF(DISCOUNT_AMT IS NULL, 'invalid discount amount; ', ''),
        IFF(DISCOUNT_AMT < 0, 'negative discount amount; ', ''),
        IFF(
            COALESCE(DISCOUNT_AMT, 0) > COALESCE(QUANTITY, 0) * COALESCE(UNIT_PRICE, 0),
            'discount exceeds gross sales amount; ',
            ''
        ),
        IFF(
            IS_VALID,
            'no valid item master record effective on transaction date; ',
            ''
        )
    ), '; '),
    COALESCE(
        TRY_TO_DATE(REGEXP_SUBSTR(SRC_FILE_NM, '[0-9]{8}'), 'YYYYMMDD'),
        INGEST_DT
    ),
    INGEST_TS,
    CURRENT_TIMESTAMP()
FROM QUARANTINED;

-- Refresh silver from the current validated bronze contents.
DELETE FROM {{ snowflake_database }}.SILVER.SILVER_SALES
WHERE TRANSACTION_DATE = TO_DATE(%s);

MERGE INTO {{ snowflake_database }}.SILVER.SILVER_SALES AS TARGET
USING (
    WITH NORMALIZED AS (
        SELECT
            TO_VARCHAR(TRANSACTION_ID) AS TRANSACTION_ID,
            TO_VARCHAR(TRANSACTION_LINE_ID) AS TRANSACTION_LINE_ID,
            TO_VARCHAR(STORE_ID) AS STORE_ID,
            TO_VARCHAR(ITEM_ID) AS ITEM_ID,
            TRY_TO_DATE(TO_VARCHAR(TRANSACTION_DATE)) AS TRANSACTION_DATE,
            TRY_TO_NUMBER(TO_VARCHAR(QUANTITY), 10, 2) AS QUANTITY,
            TRY_TO_NUMBER(TO_VARCHAR(UNIT_PRICE), 12, 2) AS UNIT_PRICE,
            TRY_TO_NUMBER(TO_VARCHAR(DISCOUNT_AMT), 12, 2) AS DISCOUNT_AMT,
            SRC_FILE_NM,
            SRC_ROW_NUM,
            INGEST_TS,
            BATCH_ID
        FROM {{ snowflake_database }}.BRONZE.BRONZE_SALES
        WHERE TRY_TO_DATE(TO_VARCHAR(TRANSACTION_DATE)) = TO_DATE(%s)
    ),
    VALIDATED AS (
        SELECT
            NORMALIZED.*,
            COALESCE(
                COALESCE(TRIM(TRANSACTION_ID), '') <> ''
                AND COALESCE(TRIM(TRANSACTION_LINE_ID), '') <> ''
                AND COALESCE(TRIM(STORE_ID), '') <> ''
                AND COALESCE(TRIM(ITEM_ID), '') <> ''
                AND TRANSACTION_DATE IS NOT NULL
                AND QUANTITY > 0
                AND UNIT_PRICE >= 0
                AND DISCOUNT_AMT >= 0
                AND DISCOUNT_AMT <= QUANTITY * UNIT_PRICE,
                FALSE
            ) AS IS_VALID
        FROM NORMALIZED
    )
    SELECT
        TRANSACTION_ID,
        TRANSACTION_DATE,
        TRANSACTION_LINE_ID,
        STORE_ID,
        ITEM_ID,
        QUANTITY,
        UNIT_PRICE,
        DISCOUNT_AMT,
        QUANTITY * UNIT_PRICE AS GROSS_SALES_AMT,
        (QUANTITY * UNIT_PRICE) - DISCOUNT_AMT AS NET_SALES_AMT,
        SRC_FILE_NM,
        SRC_ROW_NUM,
        INGEST_TS,
        BATCH_ID
    FROM VALIDATED
    WHERE IS_VALID
      AND EXISTS (
          SELECT 1
          FROM {{ snowflake_database }}.BRONZE.BRONZE_ITEM_MASTER AS ITEM_MASTER
          WHERE ITEM_MASTER.ITEM_ID = VALIDATED.ITEM_ID
            AND COALESCE(TRIM(ITEM_MASTER.ITEM_ID), '') <> ''
            AND COALESCE(TRIM(ITEM_MASTER.ITEM_NAME), '') <> ''
            AND TRY_TO_NUMBER(ITEM_MASTER.UNIT_COST, 38, 6) >= 0
            AND TRY_TO_NUMBER(ITEM_MASTER.LIST_PRICE, 38, 6) >= 0
            AND COALESCE(
                TRY_TO_DATE(
                    REGEXP_SUBSTR(ITEM_MASTER.SRC_FILE_NM, '[0-9]{8}'),
                    'YYYYMMDD'
                ),
                ITEM_MASTER.INGEST_DT
            ) <= VALIDATED.TRANSACTION_DATE
      )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRANSACTION_ID, TRANSACTION_LINE_ID
        ORDER BY INGEST_TS DESC, SRC_ROW_NUM DESC
    ) = 1
) AS SOURCE
ON
    TARGET.TRANSACTION_ID = SOURCE.TRANSACTION_ID
    AND TARGET.TRANSACTION_LINE_ID = SOURCE.TRANSACTION_LINE_ID

WHEN MATCHED THEN UPDATE SET
    TARGET.TRANSACTION_DATE = SOURCE.TRANSACTION_DATE,
    TARGET.ITEM_ID = SOURCE.ITEM_ID,
    TARGET.QUANTITY = SOURCE.QUANTITY,
    TARGET.UNIT_PRICE = SOURCE.UNIT_PRICE,
    TARGET.DISCOUNT_AMT = SOURCE.DISCOUNT_AMT,
    TARGET.GROSS_SALES_AMT = SOURCE.GROSS_SALES_AMT,
    TARGET.NET_SALES_AMT = SOURCE.NET_SALES_AMT,
    TARGET.SRC_FILE_NM = SOURCE.SRC_FILE_NM,
    TARGET.SRC_ROW_NUM = SOURCE.SRC_ROW_NUM,
    TARGET.INGEST_TS = SOURCE.INGEST_TS,
    TARGET.BATCH_ID = SOURCE.BATCH_ID,
    TARGET.UPDATED_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_LINE_ID,
    STORE_ID,
    ITEM_ID,
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_AMT,
    GROSS_SALES_AMT,
    NET_SALES_AMT,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    INGEST_TS,
    BATCH_ID,
    CREATED_TS
)
VALUES (
    SOURCE.TRANSACTION_ID,
    SOURCE.TRANSACTION_DATE,
    SOURCE.TRANSACTION_LINE_ID,
    SOURCE.STORE_ID,
    SOURCE.ITEM_ID,
    SOURCE.QUANTITY,
    SOURCE.UNIT_PRICE,
    SOURCE.DISCOUNT_AMT,
    SOURCE.GROSS_SALES_AMT,
    SOURCE.NET_SALES_AMT,
    SOURCE.SRC_FILE_NM,
    SOURCE.SRC_ROW_NUM,
    SOURCE.INGEST_TS,
    SOURCE.BATCH_ID,
    CURRENT_TIMESTAMP()
);
