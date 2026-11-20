-- Detect Gold tables created by the earlier modelling exercise.
-- Those tables use legacy natural/surrogate key names (for example ITEM_KY
-- instead of ITEM_SK) and NOT NULL constraints that block the current model.
SELECT COUNT(*) AS LEGACY_TABLE_COUNT
FROM {{ snowflake_database }}.INFORMATION_SCHEMA.COLUMNS
WHERE (TABLE_SCHEMA = 'DIMENSIONS' AND TABLE_NAME = 'DIM_ITEM'
       AND COLUMN_NAME = 'ITEM_KY')
   OR (TABLE_SCHEMA = 'DIMENSIONS' AND TABLE_NAME = 'DIM_LOCN'
       AND COLUMN_NAME = 'LOCN_KY')
   OR (TABLE_SCHEMA = 'DIMENSIONS' AND TABLE_NAME = 'DIM_DAY'
       AND COLUMN_NAME = 'DAY_KY')
   OR (TABLE_SCHEMA = 'FACTS' AND TABLE_NAME = 'FCT_SALES'
       AND COLUMN_NAME = 'ITEM_KY')
   OR (TABLE_SCHEMA = 'FACTS' AND TABLE_NAME = 'FCT_INVENTORY'
       AND COLUMN_NAME = 'ITEM_KY');
