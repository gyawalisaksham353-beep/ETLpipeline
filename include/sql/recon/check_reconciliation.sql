-- Return any failed reconciliation results for a layer boundary.
SELECT
    RECON_ID,
    BATCH_ID,
    SOURCE_NAME,
    SOURCE_LAYER,
    TARGET_LAYER,
    RECON_STATUS,
    RECON_MESSAGE
FROM {{ snowflake_database }}.CONTROL.RECONCILIATION
WHERE SOURCE_LAYER = %s
  AND TARGET_LAYER = %s
  AND RECON_STATUS = 'FAIL';
