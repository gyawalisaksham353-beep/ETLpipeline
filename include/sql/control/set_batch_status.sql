-- Update the processing status of a batch.
UPDATE {{ snowflake_database }}.CONTROL.INGEST_BATCH
SET
    STATUS = %s,
    REJECTION_REASON = %s,
    UPDATED_TS = CURRENT_TIMESTAMP()
WHERE BATCH_ID = %s;
