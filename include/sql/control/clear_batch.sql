-- Clear transient batch state before re-registering a file (idempotent discovery).
DELETE FROM {{ snowflake_database }}.QUARANTINE.FILE_QUARANTINE
WHERE BATCH_ID = %s;

DELETE FROM {{ snowflake_database }}.CONTROL.RECONCILIATION
WHERE BATCH_ID = %s;
