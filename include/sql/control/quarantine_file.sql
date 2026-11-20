-- Record a whole-file quarantine event.
INSERT INTO {{ snowflake_database }}.QUARANTINE.FILE_QUARANTINE (
    BATCH_ID,
    SOURCE_NAME,
    SRC_FILE_NM,
    REJECTION_REASON,
    EXPECTED_HEADERS,
    ACTUAL_HEADERS,
    INGEST_TS,
    QUARANTINE_TS
)
VALUES (
    %s, %s, %s, %s, %s, %s,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
