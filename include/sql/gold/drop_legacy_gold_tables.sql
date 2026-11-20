-- Replace only the Gold tables owned by this pipeline. They are fully
-- derivable from Silver, so the next Gold load rebuilds them with the current
-- dimensional model. CASCADE removes dependent legacy foreign keys/views.
DROP TABLE IF EXISTS {{ snowflake_database }}.FACTS.FCT_SALES CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.FACTS.FCT_INVENTORY CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_ITEM CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_LOCN CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_DAY CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_SUBCLASS CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_DISTRICT CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_CATEGORY CASCADE;
DROP TABLE IF EXISTS {{ snowflake_database }}.DIMENSIONS.DIM_REGION CASCADE;
