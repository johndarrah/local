--
SELECT
  project_id
  , id
  , name --, raw_sql
FROM squarewave.raw_oltp.job
WHERE
  project_id IN (41, 132)                              -- app_cash_cs and app_datamart_cco projects respectively
  AND state NOT IN ('DELETED', 'ARCHIVED', 'INACTIVE') -- other states include: 'ACTIVE', 'DEGRADED', 'DISABLED'
  AND raw_sql ILIKE ANY ('%raw_oltp%', '%cash_pii%')
;

--   Get all Datapals + DMC ETL SQL statements, source_tables and target_table, and other SQL_ITEM dependencies.
SELECT
  job_item.*
FROM squarewave.raw_oltp.job_item
LEFT JOIN squarewave.raw_oltp.job
  ON job_item.job_id = job.id
WHERE
  job.project_id IN (41, 132)
  AND job.state NOT IN ('DELETED', 'ARCHIVED', 'INACTIVE') -- other states include: 'ACTIVE', 'DEGRADED', 'DISABLED'
  //    AND job_item.job_id = 13265
  //    AND target_table = 'app_cash_cs.public.messaging_quick_texts'
QUALIFY
  job_item.job_version = MAX(job_item.job_version) OVER (PARTITION BY job_item.job_id)
;

-- List of all Source Tables for a given job_id
WITH
  base AS (
    SELECT
      job_item.*
      , STRTOK_TO_ARRAY(REPLACE(TRIM(job_item.source_tables, '[\"]'), '", "', ' ')) AS source_table_array --this garbage nested functions is necessary because an array-like thing is a varchar. ><
    FROM squarewave.raw_oltp.job_item
    LEFT JOIN squarewave.raw_oltp.job
      ON job_item.job_id = job.id
    WHERE
      job_item.job_id = 13265
    QUALIFY
      job_item.job_version = MAX(job_item.job_version) OVER (PARTITION BY job_item.job_id)
  )

SELECT DISTINCT
  TRIM(f.value, '"')                         AS source_table_address
  , SPLIT_PART(source_table_address, '.', 1) AS source_database_name
  , SPLIT_PART(source_table_address, '.', 2) AS source_schema_name
  , SPLIT_PART(source_table_address, '.', 3) AS source_table_name
FROM base
   , LATERAL FLATTEN(INPUT => source_table_array) AS f
;

-- List of all Target Tables for a given job_id
SELECT DISTINCT
  job_item.target_table
  , SPLIT_PART(job_item.target_table, '.', 1) AS target_database_name
  , SPLIT_PART(job_item.target_table, '.', 2) AS target_schema_name
  , SPLIT_PART(job_item.target_table, '.', 3) AS target_table_name
FROM squarewave.raw_oltp.job_item
LEFT JOIN squarewave.raw_oltp.job
  ON job_item.job_id = job.id
WHERE
  job_item.job_id = 13265
QUALIFY
  job_item.job_version = MAX(job_item.job_version) OVER (PARTITION BY job_item.job_id)
;
