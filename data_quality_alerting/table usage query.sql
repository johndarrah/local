SELECT DISTINCT
  user_name
  , query_type
  , query_text
  , MAX(end_date) AS query_last_run_ds
FROM app_datamart_cco.data_pipelines.snowflake_query_history
WHERE
  1 = 1
  --   date filters
  AND YEAR(end_date) = 2023
  --   not owned by Datapals
  AND LOWER(user_name) NOT IN ('anomalo', 'mellisor', 'mfazza', 'lakehouse',
                               'app_cash_3pr', 'app_datamart_cco', 'fivetran', 'v1_edge_collibra',
                               'dmc_pii', 'app_cash_cs')
  --   database.schema.table filter
  AND query_text ILIKE '%app_cash_cs.public.support_cases%'
  --   column filter
  AND query_text ILIKE '%notes%'
  -- looker only
  AND query_text ILIKE '%looker query context%'
GROUP BY 1, 2, 3
ORDER BY 1 DESC