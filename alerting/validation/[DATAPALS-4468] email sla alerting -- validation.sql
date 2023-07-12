-- author: johndarrah
-- description: email SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS
-- queue_day_touches_agg = https://block.sourcegraph.com/github.com/squareup/app-cash-cs/-/blob/datapals_ETLS/Email%20Daily%20Agg%20ETL.sql

-- Notes


WITH
  hour_ts AS (
    SELECT DISTINCT
      interval_start_time                                                  AS hour_interval
      , TO_CHAR(DATE_TRUNC(HOURS, hour_interval), 'YYYY-MM-DD HH24:MI:SS') AS ts
    FROM app_cash_cs.public.dim_date_time
    WHERE
      YEAR(report_date) = 2022
      AND report_date <= CURRENT_DATE
      AND EXTRACT(MINUTE FROM interval_start_time) = 0
  )
  , entering_email_touches AS (
  SELECT
    COUNT(DISTINCT et.touch_id) AS entering_touches
  FROM hour_ts ht
  LEFT JOIN app_cash_cs.public.email_touches et
    ON ht.hour_interval = DATE_TRUNC(HOURS, et.touch_start_time)
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(et.queue_name) = LOWER(tqc.queue_name)
  WHERE
    NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
)
  , handled_email_touches AS (
  SELECT
    COUNT(DISTINCT et.touch_id)                   AS handled_touches
    , SUM(et.response_time_minutes)               AS response_time_min
    , AVG(response_time_minutes)                  AS avg_response_time_min
    , SUM(et.touch_handle_time) / 60              AS handle_time_min
    , AVG(et.touch_handle_time) / 60              AS avg_handle_time_min
    , COUNT(DISTINCT
            CASE
              WHEN et.response_time_minutes <= 24
                AND touch_type != 'TRN'
                THEN et.touch_id
              ELSE NULL
            END)                                  AS touches_in_sl
    , touches_in_sl / NULLIFZERO(handled_touches) AS sl_percent
  FROM hour_ts ht
  LEFT JOIN app_cash_cs.public.email_touches et
    ON ht.hour_interval = DATE_TRUNC(HOURS, et.touch_time)
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(et.queue_name) = LOWER(tqc.queue_name)
  WHERE
    NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
)

SELECT
  e.entering_touches
  , h.handled_touches
  , h.touches_in_sl
  , h.sl_percent
  , h.response_time_min
  , h.avg_response_time_min
  , h.handle_time_min
  , h.avg_handle_time_min
FROM entering_email_touches e
LEFT JOIN handled_email_touches h