-- author: johndarrah
-- description: email SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS
-- queue_day_touches_agg = https://block.sourcegraph.com/github.com/squareup/app-cash-cs/-/blob/datapals_ETLS/Email%20Daily%20Agg%20ETL.sql

-- Notes


WITH
  entering_email_touches AS (
    SELECT
      TO_CHAR(DATE_TRUNC(HOURS, et.touch_start_time), 'YYYY-MM-DD HH24:MI:SS') AS entering_hour
      , ecd.employee_id
      , ecd.full_name
      , ecd.city
      , tqc.team_name                                                          AS vertical
      , tqc.communication_channel                                              AS channel
      , tqc.business_unit_name
      , COUNT(DISTINCT et.touch_id)                                            AS entering_touches
    FROM app_cash_cs.public.email_touches et
    LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
      ON et.advocate_id = ecd.cfone_id_today
      AND et.touch_start_time::DATE BETWEEN ecd.start_date AND ecd.end_date
    LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
      ON LOWER(et.queue_name) = LOWER(tqc.queue_name)
    WHERE
      YEAR(et.touch_start_time) >= '2022' --note that some chats may be resolved without interaction
      AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  , handled_email_touches AS (
  SELECT
    TO_CHAR(DATE_TRUNC(HOURS, et.touch_time), 'YYYY-MM-DD HH24:MI:SS') AS handled_hour
    , et.advocate_id
    , ecd.employee_id
    , ecd.full_name
    , ecd.city
    , tqc.team_name                                                    AS vertical
    , tqc.communication_channel                                        AS channel
    , tqc.business_unit_name
    , COUNT(DISTINCT et.touch_id)                                      AS handled_touches
    , SUM(et.response_time_minutes)                                    AS response_time_min
    , AVG(response_time_minutes)                                       AS avg_response_time_min
    , SUM(et.touch_handle_time) / 60                                   AS handle_time_min
    , AVG(et.touch_handle_time) / 60                                   AS avg_handle_time_min
    , COUNT(DISTINCT
            CASE
              WHEN et.response_time_minutes <= 24
                AND touch_type != 'TRN' -- need to translate from here: https://github.com/squareup/app-cash-cs/blob/main/datapals_ETLS/Email%20Daily%20Agg%20ETL.sql#L93
                THEN et.touch_id
              ELSE NULL
            END)                                                       AS touches_in_sl
    , touches_in_sl / NULLIFZERO(handled_touches)                      AS sl_percent
  FROM app_cash_cs.public.email_touches et
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON et.advocate_id = ecd.cfone_id_today
    AND et.touch_time ::DATE BETWEEN ecd.start_date AND ecd.end_date
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(et.queue_name) = LOWER(tqc.queue_name)
  WHERE
    1 = 1
    AND YEAR(et.touch_time) >= '2022'
    AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT
  e.entering_hour
  , e.employee_id
  , e.full_name
  , e.city
  , e.vertical
  , e.channel
  , e.business_unit_name
  , e.entering_touches
  , h.handled_touches
  , h.touches_in_sl
  , h.sl_percent
  , h.response_time_min
  , h.avg_response_time_min
  , h.handle_time_min
  , h.avg_handle_time_min
FROM entering_email_touches e
LEFT JOIN handled_email_touches h
  ON e.entering_hour = h.handled_hour
  AND e.employee_id = h.employee_id
  AND e.vertical = h.vertical
WHERE
  1 = 1
  AND e.employee_id = '44222'
;