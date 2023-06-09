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
      TO_CHAR(DATE_TRUNC(HOURS, ut.touch_assignment_time), 'YYYY-MM-DD HH24:MI:SS') AS entering_hour
      , ecd.employee_id
      , ecd.full_name
      , ecd.city
      , tqc.team_name                                                               AS vertical
      , tqc.communication_channel                                                   AS channel
      , tqc.business_unit_name
      , COUNT(DISTINCT ut.cfone_touch_id)                                           AS entering_touches
    FROM app_datamart_cco.public.universal_touches ut
    LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
      ON ut.advocate_id = ecd.cfone_id_today
      AND ut.touch_start_time::DATE BETWEEN ecd.start_date AND ecd.end_date
    LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
      ON LOWER(ut.queue_name) = LOWER(tqc.queue_name)
    WHERE
      YEAR(ut.touch_start_time) >= '2022' --note that some chats may be resolved without interaction
      AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
      AND ut.channel = 'Email'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  , handled_email_touches AS (
  SELECT
    TO_CHAR(DATE_TRUNC(HOURS, ut.touch_start_time), 'YYYY-MM-DD HH24:MI:SS') AS handled_hour
    , ut.advocate_id
    , ecd.employee_id
    , ecd.full_name
    , ecd.city
    , tqc.team_name                                                          AS vertical
    , tqc.communication_channel                                              AS channel
    , tqc.business_unit_name
    , COUNT(DISTINCT ut.cfone_touch_id)                                      AS handled_touches
    , SUM(IFF(ut.in_business_hours, ut.response_time / 60, NULL))            AS response_time_min -- do we only care about response times in bh
    , SUM(ut.handle_time) / 60                                               AS handle_time_min
    , SUM(DATEDIFF(MINUTES, ut.touch_assignment_time, ut.touch_end_time))    AS touch_lifetime_min
    , touch_lifetime_min / handle_time_min                                   AS concurrency
    , COUNT(DISTINCT
            CASE
              WHEN ut.touch_assignment_time::DATE != ut.touch_start_time::DATE
                OR NOT ut.in_business_hours
                THEN ut.cfone_touch_id
            END)                                                             AS handled_backlog_touches
    , COUNT(DISTINCT
            CASE
              WHEN ut.response_time / 60 <= 24
                AND LOWER(ut.inbound_type) != 'transfer' -- need to translate from here: https://github.com/squareup/app-cash-cs/blob/main/datapals_ETLS/Email%20Daily%20Agg%20ETL.sql#L93
                THEN ut.cfone_touch_id
              ELSE NULL
            END)                                                             AS touches_in_sla
    , COUNT(DISTINCT
            CASE
              WHEN ut.inbound_type IN ('EML', 'EML/TRN', 'TR') -- need to translate what from here: https://github.com/squareup/app-cash-cs/blob/main/datapals_ETLS/Email%20Daily%20Agg%20ETL.sql#L90
                THEN ut.cfone_touch_id
              ELSE NULL
            END)                                                             AS response_handled
    , touches_in_sla / NULLIFZERO(response_handled) * 100                    AS percent_touches_in_sla
  FROM app_datamart_cco.public.universal_touches ut
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON ut.advocate_id = ecd.cfone_id_today
    AND ut.touch_assignment_time::DATE BETWEEN ecd.start_date AND ecd.end_date
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(ut.queue_name) = LOWER(tqc.queue_name)
  WHERE
    1 = 1
    AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
    AND ut.channel = 'Email'
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
  , h.handled_backlog_touches
  , h.touches_in_sla
  , h.response_handled
  , h.percent_touches_in_sla
  , h.response_time_min
  , h.handle_time_min
  , h.touch_lifetime_min
  , h.concurrency
FROM entering_email_touches e
LEFT JOIN handled_email_touches h
  ON e.entering_hour = h.handled_hour
  AND e.employee_id = h.employee_id
  AND e.vertical = h.vertical
--   AND e.employee_id = '44222'
;

SELECT *
FROM app_cash_cs.public.email_touches