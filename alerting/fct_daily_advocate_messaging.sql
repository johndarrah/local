-- author: johndarrah
-- description: messaging SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS

-- Notes
-- timestamps in UTC
-- backlog must be handled and touch start time != assignment time or customer contact was not during business hours
-- concurrency: touch lifetime / handle time
-- entering date = when the touch was created/assigned
-- handle date = when the touch was started

WITH
  dt AS (
    SELECT DISTINCT
      report_date AS dt
    FROM app_cash_cs.public.dim_date_time
    WHERE
      YEAR(report_date) >= 2018
      AND report_date <= CURRENT_DATE
  )
  , entering_message_touches AS (
  SELECT
    d.dt
    , tqc.team_name                     AS vertical
    , tqc.communication_channel         AS channel
    , tqc.business_unit_name
    , COUNT(DISTINCT mt.touch_start_id) AS entering_touches
  FROM dt d
  LEFT JOIN app_cash_cs.preprod.messaging_touches mt
    ON d.dt = CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_start_time)::DATE
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(mt.queue_name) = LOWER(tqc.queue_name)
  WHERE
    NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
  GROUP BY 1, 2, 3, 4
)
  , handled_messaging_touches AS (
  SELECT
    d.dt
    , ecd.employee_id
    , ecd.full_name                                                       AS advocate_name
    , ecd.city                                                            AS advocate_city
    , tqc.team_name                                                       AS vertical
    , tqc.communication_channel                                           AS channel
    , tqc.business_unit_name
    , COUNT(DISTINCT mt.touch_id)                                         AS handled_touches
    , SUM(mt.response_time_seconds) / 60                                  AS response_time_min
    , AVG(mt.response_time_seconds) / 60                                  AS avg_response_time_min
    , SUM(mt.handle_time_seconds) / 60                                    AS handle_time_min
    , AVG(mt.handle_time_seconds) / 60                                    AS avg_handle_time_min
    , SUM(DATEDIFF(MINUTES, mt.touch_assignment_time, mt.touch_end_time)) AS touch_lifetime_min
    , touch_lifetime_min / handle_time_min                                AS concurrency
    , COUNT(DISTINCT
            CASE
              WHEN mt.touch_assignment_time::DATE != mt.touch_start_time::DATE
                OR NOT mt.in_business_hours
                THEN mt.touch_id
            END)                                                          AS handled_backlog_touches
    , COUNT(DISTINCT
            CASE
              WHEN (mt.response_time_seconds / 60) <= 7
                AND mt.in_business_hours
                THEN mt.touch_id
              ELSE NULL
            END)                                                          AS touches_in_sl
    , COUNT(DISTINCT IFF(mt.in_business_hours, mt.touch_id, NULL))        AS qualified_sla_touches
    , touches_in_sl / NULLIFZERO(qualified_sla_touches) * 100             AS sl_percent
  FROM dt d
  LEFT JOIN app_cash_cs.preprod.messaging_touches mt
    ON d.dt = CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_assignment_time)::DATE
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(mt.queue_name) = LOWER(tqc.queue_name)
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON mt.employee_id = ecd.employee_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_assignment_time)::DATE BETWEEN ecd.start_date AND ecd.end_date
  WHERE
    NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
  , entering_rd_ast_touches AS (
  SELECT
    d.dt
    , IFF(e.chat_record_type = 'RD Chat', 'RD', 'AST') AS vertical
    , 'CHAT'                                           AS channel
    , 'CUSTOMER SUCCESS - CORE'                        AS business_unit_name
    , COUNT(DISTINCT e.chat_transcript_id)             AS entering_touches
  FROM dt d
  LEFT JOIN app_cash_cs.public.live_agent_chat_escalations e
    ON d.dt = CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', e.chat_created_at)::DATE
  WHERE
    chat_record_type IN ('RD Chat', 'Internal Advocate Success')
  GROUP BY 1, 2, 3, 4
)
  , handled_rd_ast_touches AS (
  SELECT
    d.dt
    , e.chat_advocate_employee_id                      AS employee_id
    , e.chat_advocate                                  AS advocate_name
    , e.chat_advocate_city                             AS advocate_city
    , IFF(e.chat_record_type = 'RD Chat', 'RD', 'AST') AS vertical
    , 'CHAT'                                           AS channel
    , 'CUSTOMER SUCCESS - CORE'                        AS business_unit_name
    , COUNT(DISTINCT e.chat_transcript_id)             AS handled_touches
    , NULL                                             AS handled_backlog_touches
    , SUM(e.chat_handle_time / 60)                     AS handle_time_min
    , AVG(e.chat_handle_time) / 60                     AS avg_handle_time_min
    , SUM(e.chat_handle_time / 60)                     AS touch_lifetime_min
    , touch_lifetime_min / handle_time_min             AS concurrency
    , NULL                                             AS response_time_min
    , NULL                                             AS avg_response_time_min
    , COUNT(DISTINCT
            CASE
              WHEN e.chat_record_type = 'RD Chat'
                AND e.chat_wait_time <= 60
                AND e.chat_handle_time > 0
                THEN e.chat_transcript_id
              WHEN e.chat_record_type = 'Internal Advocate Success'
                AND e.chat_wait_time <= 180
                AND e.chat_handle_time > 0
                THEN e.chat_transcript_id
              ELSE NULL
            END)                                       AS touches_in_sl
    , COUNT(DISTINCT
            CASE
              WHEN e.chat_record_type = 'RD Chat'
                AND e.chat_wait_time <= 60
                AND e.chat_handle_time = 0
                THEN e.chat_transcript_id
              WHEN e.chat_record_type = 'Internal Advocate Success'
                AND e.chat_wait_time <= 180
                AND e.chat_handle_time = 0
                THEN e.chat_transcript_id
              ELSE NULL
            END)                                       AS abandoned_touches
    , handled_touches - abandoned_touches              AS qualified_sl_touches
    , touches_in_sl / qualified_sl_touches * 100       AS sl_percent
  FROM dt d
  LEFT JOIN app_cash_cs.public.live_agent_chat_escalations e
    ON d.dt = CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', e.chat_start_time)::DATE
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON e.chat_advocate_employee_id = ecd.employee_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', e.chat_start_time)::DATE BETWEEN ecd.start_date AND ecd.end_date
  WHERE
    chat_record_type IN ('RD Chat', 'Internal Advocate Success')
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)

  -- messaging touches
SELECT
  e.dt
  , h.employee_id
  , h.advocate_name
  , h.advocate_city
  , e.vertical
  , e.channel
  , e.business_unit_name
  , e.entering_touches
  , h.handled_touches
  , h.handled_backlog_touches
  , h.touches_in_sl
  , h.qualified_sla_touches
  , h.sl_percent
  , h.response_time_min
  , h.avg_response_time_min
  , h.handle_time_min
  , h.avg_handle_time_min
  , h.touch_lifetime_min
  , h.concurrency
FROM entering_message_touches e
LEFT JOIN handled_messaging_touches h
  ON e.dt = h.dt
  AND e.vertical = h.vertical
WHERE
  1 = 1
  AND e.dt = '2023-07-06 20:00:00'

UNION

--   AST and RD touches
SELECT
  e.dt
  , h.employee_id
  , h.advocate_name
  , h.advocate_city
  , e.vertical
  , e.channel
  , e.business_unit_name
  , e.entering_touches
  , h.handled_touches
  , h.handled_backlog_touches
  , h.touches_in_sl
  , h.qualified_sl_touches
  , h.sl_percent
  , h.response_time_min
  , h.avg_response_time_min
  , h.handle_time_min
  , h.avg_handle_time_min
  , h.touch_lifetime_min
  , h.concurrency
FROM entering_rd_ast_touches AS e
LEFT JOIN handled_rd_ast_touches AS h
  ON e.dt = h.dt
  AND e.vertical = h.vertical
and e.advocate_id
WHERE
  1 = 1
  AND e.dt = '2023-07-06 20:00:00'
ORDER BY 1 DESC