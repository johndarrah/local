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

-- app_cash_cs.public.live_agent_chat_escalations in UT but we don't have enough data to parse them out

-- For handle time and touches, we're switching to Universal Touches
-- -- I've found a few issues with app_cash_cs.preprod.messaging_touches and DMC has voiced to me that they plan on only fixing UT moving forward
-- The volume and SLA's will be when the touch occurred, not when it was assigned
-- -- the assignment date is based on when the case goes from the queue to the advocate

WITH
  entering_message_touches AS (
    SELECT
      TO_CHAR(
        DATE_TRUNC(HOURS,
                   CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_assignment_time)
          ),
        'YYYY-MM-DD HH24:MI:SS')    AS entering_hour
      , ecd.employee_id
      , ecd.full_name
      , ecd.city
      , tqc.team_name               AS vertical
      , tqc.communication_channel   AS channel
      , tqc.business_unit_name
      , COUNT(DISTINCT mt.touch_id) AS entering_touches
    FROM app_cash_cs.preprod.messaging_touches mt
    LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
      ON mt.advocate_id = ecd.cfone_id_today
      AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_assignment_time)::DATE
        BETWEEN ecd.start_date AND ecd.end_date
    LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
      ON LOWER(mt.queue_name) = LOWER(tqc.queue_name)
      --     LEFT JOIN app_cash_cs.public.live_agent_chat_escalations lace
      --       ON mt.case_id = lace.parent_case_id
      --       AND lace.chat_record_type IN ('RD Chat', 'Internal Advocate Success')
    WHERE
      YEAR(mt.touch_assignment_time) >= '2022' --note that some chats may be resolved without interaction
      AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  --   , handled_messaging_touches AS (
SELECT
  1 = 1
  --   TO_CHAR(
  --     DATE_TRUNC(HOURS,
  --                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_start_time)
  --       ),
  --     'YYYY-MM-DD HH24:MI:SS')                                            AS handled_hour
  --     , mt.advocate_id
  --     , ecd.employee_id
  --     , ecd.full_name
  --     , ecd.city
  --     , tqc.team_name                                                          AS vertical
  --     , tqc.communication_channel                                              AS channel
  --     , tqc.business_unit_name
  , COUNT(DISTINCT mt.touch_id)                                         AS handled_touches
  , SUM(mt.response_time_seconds) / 60                                  AS response_time_min
  , AVG(mt.response_time_seconds) / 60                                  AS avg_response_time_min
  , response_time_min / handled_touches                                 AS der_avg_response_time_min

  , SUM(mt.handle_time_seconds) / 60                                    AS handle_time_min
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
              AND mt.in_business_hours = TRUE
              THEN mt.touch_id
            ELSE NULL
          END)                                                          AS touches_in_sla
  , COUNT(DISTINCT IFF(mt.in_business_hours, mt.touch_id, NULL))        AS qualified_sla_touches
  , touches_in_sla / NULLIFZERO(qualified_sla_touches) * 100            AS percent_touches_in_sla -- to rename to sl_percent
FROM app_cash_cs.preprod.messaging_touches mt
LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
  ON mt.advocate_id = ecd.cfone_id_today
  AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_start_time)::DATE
    BETWEEN ecd.start_date AND ecd.end_date
LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
  ON LOWER(mt.queue_name) = LOWER(tqc.queue_name)
  --   LEFT JOIN app_cash_cs.public.live_agent_chat_escalations lace
  --     ON mt.case_id = lace.parent_case_id
  --     AND lace.chat_record_type IN ('RD Chat', 'Internal Advocate Success')
WHERE
  1 = 1
  AND YEAR(mt.touch_start_time) >= '2022'
  AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
--     AND lace.parent_case_id IS NULL -- exclude live agent
--   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- )
--   , entering_rd_ast_touches AS (
--   SELECT
--     TO_CHAR(
--       DATE_TRUNC(HOURS,
--                  CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', chat_created_at)
--         ),
--       'YYYY-MM-DD HH24:MI:SS')                       AS entering_hour
--     , chat_advocate_employee_id                      AS employee_id
--     , chat_advocate                                  AS full_name
--     , chat_advocate_city                             AS city
--     , IFF(chat_record_type = 'RD Chat', 'RD', 'AST') AS vertical
--     , 'CHAT'                                         AS channel
--     , 'CUSTOMER SUCCESS - CORE'                      AS business_unit_name
--     , COUNT(DISTINCT chat_transcript_id)             AS entering_touches
--   FROM app_cash_cs.public.live_agent_chat_escalations
--   WHERE
--     YEAR(chat_created_at) >= '2022'
--     AND chat_record_type IN ('RD Chat', 'Internal Advocate Success')
--   GROUP BY 1, 2, 3, 4, 5, 6
-- )
--   , handled_rd_ast_touches AS (
--   SELECT
--     TO_CHAR(
--       DATE_TRUNC(HOURS,
--                  CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', chat_start_time)
--         ),
--       'YYYY-MM-DD HH24:MI:SS')                       AS handled_hour
--     , chat_advocate_employee_id                      AS employee_id
--     , chat_advocate                                  AS full_name
--     , chat_advocate_city                             AS city
--     , IFF(chat_record_type = 'RD Chat', 'RD', 'AST') AS vertical
--     , 'CHAT'                                         AS channel
--     , 'CUSTOMER SUCCESS - CORE'                      AS business_unit_name
--     , COUNT(DISTINCT chat_transcript_id)             AS handled_touches
--     , NULL                                           AS handled_backlog_touches
--     , SUM(chat_handle_time / 60)                     AS handle_time_min
--     , SUM(chat_handle_time / 60)                     AS touch_lifetime_min
--     , touch_lifetime_min / handle_time_min           AS concurrency
--     , NULL                                           AS response_time_min
--     , COUNT(DISTINCT
--             CASE
--               WHEN chat_record_type = 'RD Chat'
--                 AND chat_wait_time <= 60
--                 AND chat_handle_time > 0
--                 THEN chat_transcript_id
--               WHEN chat_record_type = 'Internal Advocate Success'
--                 AND chat_wait_time <= 180
--                 AND chat_handle_time > 0
--                 THEN chat_transcript_id
--               ELSE NULL
--             END)                                     AS touches_in_sla
--     , COUNT(DISTINCT
--             CASE
--               WHEN chat_record_type = 'RD Chat'
--                 AND chat_wait_time <= 60
--                 AND chat_handle_time = 0
--                 THEN chat_transcript_id
--               WHEN chat_record_type = 'Internal Advocate Success'
--                 AND chat_wait_time <= 180
--                 AND chat_handle_time = 0
--                 THEN chat_transcript_id
--               ELSE NULL
--             END)                                     AS abandoned_touches
--     , handled_touches - abandoned_touches            AS qualified_sla_touches
--     , touches_in_sla /
--     qualified_sla_touches * 100                      AS percent_touches_in_sla
--   FROM app_cash_cs.public.live_agent_chat_escalations
--   WHERE
--     YEAR(chat_created_at) >= '2022'
--     AND chat_record_type IN ('RD Chat', 'Internal Advocate Success')
--   GROUP BY 1, 2, 3, 4, 5, 6
-- )
--
--   -- messaging touches
-- SELECT
--   e.entering_hour
--   , e.employee_id
--   , e.full_name
--   , e.city
--   , e.vertical
--   , e.channel
--   , e.business_unit_name
--   , e.entering_touches
--   , h.handled_touches
--   , h.handled_backlog_touches
--   , h.touches_in_sla
--   , h.qualified_sla_touches
--   , h.percent_touches_in_sla
--   , h.response_time_min
--   , h.handle_time_min
--   , h.touch_lifetime_min
--   , h.concurrency
-- FROM entering_message_touches e
-- LEFT JOIN handled_messaging_touches h
--   ON e.entering_hour = h.handled_hour
--   AND e.employee_id = h.employee_id
--   AND e.vertical = h.vertical
--   AND e.employee_id = '40706'
--
-- UNION
--
-- -- AST and RD touches
-- SELECT
--   e.entering_hour
--   , e.employee_id
--   , e.full_name
--   , e.city
--   , e.vertical
--   , e.channel
--   , e.business_unit_name
--   , e.entering_touches
--   , h.handled_touches
--   , h.handled_backlog_touches
--   , h.touches_in_sla
--   , h.qualified_sla_touches
--   , h.percent_touches_in_sla
--   , h.response_time_min
--   , h.handle_time_min
--   , h.touch_lifetime_min
--   , h.concurrency
-- FROM entering_rd_ast_touches AS e
-- LEFT JOIN handled_rd_ast_touches AS h
--   ON e.entering_hour = h.handled_hour
--   AND e.vertical = h.vertical
--   AND e.employee_id = h.employee_id
-- WHERE
--   e.employee_id = '19805'
-- ORDER BY 1 DESC


-- -- QA

-- live chats without a start time
-- SELECT
--   chat_advocate_employee_id AS employee_id
--   , chat_advocate           AS full_name
--   , chat_advocate_city      AS city
--   , chat_record_type
--   , chat_start_time
--   , chat_end_time
--   , *
-- FROM app_cash_cs.public.live_agent_chat_escalations
-- WHERE
--   YEAR(chat_created_at) >= '2022'
--   AND chat_record_type IN ('RD Chat', 'Internal Advocate Success')
--   AND employee_id = '19805'
--   AND chat_start_time IS NULL


;

SELECT *
FROM app_cash_cs.preprod.messaging_touches