-- author: johndarrah
-- description: messaging SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS

-- Notes
-- to identify touches coming in when there is a backlgo: mt.backlog_handled
-- -- must be handled
-- -- touch start time != assignment time or customer contact was not during business hours
-- app_cash_cs.public.live_agent_chat_escalations in UT but we don't have enough data to parse them out

-- For handle time and touches, we're switching to Universal Touches
-- -- I've found a few issues with app_cash_cs.preprod.messaging_touches and DMC has voiced to me that they plan on only fixing UT moving forward
-- The volume and SLA's will be when the touch occurred, not when it was assigned
-- -- the assignment date is based on when the case goes from the queue to the advocate

WITH
  entering_message_touches AS (
    SELECT
      ut.touch_start_time::DATE           AS entering_date_pt
      , ecd.employee_id
      , ecd.full_name
      , ecd.city
      , tqc.team_name                     AS vertical
      , tqc.communication_channel         AS channel
      , tqc.business_unit_name
      , COUNT(DISTINCT ut.cfone_touch_id) AS entering_touches
    FROM app_datamart_cco.public.universal_touches ut
    LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
      ON ut.advocate_id = ecd.cfone_id_today
      AND ut.touch_start_time::DATE BETWEEN ecd.start_date AND ecd.end_date
    LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc -- feedback add this to ut
      ON LOWER(ut.queue_name) = LOWER(tqc.queue_name)
    LEFT JOIN app_cash_cs.public.live_agent_chat_escalations lace
      ON ut.case_id = lace.parent_case_id
      AND lace.chat_record_type IN ('RD Chat', 'Internal Advocate Success')
    WHERE
      YEAR(ut.touch_start_time) >= '2022' --note that some chats may be resolved without interaction
      AND NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
      AND lace.parent_case_id IS NULL     -- exclude live agent for RD and AST since there's a CTE
      AND ecd.employee_id = '40706'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  , handled_messaging_touches AS (
  SELECT
    ut.touch_start_time::DATE                                             AS handled_date_pt
    , ut.advocate_id
    , ecd.employee_id
    , ecd.full_name
    , ecd.city
    , tqc.team_name                                                       AS vertical
    , tqc.communication_channel                                           AS channel
    , tqc.business_unit_name
    , COUNT(DISTINCT ut.cfone_touch_id)                                   AS handled_touches
    , SUM(IFF(ut.in_business_hours, ut.response_time / 60, NULL))         AS response_time_min
    , SUM(ut.handle_time) / 60                                            AS handle_time_min
    , SUM(DATEDIFF(MINUTES, ut.touch_assignment_time, ut.touch_end_time)) AS touch_lifetime_min
    , touch_lifetime_min / handle_time_min                                AS concurrency
    , COUNT(DISTINCT
            CASE
              WHEN ut.touch_assignment_time::DATE != ut.touch_start_time::DATE
                OR NOT ut.in_business_hours
                THEN ut.cfone_touch_id
            END)                                                          AS handled_backlog_touches
    , COUNT(DISTINCT
            CASE
              WHEN (ut.response_time / 60) <= 7
                AND ut.in_business_hours = TRUE
                THEN ut.cfone_touch_id
              ELSE NULL
            END)                                                          AS touches_in_sla
    , COUNT(DISTINCT IFF(ut.in_business_hours, ut.cfone_touch_id, NULL))  AS qualified_sla_touches
    , touches_in_sla / NULLIFZERO(qualified_sla_touches) * 100            AS percent_touches_in_sla
  FROM app_datamart_cco.public.universal_touches ut
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON ut.advocate_id = ecd.cfone_id_today
    AND ut.touch_assignment_time::DATE BETWEEN ecd.start_date AND ecd.end_date
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(ut.queue_name) = LOWER(tqc.queue_name)
  LEFT JOIN app_cash_cs.public.live_agent_chat_escalations lace
    ON ut.case_id = lace.parent_case_id
    AND lace.chat_record_type IN ('RD Chat', 'Internal Advocate Success')
  WHERE
    1 = 1
    AND NVL(LOWER(tqc.business_unit_name), 'other') -- expand editor window
    IN ('customer success - specialty', 'customer success - core', 'other')
    AND ecd.employee_id = '40706'
    AND lace.parent_case_id IS NULL -- exclude live agent
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
  , messaging_touches_final AS (
  SELECT
    e.entering_date_pt
    , h.employee_id
    , h.full_name
    , h.city
    , COALESCE(h.vertical, e.vertical)                     AS vertical
    , COALESCE(h.channel, e.channel)                       AS channel
    , COALESCE(h.business_unit_name, e.business_unit_name) AS business_unit_name
    , e.entering_touches
    , h.handled_touches
    , h.handled_backlog_touches
    , h.touches_in_sla
    , h.qualified_sla_touches
    , h.percent_touches_in_sla
    , h.response_time_min
    , h.handle_time_min
    , h.touch_lifetime_min
    , h.concurrency
  FROM entering_message_touches e
  LEFT JOIN handled_messaging_touches h
    ON e.entering_date_pt = h.handled_date_pt
    AND e.employee_id = h.employee_id
    AND e.vertical = h.vertical

  ORDER BY entering_date_pt DESC
)
  , entering_rd_ast_touches AS (
  SELECT
    chat_created_at::DATE                            AS entering_date_pt
    , chat_advocate_employee_id                      AS employee_id
    , chat_advocate                                  AS full_name
    , chat_advocate_city                             AS city
    , IFF(chat_record_type = 'RD Chat', 'RD', 'AST') AS vertical
    , 'CHAT'                                         AS channel
    , 'CUSTOMER SUCCESS - CORE'                      AS business_unit_name
    , COUNT(DISTINCT chat_transcript_id)             AS entering_touches
  FROM app_cash_cs.public.live_agent_chat_escalations
  WHERE
    YEAR(chat_created_at) >= '2022'
    AND chat_record_type IN ('RD Chat', 'Internal Advocate Success')
  GROUP BY 1, 2, 3, 4, 5, 6
)
  , handled_rd_ast_touches AS (
  SELECT
    chat_start_time::DATE                            AS handle_date_pt
    , chat_advocate_employee_id                      AS employee_id
    , chat_advocate                                  AS full_name
    , chat_advocate_city                             AS city
    , IFF(chat_record_type = 'RD Chat', 'RD', 'AST') AS vertical
    , 'CHAT'                                         AS channel
    , 'CUSTOMER SUCCESS - CORE'                      AS business_unit_name
    , COUNT(DISTINCT chat_transcript_id)             AS handled_touches
    , NULL                                           AS handled_backlog_touches
    , SUM(chat_handle_time / 60)                     AS handle_time_min
    , SUM(chat_handle_time / 60)                     AS touch_lifetime_min
    , touch_lifetime_min / handle_time_min           AS concurrency
    , NULL                                           AS response_time_min
    , COUNT(DISTINCT
            CASE
              WHEN chat_record_type = 'RD Chat'
                AND chat_wait_time <= 60
                AND chat_handle_time > 0
                THEN chat_transcript_id
              WHEN chat_record_type = 'Internal Advocate Success'
                AND chat_wait_time <= 180
                AND chat_handle_time > 0
                THEN chat_transcript_id
              ELSE NULL
            END)                                     AS touches_in_sla
    , COUNT(DISTINCT
            CASE
              WHEN chat_record_type = 'RD Chat'
                AND chat_wait_time <= 60
                AND chat_handle_time = 0
                THEN chat_transcript_id
              WHEN chat_record_type = 'Internal Advocate Success'
                AND chat_wait_time <= 180
                AND chat_handle_time = 0
                THEN chat_transcript_id
              ELSE NULL
            END)                                     AS abandoned_touches
    , handled_touches - abandoned_touches            AS qualified_sla_touches
    , touches_in_sla / qualified_sla_touches * 100   AS percent_touches_in_sla
  FROM app_cash_cs.public.live_agent_chat_escalations
  WHERE
    YEAR(chat_created_at) >= '2022'
    AND chat_record_type IN ('RD Chat', 'Internal Advocate Success')
  GROUP BY 1, 2, 3, 4, 5, 6
)
  , ast_rd_touches_final AS (
  SELECT
    e.entering_date_pt                                     AS entering_date_pt
    , h.employee_id
    , h.full_name
    , h.city
    , COALESCE(h.vertical, e.vertical)                     AS vertical
    , COALESCE(h.channel, e.channel)                       AS channel
    , COALESCE(h.business_unit_name, e.business_unit_name) AS business_unit_name
    , e.entering_touches
    , h.handled_touches
    , h.handled_backlog_touches
    , h.touches_in_sla
    , h.qualified_sla_touches
    , h.percent_touches_in_sla
    , h.response_time_min
    , h.handle_time_min
    , h.touch_lifetime_min
    , h.concurrency
  FROM entering_rd_ast_touches AS e
  LEFT JOIN handled_rd_ast_touches AS h
    ON e.entering_date_pt = h.handle_date_pt
    AND e.vertical = h.vertical
    AND e.employee_id = h.employee_id
  WHERE
    e.employee_id = '19805'
  ORDER BY entering_date_pt DESC
)

SELECT *
FROM messaging_touches_final

UNION

SELECT *
FROM ast_rd_touches_final
