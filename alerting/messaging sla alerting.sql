-- author: johndarrah
-- description: messaging SLA metrics for alerting
-- Notes
-- business_unit is derived from app_datamart_cco.public.team_queue_catalog
-- to identify touches coming in when there is a backlgo: mt.backlog_handled
-- -- must be handled
-- -- touch start time != assignment time
-- -- customer contact was not during business hours
-- app_cash_cs.public.live_agent_chat_escalations is this in UT

WITH
  entering_message_touches AS (
    SELECT
      mt.touch_start_time::DATE           AS entering_date_pt
      , ecd.employee_id
      , ecd.full_name
      , ecd.city
      , mt.team_name                      AS vertical
      , mt.communication_channel          AS channel
      , mt.business_unit_name
      , COUNT(DISTINCT mt.touch_start_id) AS entering_touches
    FROM app_cash_cs.preprod.messaging_touches mt
    LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
      ON mt.employee_id = ecd.employee_id
      AND mt.touch_start_time::DATE BETWEEN ecd.start_date AND ecd.end_date
    WHERE
      YEAR(mt.touch_start_time) >= '2022' --note that some chats may be resolved without interaction
      AND mt.business_unit_name IN ('CUSTOMER SUCCESS - SPECIALTY', 'CUSTOMER SUCCESS - CORE')

      AND ecd.employee_id = '40706'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  , handled_messaging_touches AS (
  SELECT
    mt.touch_assignment_time::DATE                                        AS handled_date_pt
    , ecd.employee_id
    , ecd.full_name
    , ecd.city
    , team_name                                                           AS vertical
    , mt.communication_channel                                            AS channel
    , mt.business_unit_name
    , COUNT(DISTINCT mt.touch_id)                                         AS handled_touches
    , COUNT(DISTINCT
            CASE
              WHEN (mt.response_time_seconds / 60) <= 7
                AND mt.in_business_hours = TRUE
                THEN mt.touch_id
              ELSE NULL
            END)                                                          AS touches_in_sla
    , COUNT(DISTINCT IFF(mt.in_business_hours, mt.touch_id, NULL))        AS qualified_sla_touches
    , touches_in_sla / qualified_sla_touches * 100                        AS percent_touches_in_sla
    , SUM(IFF(mt.in_business_hours, mt.response_time_seconds / 60, NULL)) AS response_time_min
    , SUM(handle_time_seconds) / 60                                       AS handle_time_min
  FROM app_cash_cs.preprod.messaging_touches mt
  JOIN app_cash_cs.public.employee_cash_dim ecd
    ON mt.employee_id = ecd.employee_id
    AND mt.touch_assignment_time::DATE BETWEEN ecd.start_date AND ecd.end_date

  WHERE
    1 = 1
    AND ecd.employee_id = '40706'

  GROUP BY 1, 2, 3, 4, 5, 6, 7
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
    , h.touches_in_sla
    , h.qualified_sla_touches
    , h.percent_touches_in_sla
    , h.response_time_min
    , h.handle_time_min
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
    chat_record_type IN ('RD Chat', 'Internal Advocate Success')
    AND YEAR(chat_created_at) >= '2022'
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
    , SUM(chat_handle_time / 60)                     AS handle_time_min
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
    chat_record_type IN ('RD Chat', 'Internal Advocate Success')
    AND YEAR(chat_created_at) >= '2022'
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
    , h.touches_in_sla
    , h.qualified_sla_touches
    , h.percent_touches_in_sla
    , NULL                                                 AS response_time_min
    , h.handle_time_min
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
