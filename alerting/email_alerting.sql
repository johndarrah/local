WITH
  entering_volume AS ( --aggregating data about chat touches
    SELECT
      TO_DATE(mt.touch_start_time)     AS entering_date
      , ecd.employee_id
      , ecd.full_name
      , ecd.city
      , team_name                      AS vertical
      , communication_channel          AS channel
      , business_unit_name
      , COUNT(DISTINCT touch_start_id) AS total_entering
      , COUNT(DISTINCT CASE
                         WHEN backlog_handled
                           THEN touch_start_id
                       END)            AS total_backlog_entering
      , COUNT(DISTINCT case_id)        AS total_cases_entering
    FROM app_cash_cs.preprod.messaging_touches mt
    LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
      ON mt.employee_id = ecd.employee_id
      AND TO_DATE(mt.touch_start_time) BETWEEN ecd.start_date AND ecd.end_date
    WHERE
      TO_DATE(mt.touch_start_time) >= '2022-01-01' --note that some chats may be resolved without interaction by M
      AND business_unit_name IN ('CUSTOMER SUCCESS - SPECIALTY', 'CUSTOMER SUCCESS - CORE')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  , handled_volume AS ( --aggregating data about chat touches
  SELECT
    TO_DATE(mt.touch_assignment_time) AS handled_date
    , ecd.employee_id
    , ecd.full_name
    , ecd.city
    , team_name                       AS vertical
    , communication_channel           AS channel
    , business_unit_name
    , COUNT(DISTINCT touch_id)        AS total_handled
    , COUNT(DISTINCT CASE
                       WHEN backlog_handled
                         THEN touch_id
                     END)             AS total_backlog_handled
    , COUNT(DISTINCT CASE
                       WHEN (response_time_seconds / 60) <= 7
                         AND in_business_hours = TRUE
                         THEN touch_id
                       ELSE NULL
                     END)             AS total_in_sl
    , SUM(CASE
            WHEN in_business_hours = TRUE
              THEN response_time_seconds / 60
            ELSE NULL
          END)                        AS total_response_time_min
    , COUNT(DISTINCT CASE
                       WHEN in_business_hours = TRUE
                         THEN touch_id
                       ELSE NULL
                     END)             AS touches_art
    , SUM(handle_time_seconds) / 60   AS total_handle_time_min
    , COUNT(DISTINCT CASE
                       WHEN in_business_hours = TRUE
                         THEN touch_id
                       ELSE NULL
                     END)             AS total_handled_sla
  FROM app_cash_cs.preprod.messaging_touches mt
  LEFT JOIN app_datamart_cco.sfdc_cfone.dim_queues dq
    ON dq.queue_name = mt.queue_name
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON mt.employee_id = ecd.employee_id
    AND TO_DATE(mt.touch_assignment_time) BETWEEN ecd.start_date AND ecd.end_date
  WHERE
    ecd.employee_id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
  , messaging_final AS
  (
    SELECT
      COALESCE(e.entering_date, h.handled_date)              AS date_pt
      , h.employee_id
      , h.full_name
      , h.city
      , COALESCE(h.vertical, e.vertical)                     AS vertical
      , COALESCE(h.channel, e.channel)                       AS channel
      , COALESCE(h.business_unit_name, e.business_unit_name) AS business_unit_name
      , e.total_entering                                     AS touches_entering
      , h.total_handled                                      AS touches_handled
      , h.total_in_sl                                        AS total_in_sl
      , h.total_response_time_min
      , h.total_handle_time_min
      , h.total_handled_sla                                  AS total_touches_sla
      , h.touches_art
    FROM entering_volume e
    LEFT JOIN handled_volume h
      ON e.entering_date = h.handled_date
      AND e.employee_id = h.employee_id
      AND e.vertical = h.vertical
  )
  , entering_rd_ast_volume AS
  (
    SELECT
      DATE(chat_created_at)                AS entering_date
      , chat_advocate_employee_id          AS employee_id
      , chat_advocate                      AS full_name
      , chat_advocate_city                 AS city
      , CASE
          WHEN chat_record_type = 'RD Chat'
            THEN 'RD'
          ELSE 'AST'
        END                                AS vertical
      , 'CHAT'                             AS channel
      , 'CUSTOMER SUCCESS - CORE'          AS business_unit_name
      , COUNT(DISTINCT chat_transcript_id) AS entering_volume
    FROM app_cash_cs.public.live_agent_chat_escalations
    WHERE
      chat_record_type IN ('RD Chat', 'Internal Advocate Success')
      AND DATE(chat_created_at) >= '2022-01-01'
    GROUP BY 1, 2, 3, 4, 5, 6
  )
  , handled_rd_ast_volume AS
  (
    SELECT
      DATE(chat_start_time)                AS handle_date
      , chat_advocate_employee_id          AS employee_id
      , chat_advocate                      AS full_name
      , chat_advocate_city                 AS city
      , CASE
          WHEN chat_record_type = 'RD Chat'
            THEN 'RD'
          ELSE 'AST'
        END                                AS vertical
      , 'CHAT'                             AS channel
      , 'CUSTOMER SUCCESS - CORE'          AS business_unit_name
      , COUNT(DISTINCT chat_transcript_id) AS total_handled
      , SUM(chat_handle_time / 60)         AS total_handle_time_min
      , COUNT(DISTINCT CASE
                         WHEN chat_record_type = 'RD Chat' AND chat_wait_time <= 60 AND chat_handle_time > 0
                           THEN chat_transcript_id
                         WHEN chat_record_type = 'Internal Advocate Success' AND chat_wait_time <= 180
                           AND chat_handle_time > 0
                           THEN chat_transcript_id
                         ELSE NULL
                       END)                AS total_in_sl
      , COUNT(DISTINCT CASE
                         WHEN chat_record_type = 'RD Chat' AND chat_wait_time <= 60 AND chat_handle_time = 0
                           THEN chat_transcript_id
                         WHEN chat_record_type = 'Internal Advocate Success' AND chat_wait_time <= 180
                           AND chat_handle_time = 0
                           THEN chat_transcript_id
                         ELSE NULL
                       END)                AS abandoned_chats
      , total_handled - abandoned_chats    AS total_handled_sla
    FROM app_cash_cs.public.live_agent_chat_escalations
    WHERE
      chat_record_type IN ('RD Chat', 'Internal Advocate Success')
      AND DATE(chat_created_at) >= '2022-01-01'
    GROUP BY 1, 2, 3, 4, 5, 6
  )
  , ast_rd_final AS
  (
    SELECT
      COALESCE(e.entering_date, h.handle_date)               AS date_pt
      , h.employee_id
      , h.full_name
      , h.city
      , COALESCE(h.vertical, e.vertical)                     AS vertical
      , COALESCE(h.channel, e.channel)                       AS channel
      , COALESCE(h.business_unit_name, e.business_unit_name) AS business_unit_name
      , e.entering_volume                                    AS touches_entering
      , h.total_handled                                      AS touches_handled
      , NULL                                                 AS total_response_time_min
      , h.total_handle_time_min
      , h.total_in_sl
      , h.total_handled_sla                                  AS total_touches_sla
      , NULL                                                 AS touches_art
    FROM entering_rd_ast_volume AS e
    LEFT JOIN handled_rd_ast_volume AS h
      ON e.entering_date = h.handle_date
      AND e.vertical = h.vertical
      AND e.employee_id = h.employee_id
  )
SELECT *
FROM messaging_final --test
UNION
SELECT *
FROM ast_rd_final
