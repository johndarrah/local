/************************************************************************************************************
Owner: John Darrah (@johndarrah)
Back Up: Mayuri Magdum (@mayurium)
Business Purpose: Denormalized fact table that aggregates messaging touch data at the queue and advocate level
Date Range: 2018-01-01 onward
Time Zone: UTC
Composite key: ts + queue_id

Other relationships:


Change Log:
2023-08-01 - table created by johndarrah for intraday tableau reporting - https://block.atlassian.net/browse/DATAPALS-4468


Notes:
* Averages aren't included here since the data may be aggregated further downstream and it would lead to data inaccuracies
* Incoming volume is excluded since that hasn't been assigned to an advocate
************************************************************************************************************/
CREATE TABLE IF NOT EXISTS personal_johndarrah.public.fct_hourly_advocate_messaging (
  ts                      TIMESTAMP_NTZ COMMENT 'Touch timestamp in UTC',
  employee_id             VARCHAR COMMENT 'Advocate employee ID',
  advocate_name           VARCHAR COMMENT 'Advocate Name',
  advocate_city           VARCHAR COMMENT 'Advocate City',
  queue_id                VARCHAR COMMENT 'Touch queue ID',
  team_name               VARCHAR COMMENT 'Touch team name',
  channel                 VARCHAR COMMENT 'Touch channel',
  business_unit_name      VARCHAR COMMENT 'Touch business_unit_name',
  handled_touches         NUMBER COMMENT 'Handled Touches',
  response_time_min       NUMBER COMMENT 'Touch response time in minutes',
  handle_time_min         NUMBER COMMENT 'Touch handle_time in minutes',
  touch_lifetime_min      NUMBER COMMENT 'Touch touch_lifetime in minutes',
  concurrency             NUMBER COMMENT 'Touches being handled at the same time',
  handled_backlog_touches NUMBER COMMENT 'Touch handled_backlog_touches',
  touches_in_sl           NUMBER COMMENT 'Touches in Service Level',
  qualified_sla_touches   NUMBER COMMENT 'Qualified SLA touches (in business hours,etc)',
  sl_percent              NUMBER COMMENT 'Touch Service Level Percent '
)
;

INSERT OVERWRITE INTO
  personal_johndarrah.public.fct_hourly_advocate_messaging
WITH
  hour_ts AS (
    SELECT DISTINCT
      interval_start_time                                                        AS hour_interval
      , TO_CHAR(DATE_TRUNC(HOURS, interval_start_time), 'YYYY-MM-DD HH24:MI:SS') AS ts
    FROM app_cash_cs.public.dim_date_time
    WHERE
      YEAR(report_date) >= 2022
      AND report_date <= CURRENT_DATE
      AND EXTRACT(MINUTE FROM interval_start_time) = 0
  )
  , handled_messaging_touches AS (
  SELECT
    ht.ts
    , ecd.employee_id
    , ecd.full_name                                                       AS advocate_name
    , ecd.city                                                            AS advocate_city
    , tqc.queue_id
    , tqc.team_name                                                       AS team_name
    , tqc.communication_channel                                           AS channel
    , tqc.business_unit_name
    , COUNT(DISTINCT mt.touch_id)                                         AS handled_touches
    , SUM(mt.response_time_seconds) / 60                                  AS response_time_min
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
                AND mt.in_business_hours
                THEN mt.touch_id
              ELSE NULL
            END)                                                          AS touches_in_sl
    , COUNT(DISTINCT IFF(mt.in_business_hours, mt.touch_id, NULL))        AS qualified_sla_touches
    , touches_in_sl / NULLIFZERO(qualified_sla_touches) * 100             AS sl_percent
  FROM hour_ts ht
  LEFT JOIN app_cash_cs.preprod.messaging_touches mt
    ON ht.hour_interval = DATE_TRUNC('hour', CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_assignment_time))
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(mt.queue_name) = LOWER(tqc.queue_name)
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON mt.employee_id = ecd.employee_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', mt.touch_assignment_time) BETWEEN ecd.start_date AND ecd.end_date

  WHERE
    NVL(LOWER(tqc.business_unit_name), 'other') IN ('customer success - specialty', 'customer success - core', 'other')
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
  , handled_rd_ast_touches AS (
  SELECT
    ht.ts
    , ecd.employee_id
    , ecd.full_name                                                                   AS advocate_name
    , ecd.city                                                                        AS advocate_city
    , IFF(e.chat_record_type = 'RD Chat', '00G5w000006vq2tEAA', '00G5w000006wBA1EAM') AS queue_id
    , IFF(e.chat_record_type = 'RD Chat', 'RD', 'AST')                                AS team_name
    , 'CHAT'                                                                          AS channel
    , 'CUSTOMER SUCCESS - CORE'                                                       AS business_unit_name
    , COUNT(DISTINCT e.chat_transcript_id)                                            AS handled_touches
    , NULL                                                                            AS handled_backlog_touches
    , SUM(e.chat_handle_time / 60)                                                    AS handle_time_min
    , SUM(e.chat_handle_time / 60)                                                    AS touch_lifetime_min
    , touch_lifetime_min / handle_time_min                                            AS concurrency
    , NULL                                                                            AS response_time_min
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
            END)                                                                      AS touches_in_sl
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
            END)                                                                      AS abandoned_touches
    , handled_touches - abandoned_touches                                             AS qualified_sla_touches
    , touches_in_sl / qualified_sla_touches * 100                                     AS sl_percent
  FROM hour_ts ht
  LEFT JOIN app_cash_cs.public.live_agent_chat_escalations e
    ON ht.hour_interval = DATE_TRUNC(HOURS, CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', e.chat_start_time))
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON e.chat_advocate_employee_id = ecd.employee_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', e.chat_start_time)::DATE BETWEEN ecd.start_date AND ecd.end_date
  WHERE
    chat_record_type IN ('RD Chat', 'Internal Advocate Success')
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
  -- messaging touches
SELECT
  ts
  , employee_id
  , advocate_name
  , advocate_city
  , queue_id
  , team_name
  , channel
  , business_unit_name
  , handled_touches
  , response_time_min
  , handle_time_min
  , touch_lifetime_min
  , concurrency
  , handled_backlog_touches
  , touches_in_sl
  , qualified_sla_touches
  , sl_percent
FROM handled_messaging_touches
WHERE
  1 = 1
  AND ts::DATE = '2023-07-06'
  AND employee_id = '44835'

UNION

--   AST and RD touches
SELECT
  ts
  , employee_id
  , advocate_name
  , advocate_city
  , queue_id
  , team_name
  , channel
  , business_unit_name
  , handled_touches
  , response_time_min
  , handle_time_min
  , touch_lifetime_min
  , concurrency
  , handled_backlog_touches
  , touches_in_sl
  , qualified_sla_touches
  , sl_percent
FROM handled_rd_ast_touches
WHERE
  1 = 1
  AND ts::DATE = '2023-07-06'
  AND employee_id = '44835'
;

SELECT *
FROM personal_johndarrah.public.fct_hourly_advocate_messaging
;

-- DROP TABLE personal_johndarrah.public.fct_hourly_advocate_messaging
DESCRIBE table personal_johndarrah.public.fct_hourly_advocate_messaging