-- author: johndarrah
-- description: voice SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS
-- CALL_RECORDS = https://block.sourcegraph.com/github.com/squareup/app-datamart-cco/-/blob/jobs/voice_touches/voice_touches.sql

-- Notes
-- timestamps in UTC
-- callbacks are missing employee information

SELECT
  1 = 1
  --   TO_CHAR(
  --     DATE_TRUNC(HOURS,
  --                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', cr.call_start_time)
  --       ),
  --     'YYYY-MM-DD HH24:MI:SS')                                          AS entering_hour
  --   , ecd.employee_id
  --   , ecd.full_name
  --   , ecd.city
  --   , CASE
  --       WHEN cr.queue_name IN ('US-EN Cash CS Manager Escalations',
  --                              'US-EN Cash CS Manager Escalation',
  --                              'US-EN Cash Concierge',
  --                              'US-EN Manager Escalations',
  --                              'US-ES Manager Escalations')
  --         THEN 'ERET'
  --       ELSE 'CORE CS'
  --     END                                                               AS vertical
  --   , 'Voice'                                                           AS channel
  , COUNT(IFF(cr.speed_to_callback IS NOT NULL, cr.contact_id, NULL)) AS count_of_callbacks
  , SUM(cr.handle_time) / 60                                          AS handle_time_min
  , AVG(cr.handle_time) / 60                                          AS avg_handle_time_min
  , SUM(cr.speed_to_callback / 60)                                    AS response_time_min
  , AVG(cr.speed_to_callback) / 60                                    AS avg_response_time_min
  , COUNT(DISTINCT
          CASE
            WHEN cr.initiation_method = 'INBOUND'
              THEN cr.contact_id
            ELSE NULL
          END)                                                        AS touches_entering
  , COUNT(DISTINCT CASE
                     WHEN cr.is_handled = TRUE
                       THEN cr.contact_id
                     ELSE NULL
                   END)                                               AS touches_handled
  , COUNT(DISTINCT
          CASE
            WHEN cr.initiation_method = 'INBOUND'
              AND cr.is_handled = TRUE
              THEN cr.contact_id
            ELSE NULL
          END)                                                        AS handled_inbound
  , COUNT(DISTINCT CASE
                     WHEN cr.initiation_method = 'INBOUND'
                       AND cr.out_of_hours = FALSE
                       THEN cr.contact_id
                     WHEN cr.initiation_method = 'INBOUND'
                       AND cr.out_of_hours = TRUE
                       AND cr.is_handled = TRUE
                       THEN cr.contact_id
                     ELSE NULL
                   END)                                               AS inbound_touches_hoops
  , COUNT(DISTINCT CASE
                     WHEN cr.is_abandoned
                       AND NOT cr.is_queued
                       AND cr.speed_to_callback IS NULL
                       AND DATEDIFF('second', cr.call_start_time, cr.call_end_time) <= 10
                       THEN cr.contact_id
                     ELSE NULL
                   END)                                               AS short_abandons
  , COUNT(DISTINCT
          CASE
            WHEN cr.initiation_method = 'OUTBOUND'
              THEN cr.contact_id
          END)                                                        AS dialouts
  , COUNT(DISTINCT
          CASE
            WHEN cr.initiation_method IN ('API', 'INBOUND')
              AND cr.wait_time < 120
              AND cr.agent_user_name IS NOT NULL
              THEN cr.contact_id
            WHEN cr.initiation_method IN ('INBOUND')
              AND cr.speed_to_callback < 1800
              THEN cr.contact_id
            ELSE NULL
          END)                                                        AS touches_in_sl
  , CASE
      WHEN inbound_touches_hoops = 0
        THEN NULL
      ELSE (inbound_touches_hoops - short_abandons)
    END                                                               AS qualified_sla_touches
  , touches_in_sl / qualified_sla_touches                             AS sl_percent
FROM app_cash_cs.preprod.call_records cr
LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
  ON ecd.amazon_connect_id = cr.agent_user_name
  AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', cr.call_start_time)::DATE BETWEEN ecd.start_date AND ecd.end_date
WHERE
  CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', cr.call_start_time)::DATE >= '2022-01-01'
-- GROUP BY 1, 2, 3, 4, 5, 6