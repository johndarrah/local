-- author: johndarrah
-- description: voice SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS

-- Notes


WITH
  voice_sla_cte AS (
    SELECT
      contact_id
      , initiation_method
      , requested_callback
      , speed_to_callback
    FROM app_cash_cs.preprod.call_records cr
    WHERE
      speed_to_callback IS NOT NULL
      AND call_date >= '2022-01-01'
  )


SELECT
  call_date                                                AS date_pt
  , ecd.employee_id
  , ecd.full_name
  , ecd.city
  , CASE
      WHEN queue_name IN ('US-EN Cash CS Manager Escalations', 'US-EN Cash CS Manager Escalation', 'US-EN Cash Concierge',
                          'US-EN Manager Escalations',
                          'US-ES Manager Escalations')
        THEN 'ERET'
      ELSE 'CORE CS'
    END                                                    AS vertical
  , 'Voice'                                                AS channel
  , SUM(v.speed_to_callback / 60)                          AS total_response_time_min
  , COUNT(v.speed_to_callback)                             AS not_null_rt
  , SUM(handle_time) / 60                                  AS total_handle_time_min
  , COUNT(DISTINCT CASE
                     WHEN cr.initiation_method = 'INBOUND'
                       THEN cr.contact_id
                     ELSE NULL
                   END)                                    AS touches_entering
  , COUNT(DISTINCT CASE
                     WHEN cr.is_handled = TRUE
                       THEN cr.contact_id
                     ELSE NULL
                   END)                                    AS touches_handled
  , COUNT(DISTINCT
          CASE
            WHEN cr.initiation_method IN ('API', 'INBOUND') AND cr.wait_time < 120 AND cr.agent_user_name IS NOT NULL
              THEN cr.contact_id
            WHEN cr.initiation_method IN ('INBOUND') AND cr.speed_to_callback < 1800
              THEN cr.contact_id
            ELSE NULL
          END)                                             AS total_in_sl
  , COUNT(DISTINCT CASE
                     WHEN cr.initiation_method = 'INBOUND' AND cr.is_handled = TRUE
                       THEN cr.contact_id
                     ELSE NULL
                   END)                                    AS handled_inbound
  , COUNT(DISTINCT CASE
                     WHEN cr.initiation_method = 'INBOUND' AND cr.requested_callback = TRUE
                       THEN cr.contact_id
                     ELSE NULL
                   END
  )                                                        AS inbound_rejected_requested_callback
  , CASE
      WHEN handled_inbound + inbound_rejected_requested_callback = 0
        THEN NULL
      ELSE handled_inbound + inbound_rejected_requested_callback
    END                                                    AS total_touches_sla
  , total_response_time_min / NULLIF(total_touches_sla, 0) AS avg_response_time_min
  , total_handle_time_min / NULLIF(touches_handled, 0)     AS avg_handle_time_min
FROM app_cash_cs.preprod.call_records cr
LEFT JOIN voice_sla_cte v
  ON v.contact_id = cr.initial_contact_id
LEFT OUTER JOIN app_cash_cs.public.employee_cash_dim ecd
  ON ecd.amazon_connect_id = cr.agent_user_name
  AND cr.call_date BETWEEN ecd.start_date AND ecd.end_date
WHERE
  call_date >= '2022-01-01'
GROUP BY 1, 2, 3, 4, 5, 6
