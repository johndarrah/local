-- author: johndarrah
-- description: email SLA metrics for alerting
-- Resources
-- CS Metrics: https://docs.google.com/spreadsheets/d/1MZUW8icn9Nx1UKc76XX05T_Op_7B7bCPo2NmAHYuGW4/edit#gid=462402470&range=C1
-- sfdc_messaging_facts: https://github.com/squareup/app-datamart-cco/blob/main/jobs/sfdc_messaging_facts/sfdc_messaging_facts.sql
-- glossary: https://wiki.sqprod.co/display/ISWIKI/CCO+Metrics+Definitions#CCOMetricsDefinitions-MESSAGINGMETRICS

-- Notes


SELECT
  date_pt
  , vertical                      AS team_name
  , channel
  , business_unit_name
  , SUM(entering_volume)          AS touches_entering
  , SUM(handled_volume)           AS touches_handled
  , SUM(handled_in_sl)            AS total_in_sl
  , SUM(response_handled)         AS total_touches_sla
  , SUM(total_response_time) * 60 AS total_response_time_min
  , SUM(total_handle_time)        AS total_handle_time_min
FROM app_cash_cs.public.queue_day_touches_agg
WHERE
  channel = 'EMAIL'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC
