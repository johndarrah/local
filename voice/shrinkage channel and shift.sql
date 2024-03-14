WITH
  base AS (
    SELECT DISTINCT
      employee_id
      , interval_start_utc::DATE                                                                                       AS interval_start_ds_utc
      , schedule_channel
      , shift_name
      , SUM(duration_seconds_during_interval) OVER (PARTITION BY employee_id, interval_start_ds_utc, schedule_channel) AS duration_by_schedule_channel
      , SUM(duration_seconds_during_interval) OVER (PARTITION BY employee_id, interval_start_ds_utc)                   AS total_duration_with_schedule_channel
      , ROUND(duration_by_schedule_channel / NULLIFZERO(total_duration_with_schedule_channel) * 100, 2)                AS pct_of_duration_by_schedule_channel
    FROM app_datamart_cco.assembled.advocate_schedule_aggregate
    WHERE
      1 = 1
      -- and interval_start_ds_utc='2024-04-07'
      AND interval_start_utc::DATE BETWEEN '2024-01-01' AND CURRENT_DATE
      AND employee_id = '57665'
    -- AND LEN(schedule_channel) > 1
    ORDER BY 2 DESC
  )

SELECT
  b.employee_id
  , b.interval_start_ds_utc
  , b.schedule_channel
  , b.shift_name
  , b.duration_by_schedule_channel
  , b.total_duration_with_schedule_channel
  , b.pct_of_duration_by_schedule_channel
  , ROW_NUMBER() OVER (PARTITION BY b.employee_id,b.interval_start_ds_utc ORDER BY schedule_channel DESC NULLS LAST,b.duration_by_schedule_channel DESC) AS test
FROM base b
-- QUALIFY
--   ROW_NUMBER() OVER (PARTITION BY b.employee_id,b.interval_start_ds_utc ORDER BY schedule_channel nulls last,b.duration_by_schedule_channel DESC) = 1
ORDER BY interval_start_ds_utc DESC
;


WITH
  base AS (
    SELECT DISTINCT
      call_start_time_utc::DATE AS ds
      , e.employee_id
    FROM app_cash_cs.preprod.call_records cr
    JOIN app_datamart_cco.workday.cs_employees_and_agents e
      ON cr.agent_user_name = e.amazon_connect_id
      AND cr.call_start_time_utc::DATE BETWEEN e.start_date AND e.end_date
    WHERE
      e.employee_id = '57665'
      AND call_start_time_utc::DATE = '2024-03-04'
    GROUP BY 1, 2
  )

SELECT
  interval_start_utc::DATE                                 AS interval_start_ds_utc
  , shift_name
  , ROUND(SUM(duration_seconds_during_interval) / 3600, 2) AS scheduled
  , ROUND(SUM(non_prod_seconds_during_interval) / 3600, 2) AS non_prod
FROM app_datamart_cco.assembled.advocate_schedule_aggregate a
JOIN base b
  ON a.employee_id = b.employee_id
  AND a.interval_start_utc::DATE = b.ds
-- join app_datamart_cco.workday.cs_employees_and_agents e
-- on a.employee_id = e.employee_id
-- and a.interval_start_utc::date BETWEEN e.start_date and e.end_date
-- join app_cash_cs.preprod.call_records cr
-- on e.amazon_connect_id = cr.agent_user_name
WHERE
  a.employee_id = '57665'
  AND interval_start_ds_utc = '2024-03-04'
GROUP BY 1, 2

;

SELECT
  COUNT(interval_start_utc::DATE || employee_id)
  , COUNT(DISTINCT (interval_start_utc::DATE || employee_id))
FROM app_datamart_cco.assembled.advocate_schedule_aggregate a
