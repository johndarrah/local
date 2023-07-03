WITH
  intervals AS (
    SELECT DISTINCT
      report_date
      , interval_start_time
      , interval_end_time
    FROM app_cash_cs.public.dim_date_time
    WHERE
      YEAR(report_date) > 2020
      AND report_date = '2023-07-03'
    ORDER BY 2
  )
  , handle_times AS (
  SELECT
    sc.case_number
    , 'https://cf1.lightning.force.com/lightning/r/Case/' || tbu.case_id || '/view' AS cf1_link
    , sc.case_creation_date_time::DATE                                              AS case_creation_date
    , ecd.full_name
    , ecd.ldap_today
    , ecd.manager
    , ecd.managers_manager
    , tbu.start_timestamp                                                           AS touch_start_time
    , tbu.stop_timestamp                                                            AS touch_end_time
    , TIME_SLICE(tbu.start_timestamp, 30, 'MINUTE')                                 AS touch_start_interval
    , DATEADD(MINUTE, 30, TIME_SLICE(tbu.stop_timestamp, 30, 'MINUTE'))             AS touch_end_interval
    , tbu.time_spent                                                                AS handle_time_seconds
    , SUM(tbu.time_spent) OVER (PARTITION BY tbu.case_id)                           AS total_case_handle_time_seconds
    , SUM(tbu.time_spent) OVER (PARTITION BY tbu.user_id)                           AS total_advocate_handle_time_seconds
    , sc.problem_tag
  FROM app_cash_cs.cfone.time_by_users tbu
  JOIN app_cash_cs.public.support_cases sc
    ON tbu.case_id = sc.case_id
  LEFT JOIN app_datamart_cco.sfdc_cfone.users u
    ON tbu.user_id = u.id
  LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
    ON LOWER(u.email) = LOWER(ecd.email_today)
    AND ecd.role_status = 'Current'
  WHERE
    1 = 1
    --   case creation date filter
    AND sc.case_creation_date_time::DATE >= '2023-06-01'
    --   touch start date filter
    AND tbu.start_timestamp::DATE >= '2023-06-05'
    --   touch end date filter
    AND tbu.stop_timestamp::DATE >= CURRENT_DATE
    AND ecd.full_name IN (
    --                         'Fatoumata Conteh',
    --                         'Jasmin Nixon',
    --                         'Vanessa Cheers',
                          'Kayla Fogarasi',
                          NULL)
    AND touch_start_time::DATE = '2023-07-03'
    AND case_number = '100278967'
    --     AND touch_start_time = '2023-07-03 15:45:22.000000000'
  ORDER BY tbu.start_timestamp
)
  , calculation AS (
  SELECT DISTINCT
    1 = 1
    , i.*
    , ht.full_name
    , ht.touch_start_interval
    , ht.touch_end_interval
    , ht.touch_start_time
    , ht.touch_end_time
    , ht.handle_time_seconds
    , CASE
    --     touch is within a single interval
        WHEN ht.touch_start_time >= i.interval_start_time
          AND ht.touch_end_time <= i.interval_end_time
          THEN DATEDIFF(SECONDS, ht.touch_start_time, ht.touch_end_time)
    --     part 1: touch is in two intervals
        WHEN ht.touch_start_time >= i.interval_start_time
          THEN DATEDIFF(SECONDS, ht.touch_start_time, i.interval_end_time)
    --     part 2: touch is in two intervals
        ELSE DATEDIFF(SECONDS, i.interval_start_time, ht.touch_end_time)
      END                                                             AS derived_handle_time_sec
    , derived_handle_time_sec / 60                                    AS derived_handle_time_min
    , SUM(derived_handle_time_sec) OVER (PARTITION BY ht.case_number) AS validation_sum
  FROM intervals i
  JOIN handle_times ht
    ON ht.touch_end_time >= i.interval_start_time
    AND ht.touch_start_time <= i.interval_end_time
  ORDER BY interval_start_time
)

SELECT
  report_date
  , interval_start_time
  , interval_end_time
  , COUNT(DISTINCT full_name)    AS total_advocates
  , SUM(derived_handle_time_min) AS total_minutes
FROM calculation
GROUP BY 1, 2, 3
ORDER BY 2

