-- base query provided
WITH
  escalations AS (
    SELECT DISTINCT
      --       DATE_TRUNC('DAY', chat_start_time) AS worked_date
      chat_start_time AS worked_ts
      , chat_advocate_ldap
      , chat_advocate_bpo_flag
    FROM app_cash_cs.public.live_agent_chat_escalations
    WHERE
      1 = 1
      AND chat_record_type = 'Internal Advocate Success'
      --       AND chat_advocate_ldap = 'kleisek'
    ORDER BY 1
  )
  , activity AS (
  SELECT
    LEFT(email, POSITION('@', email) - 1) AS ldap
    , date_time
    , code
    , status_remove_time
  FROM app_cash_cs.public.assembled_agent_activity_widget
  WHERE
    1 = 1
  --     AND ldap = 'kleisek'
)
SELECT *
FROM escalations e
JOIN activity a
  ON e.chat_advocate_ldap = a.ldap
;

-- proof of concept to get productive minutes at the 30 minute interval
WITH
  base AS (
    SELECT
      LEFT(email, POSITION('@', email) - 1) AS ldap
      , date_time                           AS start_ts
      , code
      , status_remove_time                  AS end_ts
      , CASE
          WHEN code = 'Available'
            THEN DATEDIFF(MINUTE, date_time, status_remove_time)
          ELSE NULL
        END                                 AS delta_minutes
    FROM app_cash_cs.public.assembled_agent_activity_widget
    WHERE
      1 = 1
      AND ldap = 'shyra'
      AND date_time::DATE = '2023-05-25'
      AND code = 'Available'
    ORDER BY date_time
  )

SELECT DISTINCT
  ddt.interval_start_time
  , b.start_ts
  , b.end_ts
  , IFF(ddt.interval_end_time < b.end_ts, 30, DATEDIFF(MINUTE, ddt.interval_start_time, b.end_ts)) AS available_minutes
  , SUM(available_minutes) OVER (PARTITION BY ddt.report_date)                                     AS total_available_minutes_by_day
FROM app_cash_cs.public.dim_date_time ddt
LEFT JOIN base b
  ON ddt.interval_start_time BETWEEN b.start_ts AND b.end_ts
WHERE
  1 = 1
  AND ddt.report_date = '2023-05-25'
ORDER BY 1