WITH
  base AS (
    SELECT
      call_start_time_utc::DATE AS ds
      , agent_user_name
      , is_in_app_queue
      , COUNT(contact_id)       AS total_calls
    FROM app_cash_cs.preprod.call_records
    WHERE
      YEAR(call_start_time_utc) = 2024
      AND agent_user_name IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY ds DESC, agent_user_name
  )
  , base2 AS (
    SELECT *
    FROM base
    QUALIFY
      COUNT(*) OVER (PARTITION BY ds,agent_user_name) > 1
  )
SELECT
  ds
  , SUM(IFF(is_in_app_queue, total_calls, NULL)) AS pda_calls
  , SUM(total_calls)                             AS all_calls
  , ROUND(pda_calls / all_calls * 100, 2)        AS percent_of_pda_calls
FROM base2
GROUP BY 1
ORDER BY 1 DESC
;

SELECT
  call_start_time_utc::DATE AS ds
  , COUNT(contact_id)       AS total_calls
FROM app_cash_cs.preprod.call_records
WHERE
  YEAR(call_start_time_utc) = 2024
  AND agent_user_name IS NOT NULL
  AND is_in_app_queue
GROUP BY 1
ORDER BY 1 DESC