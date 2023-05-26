-- author: john darrah
-- sources
-- code
-- https://squarewave.sqprod.co/#/jobs/379/sql
-- looker
-- https://square.cloud.looker.com/explore/Support/cash_daily_queue_volumes?qid=AR6l2wjfFLMm7IWNNCsHhh&origin_space=11017&toggle=fil


WITH
  handled_agg AS (
    SELECT
      TIME_SLICE(touch_time, 30, 'minute')                AS touch_30_ts
      , e.queue_name
      , e.queue_id
      , q.communication_channel                           AS channel
      , CASE
          WHEN s.country_code != 'US' AND s.country_code IS NOT NULL
            THEN 'GLOBAL'
          ELSE q.team_name
        END                                               AS vertical
      , q.business_unit_name
      , COUNT(DISTINCT touch_id)                          AS handled_volume
      , COUNT(DISTINCT CASE
                         WHEN touch_type = 'EML' OR touch_type = 'EML/STS'
                           THEN touch_id
                         ELSE NULL
                       END)                               AS email_handled
      , COUNT(DISTINCT CASE
                         WHEN (touch_type = 'TRN' OR touch_type = 'EML/TRN')
                           THEN touch_id
                         ELSE NULL
                       END)                               AS transfer_handled
      , COUNT(DISTINCT CASE
                         WHEN touch_type = 'STS'
                           THEN touch_id
                         ELSE NULL
                       END)                               AS resolved_handled
      , COUNT(DISTINCT CASE
                         WHEN touch_type = 'TR' OR touch_type = 'EML/TR'
                           THEN touch_id
                         ELSE NULL
                       END)                               AS bulk_handled
      , COUNT(DISTINCT CASE
                         WHEN touch_type IN ('EML', 'EML/TRN', 'TR', 'EML/STS', 'EML/TR')
                           THEN touch_id
                         ELSE NULL
                       END)                               AS response_handled
      , SUM(CASE
              WHEN touch_type IN ('EML', 'EML/TRN', 'TR', 'EML/STS', 'EML/TR')
                THEN response_time_hours
              ELSE 0
            END)                                          AS total_response_time
      , SUM(touch_handle_time)                            AS total_handle_time
      , total_response_time / NULLIF(response_handled, 0) AS art
      , total_handle_time / NULLIF(handled_volume, 0)     AS aht
      , COUNT(DISTINCT CASE
                         WHEN (response_time_hours <= 24 AND touch_type != 'TRN')
                           THEN touch_id
                         ELSE NULL
                       END)                               AS handled_in_sl
    FROM app_cash_cs.public.email_touches AS e
    LEFT JOIN app_cash_cs.public.support_cases AS s
      ON e.case_id = s.case_id
    LEFT JOIN app_datamart_cco.archive.team_queue_catalog_all_sfdc AS q
      ON q.queue_id = e.queue_id
    WHERE
      YEAR(touch_30_ts) = 2023
      AND e.queue_name IN ('CS Crypto')
    GROUP BY 1, 2, 3, 4, 5, 6
  )
SELECT
  touch_30_ts
  , queue_name
  , SUM(response_handled)                                          AS response_volume
  , NVL(SUM(handled_in_sl) / NULLIFZERO(SUM(response_handled)), 0) AS percent_in_sla
FROM handled_agg
WHERE
  touch_30_ts IS NOT NULL
  AND YEAR(touch_30_ts) = 2023
  AND response_handled > 0
GROUP BY 1, 2
ORDER BY 1 DESC

;
