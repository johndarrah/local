-- Cf1 cases missing from AWC
SELECT
  DATE_TRUNC(WEEK, c.case_creation_date_time)::DATE          AS week_ds
  , COUNT(DISTINCT IFF(cr.case_id IS NULL, c.case_id, NULL)) AS null_case_count
  , COUNT(DISTINCT c.case_id)                                AS total_case_count
  , ROUND(null_case_count / total_case_count * 100) || '%'   AS pct_of_cf1_calls_missing_from_awc
FROM app_cash_cs.public.support_cases c
LEFT JOIN app_cash_cs.preprod.call_records cr
  ON c.case_id = cr.case_id
WHERE
  1 = 1
  AND YEAR(c.case_creation_date) >= 2023
  AND c.origin = 'Phone'
GROUP BY 1
ORDER BY 1 DESC
;

-- AWC cases missing from CF1
SELECT
  DATE_TRUNC(WEEK, cr.call_date)::DATE                     AS week_ds
  , COUNT_IF(cr.case_id IS NULL)                           AS null_case_count
  , COUNT(*)                                               AS total_case_count
  , ROUND(null_case_count / total_case_count * 100) || '%' AS pct_of_awc_calls_missing_from_cf1
FROM app_cash_cs.preprod.call_records cr
WHERE
  1 = 1
  AND YEAR(cr.call_date) >= 2023
  AND cr.initiation_method = 'INBOUND'
  AND cr.talk_time > 5
GROUP BY 1
ORDER BY 1 DESC
