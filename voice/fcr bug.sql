-- current: 1,028,084
-- 6,459,889
WITH
  base AS (
    SELECT DISTINCT
      sc.customer_token
      , sc.problem_tag
      , CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', sc.case_creation_date_time) AS case_creation_ts_utc
      , sc.case_id
      , sc.origin
    FROM app_cash_cs.public.support_cases sc
    JOIN app_cash_cs.preprod.call_records cr
      ON cr.case_id = sc.case_id
    WHERE
      1 = 1
      -- AND YEAR(sc.case_creation_date_time) = 2024
      and year(case_creation_ts_utc)=2024
      AND sc.customer_token IS NOT NULL
    -- AND sc.customer_token = 'C_002210y39'
  )
  , new_issue_contact AS (
    SELECT DISTINCT
      b1.case_id
      , DATEDIFF(DAY, b1.case_creation_ts_utc, b2.case_creation_ts_utc) AS days_since_new_issue_contact
      , b2.case_creation_ts_utc                                         AS new_issue_contact_case_ts_utc
      , TRUE                                                            AS has_new_issue_contact
      , b2.problem_tag                                                  AS new_issue_contact_problem_tag
    FROM base b1
    JOIN base b2
      ON b1.customer_token = b2.customer_token
      AND b1.case_id != b2.case_id
      AND b1.problem_tag != b2.problem_tag
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY b1.case_id ORDER BY ABS(days_since_new_issue_contact)) = 1
  )
  , repeat_issue_contact AS (
    SELECT DISTINCT
      b1.case_id
      , DATEDIFF(DAY, b1.case_creation_ts_utc, b2.case_creation_ts_utc) AS days_since_repeat_issue_contact
      , b2.case_creation_ts_utc                                         AS repeat_issue_contact_case_ts_utc
      , TRUE                                                            AS has_repeat_issue_contact
      , b2.problem_tag                                                  AS repeat_issue_contact_problem_tag
    FROM base b1
    JOIN base b2
      ON b1.customer_token = b2.customer_token
      AND b1.case_id != b2.case_id
      AND b1.problem_tag = b2.problem_tag
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY b1.case_id ORDER BY ABS(days_since_repeat_issue_contact)) = 1
  )
  , any_issue_contact AS (
    SELECT DISTINCT
      b1.case_id
      , DATEDIFF(DAY, b1.case_creation_ts_utc, b2.case_creation_ts_utc) AS days_since_any_issue_contact
      , b2.case_creation_ts_utc                                         AS any_issue_contact_case_ts_utc
      , TRUE                                                            AS has_any_issue_contact
      , b2.problem_tag                                                  AS any_issue_contact_problem_tag
    FROM base b1
    JOIN base b2
      ON b1.customer_token = b2.customer_token
      AND b1.case_id != b2.case_id
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY b1.case_id ORDER BY ABS(days_since_any_issue_contact)) = 1
  )
, fcr_calculation AS (
SELECT
  b.case_id
  , b.customer_token
  , b.problem_tag
  , b.case_creation_ts_utc
  , ric.days_since_repeat_issue_contact
  , ric.repeat_issue_contact_case_ts_utc
  , ric.has_repeat_issue_contact
  , ric.repeat_issue_contact_problem_tag
  , nic.days_since_new_issue_contact
  , nic.new_issue_contact_case_ts_utc
  , nic.has_new_issue_contact
  , nic.new_issue_contact_problem_tag
  , aic.days_since_any_issue_contact
  , aic.any_issue_contact_case_ts_utc
  , aic.has_any_issue_contact
  , aic.any_issue_contact_problem_tag
  , IFF(ABS(NVL(ric.days_since_repeat_issue_contact, 100)) > 7, TRUE, FALSE)  AS is_fcr_7_day
  , IFF(ABS(NVL(ric.days_since_repeat_issue_contact, 100)) > 14, TRUE, FALSE) AS is_fcr_14_day
  , IFF(ABS(NVL(ric.days_since_repeat_issue_contact, 100)) > 28, TRUE, FALSE) AS is_fcr_28_day
FROM base b
LEFT JOIN new_issue_contact nic
  ON b.case_id = nic.case_id
LEFT JOIN repeat_issue_contact ric
  ON b.case_id = ric.case_id
LEFT JOIN any_issue_contact aic
  ON b.case_id = aic.case_id
WHERE
  1 = 1
-- QUALIFY
--   COUNT(*) OVER (PARTITION BY b.customer_token) > 1
ORDER BY b.customer_token

)

SELECT
  1 = 1
     ,count_if(is_fcr_7_day) as test
  , ROUND(AVG(ABS(days_since_any_issue_contact)))                                     AS avg_abs_days_since_any_issue_contact
  , MEDIAN(IFF(has_any_issue_contact, ABS(days_since_any_issue_contact), NULL))       AS median_abs_days_since_any_issue_contact

  , ROUND(AVG(ABS(days_since_new_issue_contact)))                                     AS avg_abs_days_since_new_issue_contact
  , ROUND(AVG(ABS(days_since_repeat_issue_contact)))                                  AS avg_abs_days_since_repeat_issue_contact
  , MEDIAN(IFF(has_repeat_issue_contact, ABS(days_since_repeat_issue_contact), NULL)) AS median_abs_days_since_repeat_issue_contact
  , ROUND(COUNT(DISTINCT IFF(has_new_issue_contact, customer_token, NULL)) /
            COUNT(DISTINCT customer_token) * 100, 2)                                  AS percent_of_new_issue_customers
  , ROUND(COUNT(DISTINCT IFF(has_repeat_issue_contact, customer_token, NULL)) /
            COUNT(DISTINCT customer_token) * 100, 2)                                  AS percent_of_repeat_issue_customers
  , ROUND(COUNT_IF(is_fcr_7_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2)      AS total_fcr_7_day
  , ROUND(COUNT_IF(is_fcr_14_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2)     AS total_fcr_14_day
  , ROUND(COUNT_IF(is_fcr_28_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2)     AS total_fcr_28_day
FROM fcr_calculation
WHERE
  1 = 1
;
