WITH
  base AS (
    SELECT DISTINCT
      sc.customer_id
      , sc.customer_token
      , sc.problem_tag
      , sc.case_creation_date_time
      , sc.case_id
      , sc.origin
    -- , cr.customer_endpoint
    -- , cr.is_customer
    -- , cr.is_voice_pii_passed
    -- , cr.voice_pii_customer_token
    -- , sc.problem_type
    -- , sc.selected_category
    -- , sc.amazon_connect_contact_id
    -- , sc.channel
    -- , sc.mapped_queue
    -- , sc.vertical
    -- , sc.contact_id
    -- , cr.case_customer_token
    FROM app_cash_cs.public.support_cases sc
    JOIN app_cash_cs.preprod.call_records cr
      ON cr.case_id = sc.case_id
      AND YEAR(cr.call_start_time_utc) = 2024
    WHERE
      1 = 1
      AND YEAR(sc.case_creation_date_time) = 2024
      AND sc.customer_token IS NOT NULL
    -- AND cr.customer_endpoint = '+++ts3Nrw3HBFO0oHFSvrHN9wnQ4tqYMIt/oVfd/zyw='
    -- AND sc.customer_token = 'C_fhc2c8ye6'
  )
  , repeat_contact AS (
    SELECT DISTINCT
      b1.case_id
      , DATEDIFF(DAY, b1.case_creation_date_time, b2.case_creation_date_time) AS days_since_most_recent_case
      , b2.case_creation_date_time                                            AS most_recent_case_ts
      , ARRAY_CONSTRUCT(b1.origin, b2.origin)                                 AS origins
    FROM base b1
    JOIN base b2
      ON b1.customer_token = b2.customer_token
      AND b1.case_id != b2.case_id
      AND b1.problem_tag = b2.problem_tag
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY b1.case_id ORDER BY ABS(days_since_most_recent_case)) = 1
  )
  -- , fcr_calculation AS (
    SELECT
      1 = 1
      , rc.days_since_most_recent_case
      , rc.most_recent_case_ts
      , rc.origins
      , IFF(rc.most_recent_case_ts IS NOT NULL, TRUE, FALSE)                 AS has_repeat_contact
      , IFF(ABS(NVL(rc.days_since_most_recent_case, 100)) > 7, TRUE, FALSE)  AS is_fcr_7_day
      , IFF(ABS(NVL(rc.days_since_most_recent_case, 100)) > 14, TRUE, FALSE) AS is_fcr_14_day
      , IFF(ABS(NVL(rc.days_since_most_recent_case, 100)) > 28, TRUE, FALSE) AS is_fcr_28_day
      , b.customer_id
      , b.customer_token
      , b.problem_tag
      , b.case_creation_date_time
      , b.case_id
    FROM base b
    LEFT JOIN repeat_contact rc
      ON b.case_id = rc.case_id
    WHERE
      1 = 1
    ORDER BY b.case_id;
  )

SELECT
  ROUND(AVG(ABS(days_since_most_recent_case)))                                    AS avg_abs_days_since_most_recent_case
  , ROUND(COUNT(DISTINCT IFF(has_repeat_contact, customer_token, NULL)) /
            COUNT(DISTINCT customer_token) * 100, 2)                              AS percent_of_repeat_customers
  , ROUND(COUNT_IF(is_fcr_7_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2)  AS total_fcr_7_day
  , ROUND(COUNT_IF(is_fcr_14_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2) AS total_fcr_14_day
  , ROUND(COUNT_IF(is_fcr_28_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2) AS total_fcr_28_day
FROM fcr_calculation
