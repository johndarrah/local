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
    -- AND sc.customer_token = 'C_002210y39'
  )
  , new_issue_contact AS (
    SELECT DISTINCT
      b1.case_id
      , DATEDIFF(DAY, b1.case_creation_date_time, b2.case_creation_date_time) AS days_since_new_issue_contact
      , b2.case_creation_date_time                                            AS new_issue_contact_case_ts
      -- , ARRAY_CONSTRUCT(b1.origin, b2.origin)                                 AS origins
      , TRUE                                                                  AS has_new_issue_contact
      , b2.problem_tag                                                        AS new_issue_contact_problem_tag
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
      , DATEDIFF(DAY, b1.case_creation_date_time, b2.case_creation_date_time) AS days_since_repeat_issue_contact
      , b2.case_creation_date_time                                            AS repeat_issue_contact_case_ts
      -- , ARRAY_CONSTRUCT(b1.origin, b2.origin)                                 AS origins
      , TRUE                                                                  AS has_repeat_issue_contact
      , b2.problem_tag                                                        AS repeat_issue_contact_problem_tag
    FROM base b1
    JOIN base b2
      ON b1.customer_token = b2.customer_token
      AND b1.case_id != b2.case_id
      AND b1.problem_tag = b2.problem_tag
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY b1.case_id ORDER BY ABS(days_since_repeat_issue_contact)) = 1
  )
  , fcr_calculation AS (
    SELECT
      b.customer_id
      , b.customer_token
      , b.problem_tag
      , b.case_creation_date_time
      , b.case_id
      , ric.days_since_repeat_issue_contact
      , ric.repeat_issue_contact_case_ts
      , ric.has_repeat_issue_contact
      , ric.repeat_issue_contact_problem_tag
      , nic.days_since_new_issue_contact
      , nic.new_issue_contact_case_ts
      , nic.has_new_issue_contact
      , nic.new_issue_contact_problem_tag

      , IFF(ABS(NVL(ric.days_since_repeat_issue_contact, 100)) > 7, TRUE, FALSE)  AS is_fcr_7_day
      , IFF(ABS(NVL(ric.days_since_repeat_issue_contact, 100)) > 14, TRUE, FALSE) AS is_fcr_14_day
      , IFF(ABS(NVL(ric.days_since_repeat_issue_contact, 100)) > 28, TRUE, FALSE) AS is_fcr_28_day

    FROM base b
    LEFT JOIN new_issue_contact nic
      ON b.case_id = nic.case_id
    LEFT JOIN repeat_issue_contact ric
      ON b.case_id = ric.case_id
    WHERE
      1 = 1
    -- QUALIFY
    --   COUNT(*) OVER (PARTITION BY b.customer_token) > 1
    ORDER BY b.customer_token
  )

SELECT
  ROUND(AVG(ABS(days_since_new_issue_contact)))                                   AS avg_abs_days_since_new_issue_contact
  , ROUND(AVG(ABS(days_since_repeat_issue_contact)))                              AS avg_abs_days_since_repeat_issue_contact
  , ROUND(COUNT(DISTINCT IFF(has_repeat_issue_contact, customer_token, NULL)) /
            COUNT(DISTINCT customer_token) * 100, 2)                              AS percent_of_repeat_customers
  , ROUND(COUNT_IF(is_fcr_7_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2)  AS total_fcr_7_day
  , ROUND(COUNT_IF(is_fcr_14_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2) AS total_fcr_14_day
  , ROUND(COUNT_IF(is_fcr_28_day) / NULLIFZERO(COUNT(DISTINCT case_id)) * 100, 2) AS total_fcr_28_day
FROM fcr_calculation
