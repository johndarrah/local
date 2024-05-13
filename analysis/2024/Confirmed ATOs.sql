WITH
  -- correction_comments AS ( -- using this CTE to flag any case number that has a corection comment linked to it even if it had more than one ATO comment
  --   SELECT DISTINCT
  --     target_token
  --     , REGEXP_SUBSTR(comment, '[0-9]{8}([0-9]{1})?') AS correction_comment_case_number
  --     , TRUE                                          AS correction_flag
  --   FROM app_cash_cs.public.ato_hashtags
  --   WHERE
  --     UPPER(comment) ILIKE '%#$ATO_CORRECTION%'
  -- ) ,
  ato_hashtags AS (
    SELECT
      h.target_token                                                          AS customer_token
      , REGEXP_SUBSTR(h.comment, '[0-9]{8}([0-9]{1})?')                       AS comment_case_number
      , CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', h.hashtag_date_time)   AS hashtag_ts_utc
      -- , h.target_token || h.hashtag_date_time                                 AS unique_identifier
      , h.full_name
      , h.ldap                                                                AS advocate_ldap
      , h.employee_id
      , h.comment
      , h.hashtag
      , CASE
          WHEN TRIM(h.comment) ILIKE '%#$ATO_INV_ATO%'
            THEN 'Self Reported'
          WHEN TRIM(h.comment) ILIKE '%#$ATO_INV_LATO%'
            THEN 'Auto-Lock'
          ELSE 'Other'
        END                                                                   AS autolock_or_self_reported
      , CASE
          WHEN TRIM(h.comment) ILIKE ANY ('%#$ATO_RESET_CONFIRMED%', '%#$ATO_REIMBURSMENT%', '%#$ATO_SECURED_FFATO%')
            THEN TRUE
          ELSE FALSE
        END                                                                   AS is_confirmed_ato
      , CASE
          WHEN autolock_or_self_reported = 'Auto-Lock'
            AND is_confirmed_ato
            THEN 'Confirmed Auto-Lock'
          WHEN autolock_or_self_reported = 'Self Reported'
            AND is_confirmed_ato
            THEN 'Confirmed Self Reported'
          ELSE NULL
        END                                                                   AS ato_type
      , ARRAY_AGG(DISTINCT h.hashtag) OVER (PARTITION BY comment_case_number) AS hashtag_array
    -- , h.ato_hash_key
    -- , ROW_NUMBER() OVER (PARTITION BY customer_token, h.comment, h.hashtag_at ORDER BY h.hashtag_at DESC)    AS hashtag_row_number
    -- , ROW_NUMBER() OVER (PARTITION BY customer_token, comment_case_number ORDER BY h.hashtag_date_time DESC) AS comment_row_number
    -- , cc.correction_flag
    -- , CASE
    --     WHEN cc.correction_flag = TRUE
    --       THEN comment_row_number
    --     WHEN cc.correction_flag IS NULL
    --       THEN hashtag_row_number
    --   END                                                                                                    AS row_num
    FROM app_cash_cs.public.ato_hashtags h -- https://github.com/squareup/app-datamart-cco/blob/main/jobs/regulator_risk_hashtags/regulator_risk_hashtags.sql
    WHERE
      1 = 1
      AND YEAR(hashtag_ts_utc) >= 2023
      AND is_confirmed_ato
    -- LEFT JOIN correction_comments cc
    --   ON comment_case_number = cc.correction_comment_case_number
  )
  , rollbacks AS (
    SELECT
      r.customer_token
      , r.category
      , r.mass_rollback_type
      , r.rolled_back_to AS rolled_back_to_ts_utc
      , r.created_at     AS roll_back_actioned_ts_utc
    FROM app_cash.app.asset_rollbacks r
    WHERE
      1 = 1
      AND YEAR(roll_back_actioned_ts_utc) >= 2023
    -- remove dupicate rollbacks occuring on the same day
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY r.customer_id,r.created_at::DATE ORDER BY r.created_at DESC) = 1
  )
  , notary AS (
    SELECT
      a.customer_token
      , d.category
      , d.reason
      , d.submitted_at_utc
      , d.locking_models
      , a.advocate_ldap
    FROM app_datamart_cco.notary.ato_details d
    LEFT JOIN app_datamart_cco.notary.assignments a
      ON d.assignment_id = a.assignment_id
    WHERE
      1 = 1
      AND YEAR(d.submitted_at_utc) >= 2023
  )

SELECT DISTINCT
  ah.customer_token
  , ah.comment_case_number
  , ah.hashtag_ts_utc
  , ah.hashtag
  , ah.autolock_or_self_reported
  , ah.is_confirmed_ato
  , ah.ato_type
  , ah.advocate_ldap
  , ah.hashtag_array
  , r.category
  , r.mass_rollback_type
  , r.rolled_back_to_ts_utc
  , r.roll_back_actioned_ts_utc
-- , n.category         AS notary_category
-- , n.reason           AS notary_reason
-- , n.submitted_at_utc AS notary_submitted_at_utc
-- , n.locking_models   AS notary_locking_models
-- , n.advocate_ldap    AS notary_advocate_ldap
FROM rollbacks r
LEFT JOIN ato_hashtags ah
  ON  r.customer_token = ah.customer_token
  AND  r.roll_back_actioned_ts_utc::DATE = ah.hashtag_ts_utc::DATE
LEFT JOIN notary n
  ON ah.customer_token = n.customer_token
  AND ah.hashtag_ts_utc::DATE = n.submitted_at_utc::DATE
WHERE
  1 = 1
  -- AND r.customer_token IS NULL
  AND ah.customer_token = 'C_5ednd1m75'
-- AND r.mass_rollback_type IS NULL
-- AND NVL(r.category, '') NOT IN ('FRIENDLY_FRAUD_ATO', 'RAT_ANDROID')
-- AND r.roll_back_actioned_ts_utc IS NOT NULL
-- AND ah.comment_case_number = '122893444'
-- AND ah.autolock_or_self_reported = 'Other'
ORDER BY ah.comment_case_number
;

