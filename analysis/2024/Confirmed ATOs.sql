WITH
  ato_hashtags AS (
    SELECT
      h.target_token                                                                                 AS customer_token
      , REGEXP_SUBSTR(h.comment, '[0-9]{8}([0-9]{1})?')                                              AS comment_case_number
      , CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', h.hashtag_date_time)                          AS hashtag_ts_utc
      , h.full_name
      , h.ldap                                                                                       AS advocate_ldap
      , h.employee_id
      , h.comment
      , h.hashtag
      , ARRAY_AGG(DISTINCT LOWER(h.hashtag)) OVER (PARTITION BY h.target_token ,comment_case_number) AS hashtag_array
      , CASE
          WHEN ARRAY_CONTAINS('ato_inv_ato'::VARIANT, hashtag_array)
            THEN 'Self Reported'
          WHEN ARRAY_CONTAINS('ato_inv_lato'::VARIANT, hashtag_array)
            THEN 'Auto-Lock'
          ELSE 'No Applicable Hashtag'
        END                                                                                          AS autolock_or_self_reported
      , CASE
          WHEN ARRAY_CONTAINS('ato_reset_confirmed'::VARIANT, hashtag_array)
            THEN TRUE
          WHEN ARRAY_CONTAINS('ato_reimbursment'::VARIANT, hashtag_array)
            THEN TRUE
          WHEN ARRAY_CONTAINS('ato_secured_ffato'::VARIANT, hashtag_array)
            THEN TRUE
          WHEN ARRAY_CONTAINS('ato_p2p_escalation'::VARIANT, hashtag_array)
            THEN TRUE
          ELSE FALSE
        END                                                                                          AS is_confirmed_via_hashtag
      , CASE
          WHEN autolock_or_self_reported = 'Auto-Lock'
            AND is_confirmed_via_hashtag
            THEN 'Confirmed Auto-Lock'
          WHEN autolock_or_self_reported = 'Self Reported'
            AND is_confirmed_via_hashtag
            THEN 'Confirmed Self Reported'
          ELSE NULL
        END                                                                                          AS ato_type
    FROM app_cash_cs.public.ato_hashtags h -- https://github.com/squareup/app-datamart-cco/blob/main/jobs/regulator_risk_hashtags/regulator_risk_hashtags.sql
    WHERE
      1 = 1
      AND YEAR(hashtag_ts_utc) >= 2024
      AND hashtag IN ('ATO_INV_LATO',
                      'ATO_INV_ATO'
        -- 'ATO_RESET_CONFIRMED'
        -- 'ATO_P2P_ESCALATION',
        -- 'ATO_REIMBURSMENT'
        )
    -- AND customer_token = 'C_g62hagynx'
    -- QUALIFY
    --   ROW_NUMBER() OVER (PARTITION BY customer_token, hashtag_ts_utc::DATE ORDER BY autolock_or_self_reported != 'No Applicable Hashtag' DESC) = 1
  )
  , rollbacks AS (
    SELECT
      id                 AS rollback_id
      , r.customer_token
      , r.category
      , r.mass_rollback_type
      , r.rolled_back_to AS rolled_back_to_ts_utc
      , r.created_at     AS roll_back_actioned_ts_utc
    FROM app_cash.app.asset_rollbacks r
    WHERE
      1 = 1
      AND r.mass_rollback_type IS NULL
      AND NVL(r.category, '') NOT IN ('FRIENDLY_FRAUD_ATO', 'RAT_ANDROID')
      AND YEAR(roll_back_actioned_ts_utc) >= 2024
    -- remove duplicate rollbacks occuring on the same day
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY r.customer_id,r.created_at::DATE ORDER BY r.created_at DESC) = 1
  )
  , base AS (
    SELECT DISTINCT
      r.rollback_id
      , r.customer_token
      , r.category
      , r.mass_rollback_type
      , r.rolled_back_to_ts_utc
      , r.roll_back_actioned_ts_utc
      , ah.hashtag_ts_utc
      , ah.comment_case_number
      , CASE
          WHEN ah.autolock_or_self_reported IS NOT NULL
            THEN ah.autolock_or_self_reported
          WHEN ah.autolock_or_self_reported IS NULL
            AND sc.case_id IS NOT NULL
            THEN 'Self Reported'
          ELSE 'No Applicable Hashtag'
        END                                                                        AS autolock_or_self_reported
      , ah.is_confirmed_via_hashtag
      , ah.ato_type
      , ah.advocate_ldap
      , ah.hashtag_array
      , sc.case_id
      , sc.case_number
      , CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', sc.case_creation_date_time) AS case_creation_ts_utc
      , sc.last_assigned_queue
    FROM rollbacks r
    LEFT JOIN app_cash_cs.public.support_cases sc
      ON r.customer_token = sc.customer_token
      AND CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', sc.case_creation_date_time)::DATE
        BETWEEN r.rolled_back_to_ts_utc::DATE AND r.roll_back_actioned_ts_utc::DATE
    LEFT JOIN ato_hashtags ah
      ON r.customer_token = ah.customer_token
      AND ah.hashtag_ts_utc::DATE BETWEEN r.rolled_back_to_ts_utc::DATE AND r.roll_back_actioned_ts_utc::DATE
    -- AND r.roll_back_actioned_ts_utc::DATE = ah.hashtag_ts_utc::DATE
    WHERE
      1 = 1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY rollback_id ORDER BY last_assigned_queue = 'Risk ATO' DESC,case_creation_ts_utc) = 1
    ORDER BY ah.comment_case_number
  )
--------------------- Counts
SELECT DISTINCT
  autolock_or_self_reported
  , COUNT(*)
  , COUNT(DISTINCT rollback_id)
FROM base
WHERE
  1 = 1
GROUP BY 1
;

--------------------- Pulling ATO details

SELECT DISTINCT *
FROM base
WHERE
  1 = 1
  -- AND case_id = '5005w00002QA5QnAAL'
  -- AND customer_token = 'C_g62hagynx'
  AND autolock_or_self_reported = 'No Applicable Hashtag'


--------------------- Validation for missing hashtags
-- SELECT DISTINCT
--   CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', ah.hashtag_date_time)        AS hashtag_ts_utc
--   -- , ah.comment
--   -- , ah.hashtag
--   , ARRAY_AGG(DISTINCT LOWER(ah.hashtag)) OVER (PARTITION BY ah.target_token) AS hashtag_array
--   , CASE
--       WHEN ARRAY_CONTAINS('ato_inv_ato'::VARIANT, hashtag_array)
--         THEN 'Self Reported'
--       WHEN ARRAY_CONTAINS('ato_inv_lato'::VARIANT, hashtag_array)
--         THEN 'Auto-Lock'
--       ELSE 'No Applicable Hashtag'
--     END                                                                       AS autolock_or_self_reported
--   , CASE
--       WHEN ARRAY_CONTAINS('ato_reset_confirmed'::VARIANT, hashtag_array)
--         THEN TRUE
--       WHEN ARRAY_CONTAINS('ato_reimbursment'::VARIANT, hashtag_array)
--         THEN TRUE
--       WHEN ARRAY_CONTAINS('ato_secured_ffato'::VARIANT, hashtag_array)
--         THEN TRUE
--       WHEN ARRAY_CONTAINS('ato_p2p_escalation'::VARIANT, hashtag_array)
--         THEN TRUE
--       ELSE FALSE
--     END                                                                       AS is_confirmed_ato
--   , r.created_at                                                              AS roll_back_actioned_ts_utc
--   , r.rolled_back_to
-- FROM app_cash_cs.public.ato_hashtags ah
-- JOIN app_cash.app.asset_rollbacks r
--   ON r.customer_token = ah.target_token
--   AND r.created_at::DATE = hashtag_ts_utc::DATE
-- WHERE
--   1 = 1
-- -- AND ah.target_token = 'C_5nevrqmzb'
-- QUALIFY
--   autolock_or_self_reported = 'Self Reported'
--   AND NOT is_confirmed_ato

--------------------- confirmed atos count alignment
-- WITH
--   base AS (
--     SELECT DISTINCT *
--     FROM app_cash.app.asset_rollbacks r
--     WHERE
--       1 = 1
--       AND r.mass_rollback_type IS NULL
--       AND NVL(r.category, '') NOT IN ('FRIENDLY_FRAUD_ATO', 'RAT_ANDROID')
--       AND YEAR(created_at) >= 2024
--     QUALIFY
--       ROW_NUMBER() OVER (PARTITION BY r.customer_id,r.created_at::DATE ORDER BY r.created_at DESC) = 1
--   )
-- SELECT
--   COUNT(*)
-- FROM base