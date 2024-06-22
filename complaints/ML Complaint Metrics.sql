WITH
  base AS (
    SELECT
      sc.case_id
      , CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', case_creation_date) AS case_created_ts_utc
      , DATE_TRUNC(MONTH, case_created_ts_utc)::DATE                       AS case_created_month
      , IFF(((sc.is_flagged_by_advocate OR sc.is_flagged_by_ml_listener)
        AND sc.origin = 'Chat'), sc.case_id, NULL)                         AS flagged_as_complaint_by_advocate
      , IFF(sc.origin = 'Chat', sc.case_id, NULL)                          AS is_messaging_case
      , IFF(ml.has_complaint_ccot = 1, sc.case_id, NULL)                   AS flagged_as_complaint_by_ccot
      , CASE
          WHEN ml.has_complaint_ccot = 1 AND ml.has_complaint_ml = 1
            THEN 1
          ELSE 0
        END                                                                AS tp_ml_alert
      , CASE
          WHEN ml.has_complaint_ccot = 1 AND ml.has_complaint_ml = 0
            THEN 1
          ELSE 0
        END                                                                AS fn_ml_alert
      , CASE
          WHEN ml.has_complaint_ccot = 0 AND ml.has_complaint_ml = 1
            THEN 1
          ELSE 0
        END                                                                AS fp_ml_alert
    FROM app_cash_cs.public.support_cases sc
    JOIN app_cash_beta.app.complaint_comparison_ccot_advocate_ml ml
      ON sc.case_id = ml.case_id
  )
SELECT
  case_created_month
  ------- expected complaint rate using CCOT reviewed cases -------
  , COUNT(flagged_as_complaint_by_ccot)                                                              AS total_cases_with_complaints
  , COUNT(case_id)                                                                                   AS total_reviewed_cases_by_ccot
  , total_cases_with_complaints / NULLIFZERO(total_reviewed_cases_by_ccot)                           AS old_expected_complaint_rate
  ------- expected complaint rate using ML metrics -------
  , SUM(tp_ml_alert) / NULLIFZERO((SUM(tp_ml_alert) + SUM(fn_ml_alert)))                             AS ml_listener_recall
  , SUM(tp_ml_alert) / NULLIFZERO((SUM(tp_ml_alert) + SUM(fp_ml_alert)))                             AS ml_listener_precision
  , COUNT(DISTINCT flagged_as_complaint_by_advocate) / NULLIFZERO(COUNT(DISTINCT is_messaging_case)) AS complaints_chat_alert_rate
  , complaints_chat_alert_rate * ml_listener_precision / ml_listener_recall                          AS new_expected_complaint_rate
  , (SUM(fn_ml_alert) + SUM(tp_ml_alert)) / NULLIFZERO(total_reviewed_cases_by_ccot)                 AS new_expected_complaint_rate_v2
  ------- general -------
  , ROW_NUMBER() OVER (ORDER BY case_created_month DESC) = 1                                         AS most_recent_month
FROM base
GROUP BY 1
HAVING
  total_reviewed_cases_by_ccot >= 1000
-- QUALIFY
--   most_recent_month
ORDER BY 1 DESC

;


-- Gives the counts of complaints flagged by Advocate vs CCOT
WITH
  base AS (
    SELECT
      sc.case_id
      , sc.origin
      , CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', case_creation_date) AS case_created_ts_utc
      , DATE_TRUNC(MONTH, case_created_ts_utc)::DATE                       AS case_created_month
      , IFF(((sc.is_flagged_by_advocate OR sc.is_flagged_by_ml_listener)
        AND sc.origin = 'Chat'), sc.case_id, NULL)                         AS flagged_as_complaint_by_advocate
      , IFF(sc.origin = 'Chat', sc.case_id, NULL)                          AS is_messaging_case
      , IFF(ml.has_complaint_ccot = 1, sc.case_id, NULL)                   AS flagged_as_complaint_by_ccot
      , CASE
          WHEN ml.has_complaint_ccot = 1 AND ml.has_complaint_ml = 1
            THEN 1
          ELSE 0
        END                                                                AS tp_ml_alert
      , CASE
          WHEN ml.has_complaint_ccot = 1 AND ml.has_complaint_ml = 0
            THEN 1
          ELSE 0
        END                                                                AS fn_ml_alert
      , CASE
          WHEN ml.has_complaint_ccot = 0 AND ml.has_complaint_ml = 1
            THEN 1
          ELSE 0
        END                                                                AS fp_ml_alert
    FROM app_cash_cs.public.support_cases sc
    JOIN app_cash_beta.app.complaint_comparison_ccot_advocate_ml ml
      ON sc.case_id = ml.case_id
  )

SELECT
  COUNT(IFF(((sc.is_flagged_by_advocate OR sc.is_flagged_by_ml_listener)
    AND sc.origin = 'Chat'), sc.case_id, NULL))             AS flagged_as_complaint_by_advocate
  , COUNT(IFF(ml.has_complaint_ccot = 1, sc.case_id, NULL)) AS flagged_as_complaint_by_ccot
FROM app_cash_cs.public.support_cases sc
JOIN app_cash_beta.app.complaint_comparison_ccot_advocate_ml ml
  ON sc.case_id = ml.case_id