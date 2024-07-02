CREATE OR REPLACE TABLE app_datamart_cco.public.complaint_ml_metrics AS
  WITH
    ccot_reviews AS (
      SELECT
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created_date_utc::TIMESTAMP_NTZ) AS created_ds_pst
        , DATE_TRUNC(MONTH, created_ds_pst)::DATE                                       AS created_month_pst
        , IFF(ml.has_complaint_ccot = 1, sc.case_id, NULL)                              AS flagged_as_complaint_by_ccot
        , IFF(ml.case_id IS NOT NULL, sc.case_id, NULL)                                 AS is_reviewed_by_ccot
        , CASE
            WHEN ml.has_complaint_ccot = 1 AND ml.has_complaint_ml = 1
              THEN 1
            ELSE 0
          END                                                                           AS tp_ml_alert
        , CASE
            WHEN ml.has_complaint_ccot = 1 AND ml.has_complaint_ml = 0
              THEN 1
            ELSE 0
          END                                                                           AS fn_ml_alert
        , CASE
            WHEN ml.has_complaint_ccot = 0 AND ml.has_complaint_ml = 1
              THEN 1
            ELSE 0
          END                                                                           AS fp_ml_alert
      FROM app_datamart_cco.public.cash_support_cases_wide sc
      LEFT JOIN app_cash_beta.app.complaint_comparison_ccot_advocate_ml ml
        ON sc.case_id = ml.case_id
      WHERE
        created_month_pst >= '2023-10-01'
    )
    , ml_alerting_rate AS (
      SELECT
        DATE_TRUNC(MONTH, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.created_date_utc::TIMESTAMP_NTZ))::DATE AS created_month_pst
        , ROUND(COUNT(DISTINCT IFF(sc.is_flagged_by_ml_listener, case_id, NULL))
                  / NULLIFZERO(COUNT(DISTINCT IFF(origin = 'Chat', case_id, NULL))), 4)                             AS ml_alerting_rate
      FROM app_datamart_cco.public.cash_support_cases_wide sc
      WHERE
        1 = 1
        AND sc.created_date_utc::DATE >= '2024-04-01'
      GROUP BY 1
    )
    , advocate_flagging_rate AS (
      SELECT
        DATE_TRUNC(MONTH, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.created_date_utc::TIMESTAMP_NTZ))::DATE AS created_month_pst
        , COUNT(DISTINCT c.case_id)                                                                                 AS total_complaints
        , COUNT(DISTINCT sc.case_id)                                                                                AS total_cases
        , total_complaints / NULLIFZERO(total_cases)                                                                AS complaint_messaging_flagging_rate
        , SUM(total_complaints)
              OVER (ORDER BY created_month_pst ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )
          / NULLIFZERO(SUM(total_cases)
                           OVER (ORDER BY created_month_pst ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ))             AS complaint_messaging_flagging_rate_trailing_3m
      FROM app_datamart_cco.public.cash_support_cases_wide sc
      LEFT JOIN app_datamart_cco.public.complaints_base_public c
        ON sc.case_id = c.case_id
        AND workflow = 'Internal'
      WHERE
        1 = 1
        AND sc.created_date_utc::DATE >= '2024-04-01'
      GROUP BY 1
      ORDER BY 1 DESC
    )
  SELECT
    c.created_month_pst
    ------- expected complaint rate using CCOT reviewed cases -------
    , COUNT(DISTINCT flagged_as_complaint_by_ccot)                                                           AS total_cases_with_complaints
    , COUNT(DISTINCT is_reviewed_by_ccot)                                                                    AS total_reviewed_cases_by_ccot
    , ROUND(total_cases_with_complaints / NULLIFZERO(total_reviewed_cases_by_ccot), 4)                       AS ccot_expected_complaint_rate

    ------- ML Metrics -------
    -- Among the verified complaints, what fraction is being flagged? This is also known as the capture rate.
    , SUM(tp_ml_alert) / NULLIFZERO((SUM(tp_ml_alert) + SUM(fn_ml_alert)))                                   AS ml_listener_recall
    -- When the model issues an alert, how often is it correct? This is also known as accuracy.
    , SUM(tp_ml_alert) / NULLIFZERO((SUM(tp_ml_alert) + SUM(fp_ml_alert)))                                   AS ml_listener_precision
    , MAX(m.ml_alerting_rate)                                                                                AS ml_alerting_rate
    , ROUND(MAX(m.ml_alerting_rate) * ml_listener_precision / NULLIFZERO(ml_listener_recall), 4)             AS ml_expected_complaint_rate

    -- complaint flagging rate
    , MAX(a.complaint_messaging_flagging_rate)                                                               AS complaint_messaging_flagging_rate
    , MAX(a.complaint_messaging_flagging_rate) / NULLIFZERO(ccot_expected_complaint_rate)                    AS ccot_complaint_identification_rate
    , MAX(a.complaint_messaging_flagging_rate) / NULLIFZERO(ml_expected_complaint_rate)                      AS ml_complaint_identification_rate
    ------- general -------
    , ROW_NUMBER() OVER (ORDER BY ml_expected_complaint_rate IS NOT NULL DESC, c.created_month_pst DESC) = 1 AS most_recent_valid_month
  FROM ccot_reviews c
  LEFT JOIN ml_alerting_rate m
    ON c.created_month_pst = m.created_month_pst
  LEFT JOIN advocate_flagging_rate a
    ON m.created_month_pst = a.created_month_pst
  WHERE
    1 = 1
  -- and m.created_month_pst >= '2024-04-01'
  GROUP BY 1
  ORDER BY 1 DESC
;

SELECT *
FROM app_datamart_cco.public.complaint_ml_metrics

;

USE WAREHOUSE adhoc__xlarge
;

SELECT CURRENT_WAREHOUSE()
;

DROP TABLE app_datamart_cco.public.complaint_rates