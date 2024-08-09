-- Change Log
--

CREATE OR REPLACE TABLE personal_johndarrah.public.complaint_ml_alerting_sev AS
  SELECT
    c.message_token
    , c.message_at_utc
    , p.conversation_token
    , p.detected_at
    -- update the sev label here
    --------------------- start
    , CASE
        WHEN c.message_at_utc BETWEEN '2024-07-18 13:50:00' AND '2024-07-18 17:30:00'
          THEN 'cash-inc-5039_lots-of-risk-evaluations-failing'
        WHEN c.message_at_utc BETWEEN '2024-08-01 00:25:00' AND '2024-08-01 01:52:00'
          THEN 'cash-inc-5090_events-to-supportal_ml_chat_events-not-being-published'
        ELSE NULL
      END                                                                   AS sev_label
    , CASE
        WHEN c.message_at_utc BETWEEN '2024-07-18 13:50:00' AND '2024-07-18 17:30:00'
          THEN '2024-07-18 13:50:00'::TIMESTAMP_NTZ
        WHEN c.message_at_utc BETWEEN '2024-08-01 00:25:00' AND '2024-08-01 01:52:00'
          THEN '2024-08-01 00:25:00'::TIMESTAMP_NTZ
        ELSE NULL
      END                                                                   AS sev_start_utc
    , CASE
        WHEN c.message_at_utc BETWEEN '2024-07-18 13:50:00' AND '2024-07-18 17:30:00'
          THEN '2024-07-18 17:30:00'::TIMESTAMP_NTZ
        WHEN c.message_at_utc BETWEEN '2024-08-01 00:25:00' AND '2024-08-01 01:52:00'
          THEN '2024-08-01 01:52:00'::TIMESTAMP_NTZ
        ELSE NULL
      END                                                                   AS sev_end_utc
    , app_cash_cs.public.business_days_between(p.detected_at, CURRENT_DATE) AS complaint_age
    , CURRENT_TIMESTAMP                                                     AS etl_run_date_utc
  --------------------- end
  FROM app_cash_beta.support_chat.support_chat_message_level c
  JOIN cash_data_bot.public.cash_support_chat_complaint_prospects p
    ON c.message_token = p.chat_message_token
  WHERE
    1 = 1
    AND c.sender = 'CUSTOMER'
    AND sev_label IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY p.conversation_token ORDER BY p.detected_at) = 1
;


-- grants
GRANT ALL ON DATABASE personal_johndarrah TO ROLE app_cash_cs__snowflake__read_only
;

GRANT ALL ON SCHEMA personal_johndarrah.public TO ROLE app_cash_cs__snowflake__read_only
;

GRANT ALL ON TABLE personal_johndarrah.public.complaint_ml_alerting_sev TO ROLE app_cash_cs__snowflake__read_only
;