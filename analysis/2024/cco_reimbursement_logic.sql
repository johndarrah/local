CREATE OR REPLACE TABLE personal_johndarrah.public.p2p_reimbursements AS
  SELECT
    ps.payment_id
    , ps.recipient_token                           AS customer_token
    , 'p2p_reimbursement'                          AS type
    , ps.created_at                                AS payment_created_at
    , ps.amount_usd                                AS payment_amount_usd
    , ps.sender_token                              AS payment_sender_token
    , ps.initiator_notes                           AS payment_initiator_notes
    , ps.creation_mechanism                        AS payment_creation_mechanism
    , ps.state                                     AS payment_state
    , ps.push_transaction_token                    AS payment_push_transaction_token
    , ps.pull_transaction_token                    AS payment_pull_transaction_token
    , al.id                                        AS regulatory_audit_log_id
    , al.created_at                                AS regulator_created_at
    , al.comment                                   AS regulator_comment
    , al.action_name                               AS regulator_action_name
    , al.actor_uid
    , e.cfone_id_today
    , e.ldap_today
    , e.team_code
    , sc.case_number
    , sc.case_id
    , sc.case_creation_date_time
    , sc.banking_transaction_token                 AS case_banking_transaction_token
    , sc.disputron_description                     AS case_disputron_description
    , sc.last_assigned_queue                       AS case_last_assigned_queue
    , sc.payment_id                                AS case_payment_id
    , sc.origin                                    AS case_origin
    , COUNT(ps.payment_id)
            OVER (PARTITION BY ps.recipient_token) AS total_reimburesents_per_customer
  FROM app_cash.app.payment_summary ps
  LEFT JOIN regulator.wd_mysql_regulator_001__regulator_production.audit_logs al
    ON ps.recipient_token = al.target_token
    AND al.action_name ILIKE 'franklin::reimburse%'
    AND DATE_TRUNC(SECONDS, ps.created_at) = DATE_TRUNC(SECONDS, al.created_at)
  LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents e
    ON al.actor_uid = e.uid
    AND ps.created_at BETWEEN e.start_date AND e.end_date
  LEFT JOIN app_cash_cs.public.support_cases sc
    ON REGEXP_SUBSTR(al.comment, '[0-9]{8}([0-9]{1})?')::VARCHAR = sc.case_number
  WHERE
    1 = 1
    AND ps.creation_mechanism = 'REIMBURSEMENT'
    AND ps.sender_token = 'C_hmd8xayr1'
    AND ps.created_at::DATE >= '2023-07-01'
    AND ps.state = 'PAID_OUT'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY ps.payment_id ORDER BY sc.banking_transaction_token IS NULL DESC, al.created_at) = 1
;

GRANT ALL ON DATABASE personal_johndarrah TO ROLE app_cash_cs__snowflake__admin
;

GRANT ALL ON SCHEMA personal_johndarrah.public TO ROLE app_cash_cs__snowflake__read_only
;

GRANT ALL ON TABLE personal_johndarrah.public.p2p_reimbursements TO ROLE app_cash_cs__snowflake__read_only
;
