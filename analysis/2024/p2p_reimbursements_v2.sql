CREATE OR REPLACE TABLE personal_johndarrah.public.p2p_reimbursements_v2 AS
 SELECT
   ps.payment_id
   , ps.recipient_token                           AS customer_token
   , 'P2P Reimbursement'                          AS type
   , ps.created_at                                AS payment_created_at
   , ps.amount_usd                                AS payment_amount_usd
   , ps.recipient_token                           AS payment_recipient_token
   , ps.sender_token                              AS payment_sender_token
   , ps.initiator_notes                           AS payment_initiator_notes
   , ps.creation_mechanism                        AS payment_creation_mechanism
   , ps.state                                     AS payment_state
   , ps.push_transaction_token                    AS payment_push_transaction_token
   , ps.pull_transaction_token                    AS payment_pull_transaction_token
   , ps.payment_reference
   , ps.failed_at                                 AS payment_failed_at
   , ps.failure_reason                            AS payment_failure_reason
   , ps.network_product                           AS payment_network_product
   , ps.pull_state                                AS payment_pull_state
   , ps.pull_result                               AS payment_pull_result
   , ps.pull_instrument_type                      AS payment_pull_instrument_type
   , ps.external_id                               AS payment_external_id
   , ps.orientation                               AS payment_orientation
   , al.id                                        AS regulatory_audit_log_id
   , al.created_at                                AS regulator_created_at
   , al.reason                                    AS regulator_comment
   , al.action_name                               AS regulator_action_name
   , al.actor_uid
   , e.cfone_id_today
   , e.ldap_today
   , e.team_code
   , sc.case_number
   , sc.case_id
   , sc.created_date_utc
   , ff.banking_transaction_token                 AS case_banking_transaction_token
   , sc.disputron_description                     AS case_disputron_description
   , sc.last_assigned_queue_name                  AS case_last_assigned_queue
   , ff.payment_id                                AS case_payment_id
   , sc.origin                                    AS case_origin
   , COUNT(ps.payment_id)
           OVER (PARTITION BY ps.recipient_token) AS total_reimburesents_per_customer
   , n.queue                                      AS notary_queue
   , n.occurred_at                                AS notary_occurred_at
   , n.submitted_at                               AS notary_submitted
   , n.creator                                    AS notary_creator
   , n.user_id                                    AS notary_user_id
   , n.full_name                                  AS notary_full_name
   , n.team_code                                  AS notary_team_code
   , n.status                                     AS notary_status
   , sc2.case_number                              AS case_datediff_case_number
   , sc2.case_id                                  AS case_datediff_case_id
   , sc2.created_date_utc                         AS case_datediff_created_date_utc
   , sc2.disputron_description                    AS case_datediff_disputron_description
   , sc2.last_assigned_queue_name                 AS case_datediff_last_assigned_queue_name
   , sc2.origin                                   AS case_datediff_origin
   , ff2.case_id                                  AS case_payment_id_case_id
   , sc.case_id IS NOT NULL                       AS has_cf1_case_from_reg
   , n.submitted_at IS NOT NULL                   AS has_notary_case
   , sc2.case_id IS NOT NULL                      AS has_cf1_case_from_3_day_diff
   , ff2.case_id IS NOT NULL                      AS has_payment_id_on_cf1_case
   , al.id IS NOT NULL                            AS has_regulator_case
 FROM app_cash.app.payment_summary ps
 LEFT JOIN regauditlogs.raw_oltp.audit_logs al
   ON ps.recipient_token = al.entity_token
   AND al.action_name ILIKE 'franklin::reimburse%'
   AND ps.created_at BETWEEN DATEADD(DAY, -1, al.created_at) AND DATEADD(DAY, 1, al.created_at) -- experimenting with the time window + or - a day
 LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents e
   ON al.actor_uid = e.uid
   AND ps.created_at BETWEEN e.start_date AND e.end_date
 LEFT JOIN app_datamart_cco.public.cash_support_cases_wide sc
   ON REGEXP_SUBSTR(al.reason, '[0-9]{8}([0-9]{1})?')::VARCHAR = sc.case_number
 LEFT JOIN app_cash_cs.preprod.franklin_flow ff
   ON sc.case_id = ff.case_id
 LEFT JOIN app_cash_cs.public.notary_assignments_queue n
   ON ps.recipient_token = n.customer_token
   AND ps.created_at::DATE = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', n.occurred_at)::DATE
 LEFT JOIN app_datamart_cco.public.cash_support_cases_wide sc2
   ON ps.recipient_token = sc2.customer_token
   AND sc2.created_date_utc BETWEEN DATEADD(DAY, -3, ps.created_at) AND ps.created_at
 LEFT JOIN app_cash_cs.preprod.franklin_flow ff2
   ON ps.payment_id = ff2.payment_id
 WHERE
   1 = 1
   AND ps.creation_mechanism = 'REIMBURSEMENT'
   AND ps.sender_token = 'C_hmd8xayr1'
   AND ps.created_at::DATE >= '2024-04-01'
   AND ps.state = 'PAID_OUT'
 QUALIFY
   ROW_NUMBER() OVER (PARTITION BY ps.payment_id ORDER BY ff.banking_transaction_token IS NULL DESC, al.created_at) = 1
;


GRANT ALL ON DATABASE personal_johndarrah TO ROLE app_cash_cs__snowflake__admin
;


GRANT ALL ON SCHEMA personal_johndarrah.public TO ROLE app_cash_cs__snowflake__read_only
;


GRANT ALL ON TABLE personal_johndarrah.public.p2p_reimbursements_v2 TO ROLE app_cash_cs__snowflake__read_only
;



SELECT
  has_cf1_case_from_reg
  , has_notary_case
  , has_cf1_case_from_3_day_diff
  , has_payment_id_on_cf1_case
  , has_regulator_case
  , COUNT(*)
FROM personal_johndarrah.public.test
GROUP BY 1, 2, 3, 4, 5

;


SELECT *
FROM personal_johndarrah.public.p2p_reimbursements_v2
WHERE
  1 = 1
  AND NOT has_cf1_case_from_reg
  AND NOT has_notary_case
  AND NOT has_cf1_case_from_3_day_diff
  AND NOT has_payment_id_on_cf1_case
  AND NOT has_regulator_case