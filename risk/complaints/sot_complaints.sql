WITH
  complaints_base AS (
    SELECT
      c.workflow
      , c.id
      , c.complaint_number
      , c.created_date_utc::TIMESTAMP_NTZ                       AS created_ts_utc
      , c.date_complaint_closed_utc                             AS closed_ts_utc
      , c.created_by_id
      , c.case_id
      , c.case_number
      , c.state                                                 AS customer_state
      , c.country
      , COALESCE(c.business_unit, 'Cash App')                   AS business_unit
      , c.flagged_by
      , c.complaint_review_notes
      , COALESCE(owner.employee_id::VARCHAR, tqc.queue_id)      AS owner_id              -- CF1 COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR QUEUE; IF EMPLOYEE_ID IS NULL, COALESCE GRABS THE QUEUE OWNER_ID
      , COALESCE(owner.full_name, tqc.queue_name)               AS owner_name            -- CF1 COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR QUEUE; IF FULL_NAME IS NULL, COALESCE GRABS THE QUEUE NAME
      , owner.ldap_today                                        AS owner_ldap
      , c.primary_complaint
      , c.escalated_to_legal
      , c.escalated_to_legal_date_utc::TIMESTAMP_NTZ            AS escalated_to_legal_ts_utc
      , c.root_cause_notes
      , c.substantiated

      , COALESCE(modifier.employee_id::VARCHAR, ulm.id)         AS last_modified_by_id   -- CF1 COMPLAINT RECORD CAN BE MODIFIED BY EMPLOYEE OR SUPPORTAL BOT (ONLY OTHER KNOWN VALUE); IF EMPLOYEE_ID IS NULL, COALESCE GRABS THE ID FROM USERS TABLE; CAST NECESSARY SINCE EMPLOYEE_ID IS NUMBER TYPE
      , COALESCE(modifier.full_name, ulm.name)                  AS last_modified_by_name -- CF1 COMPLAINT RECORD CAN BE MODIFIED BY EMPLOYEE OR SUPPORTAL BOT (ONLY OTHER KNOWN VALUE); IF EMPLOYEE_ID IS NULL, COALESCE GRABS THE NAME FROM USERS TABLE
      , c.last_modified_date_utc::TIMESTAMP_NTZ                 AS last_modified_ts_utc
      , c.system_modstamp_utc::TIMESTAMP_NTZ                    AS system_modstamp_ts_utc
      , c.last_activity_date::TIMESTAMP_NTZ                     AS last_activity_ts_utc
      , c.last_viewed_date_utc::TIMESTAMP_NTZ                   AS last_viewed_ts_utc
      , c.last_referenced_date_utc::TIMESTAMP_NTZ               AS last_referenced_ts_utc
      -------------------- xanadu changes --------------------
      , c.case_resolution_type
      , c.intake_channel
      , c.pii_verified
      , c.complaint_status
      , c.customer_token
      , c.date_complaint_acknowledged_utc::TIMESTAMP_NTZ        AS acknowledged_ts_utc
      , c.date_complaint_closed_utc::TIMESTAMP_NTZ              AS closed_ts_utc
      , c.date_complaint_investigated_utc::TIMESTAMP_NTZ        AS investigated_ts_utc
      , c.date_flagged_utc::TIMESTAMP_NTZ                       AS flagged_ts_utc
      , c.date_received::TIMESTAMP_NTZ                          AS received_ts_utc
      , c.date_record_resolved_utc::TIMESTAMP_NTZ               AS resolved_ts_utc
      , c.complaint_description
      , c.investigator_id
      , investigator.ldap_today                                 AS investigator_ldap
      , investigator.full_name                                  AS investigator_name
      , c.is_duplicate
      , c.escalated_to_external_partner
      , c.escalated_to_external_partner_date_utc::TIMESTAMP_NTZ AS escalated_to_external_partner_ts_utc
      , c.external_complaint_number
      , c.legal_tag
      , c.primary_issue
      , c.primary_issue_root_cause
      , c.product_type
      , c.redress_paid
      , c.redress_requested
      , c.is_reopened
      , c.resulting_action
      , c.secondary_issue
      , c.secondary_issue_root_cause
      , c.severity_tier
      , c.is_handled_by_ccot
      , c.date_early_resolution_due::TIMESTAMP_NTZ              AS early_resolution_due_ts_utc
      , c.date_formal_acknowledgement_due::TIMESTAMP_NTZ        AS formal_acknowledgement_due_ts_utc
      , c.date_formal_response_due::TIMESTAMP_NTZ               AS formal_response_due_ts_utc
      , c.agency
      , c.source_type
      , c.portal
      , c.pushback
      , c.rejected_response
      , c.date_response_sent_utc::TIMESTAMP_NTZ                 AS external_response_sent_ts_utc
      , c.date_response_due::TIMESTAMP_NTZ                      AS response_due_ts_utc
    FROM cash_complaints.xanadu_testing.clean_complaints c
    LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents modifier
      ON c.last_modified_by_id = modifier.cfone_id_today
      AND c.date_flagged_utc::DATE BETWEEN modifier.start_date AND modifier.end_date -- FOR CF1 LAST MODIFIED DATE WHEN IT IS EMPLOYEE; COMPLAINT RECORD CAN BE MODIFIED BY EMPLOYEE OR SUPPORTAL BOT (ONLY OTHER KNOWN VALUE)
    LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents owner
      ON c.owner_id = owner.cfone_id_today
      AND c.date_flagged_utc::DATE BETWEEN owner.start_date AND owner.end_date -- FOR CF1 COMPLAINT RECORD OWNER NAME WHEN IT IS EMPLOYEE; COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR A QUEUE
    LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents investigator
      ON c.investigator_id = investigator.cfone_id_today
      AND c.date_flagged_utc::DATE BETWEEN investigator.start_date AND investigator.end_date -- for investigator
    LEFT JOIN app_datamart_cco.sfdc_cfone.clean_user ulm -- user_last_modified
      ON c.last_modified_by_id = ulm.id -- FOR CF1 LAST MODIFIED DATE WHEN IT IS SUPPORTAL BOT OR ANYTHING OTHER THAN EMPLOYEE
    LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
      ON c.owner_id = tqc.queue_id -- FOR CF1 COMPLAINT RECORD OWNER NAME WHEN IT IS QUEUE; COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR A QUEUE
    WHERE
      1 = 1
      AND c.complaint_status IS NOT NULL -- will always be non NULL for post-xanadu records
  )
  , responded_bbb_history AS (
    SELECT
      parent_id
      , field
      , new_value                     AS bbb_responded_ts_utc
      , created_at_utc::TIMESTAMP_NTZ AS bbb_input_responded_ts_utc
    FROM cash_complaints.sfdc_cfone.clean_complaint_history
    WHERE
      field = 'Date_Responded_in_BBB__c'
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY bbb_input_responded_ts_utc DESC) = 1
  )
  , sponsoring_bank AS (
    SELECT DISTINCT
      customer_token
      , sponsoring_bank
    FROM app_cash.app.banklin_cash_card_issuance
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY customer_token ORDER BY created_at DESC) = 1
  )

SELECT
  b.complaint_number
  , b.case_number
  , b.case_id
  , b.cased_ts_utc
  , b.workflow
  , IFF(b.workflow IN ('Regulatory', 'Pre-Litigation', 'BBB'), TRUE, FALSE)            AS is_external_complaint
  , CASE
      WHEN b.workflow = 'Internal'
        THEN NULL
      WHEN b.workflow = 'Regulatory'
        THEN b.agency
      WHEN b.workflow = 'Pre-Litigation'
        THEN TRIM(CONCAT(COALESCE(b.customer_state, ''), ' ', COALESCE(b.source_type, '')))
      WHEN b.workflow = 'BBB'
        THEN b.portal
      ELSE NULL
    END                                                                                AS third_party_source
  , b.business_unit
  , b.created_ts_utc
  , CASE
      WHEN b.workflow = 'Internal'
        THEN DATEADD(DAY, 30, b.received_ts_utc)
      WHEN b.workflow = 'Regulatory'
        THEN b.response_due_ts_utc
      WHEN b.workflow = 'Pre-Litigation'
        THEN b.response_due_ts_utc
      WHEN b.workflow = 'BBB'
        THEN b.response_due_ts_utc
      ELSE NULL
    END                                                                                AS response_due_ts_utc
  , CASE
      WHEN b.workflow = 'Internal'
        THEN NULL
      WHEN b.workflow = 'Regulatory'
        THEN b.external_response_sent_ts_utc
      WHEN b.workflow = 'Pre-Litigation'
        THEN b.external_response_sent_ts_utc
      WHEN b.workflow = 'BBB'
        THEN rbh.bbb_responded_ts_utc
      ELSE NULL
    END                                                                                AS external_response_sent_ts_utc
  , CASE
      WHEN b.workflow = 'Internal'
        THEN 15
      WHEN b.workflow = 'Regulatory'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.response_due_ts_utc)
      WHEN b.workflow = 'Pre-Litigation'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.response_due_ts_utc)
      WHEN b.workflow = 'BBB'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.response_due_ts_utc)
      ELSE NULL
    END                                                                                AS formal_response_sla_threshold
  , CASE
      WHEN b.workflow = 'Internal'
        THEN 6
      WHEN b.workflow = 'Regulatory'
        THEN NULL
      WHEN b.workflow = 'Pre-Litigation'
        THEN NULL
      WHEN b.workflow = 'BBB'
        THEN NULL
      ELSE NULL
    END                                                                                AS acknowledgement_sla_threshold
  , CASE
      WHEN b.workflow = 'Internal'
        THEN 5
      WHEN b.workflow = 'Regulatory'
        THEN NULL
      WHEN b.workflow = 'Pre-Litigation'
        THEN NULL
      WHEN b.workflow = 'BBB'
        THEN NULL
      ELSE NULL
    END                                                                                AS early_resolution_sla_threshold
  , CASE
      WHEN b.workflow = 'Internal'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.acknowledged_ts_utc)
      WHEN b.workflow = 'Regulatory'
        THEN NULL
      WHEN b.workflow = 'Pre-Litigation'
        THEN NULL
      WHEN b.workflow = 'BBB'
        THEN NULL
      ELSE NULL
    END                                                                                AS time_to_acknowledge_business_days
  , CASE
      WHEN b.workflow = 'Internal'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.closed_ts_utc)
      WHEN b.workflow = 'Regulatory'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.closed_ts_utc)
      WHEN b.workflow = 'Pre-Litigation'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.closed_ts_utc)
      WHEN b.workflow = 'BBB'
        THEN app_cash_cs.public.business_days_between(b.received_ts_utc, b.closed_ts_utc)
      ELSE NULL
    END                                                                                AS time_to_closure_business_days
  , CASE
      WHEN b.workflow = 'Internal'
        THEN time_to_closure_business_days <= formal_response_sla_threshold
      WHEN b.workflow = 'Regulatory'
        THEN time_to_closure_business_days <= formal_response_sla_threshold
      WHEN b.workflow = 'Pre-Litigation'
        THEN time_to_closure_business_days <= formal_response_sla_threshold
      WHEN b.workflow = 'BBB'
        THEN time_to_closure_business_days <= formal_response_sla_threshold
      ELSE FALSE
    END                                                                                AS is_compliant_informal_response_sla
  , CASE
      WHEN b.workflow = 'Internal'
        THEN COALESCE(time_to_acknowledge_business_days <= acknowledgement_sla_threshold, FALSE)
      WHEN b.workflow = 'Regulatory'
        THEN NULL
      WHEN b.workflow = 'Pre-Litigation'
        THEN NULL
      WHEN b.workflow = 'BBB'
        THEN NULL
      ELSE NULL
    END                                                                                AS is_compliant_in_acknowledgement_sla
  , CASE
      WHEN b.workflow = 'Internal'
        THEN COALESCE(time_to_closure_business_days <= early_resolution_sla_threshold, FALSE)
      WHEN b.workflow = 'Regulatory'
        THEN NULL
      WHEN b.workflow = 'Pre-Litigation'
        THEN NULL
      WHEN b.workflow = 'BBB'
        THEN NULL
      ELSE NULL
    END                                                                                AS is_compliant_in_early_resolution_sla
  , CASE
      WHEN b.workflow = 'Internal'
        THEN DATEDIFF(DAY, b.created_ts_utc, b.resolved_ts_utc)
      WHEN b.workflow = 'Regulatory'
        THEN DATEDIFF(DAY, b.received_ts_utc, b.investigated_ts_utc)
      WHEN b.workflow = 'Pre-Litigation'
        THEN DATEDIFF(DAY, b.received_ts_utc, b.investigated_ts_utc)
      WHEN b.workflow = 'BBB'
        THEN DATEDIFF(DAY, b.received_ts_utc, rbh.bbb_responded_ts_utc)
      ELSE NULL
    END                                                                                AS cert_investigation_time
  , b.customer_state
  , b.country
  , b.escalated_to_legal
  , b.escalated_to_external_partner_ts_utc
  , b.root_cause_notes
  , b.complaint_review_notes
  , b.primary_complaint
  , CASE
      WHEN b.workflow = 'Internal'
        THEN FALSE
      WHEN b.workflow = 'Regulatory'
        THEN b.pushback
      WHEN b.workflow = 'Pre-Litigation'
        THEN b.pushback
      WHEN b.workflow = 'BBB'
        THEN b.rejected_response
      ELSE NULL
    END                                                                                AS pushback
  , CONCAT('https://cf1.lightning.force.com/lightning/r/Complaint__c/', b.id, '/view') AS complaint_investigation_link
  , b.case_resolution_type
  , b.intake_channel
  , b.pii_verified
  , b.complaint_status
  , b.customer_token
  , b.acknowledged_ts_utc
  , b.closed_ts_utc
  , b.investigated_ts_utc
  , b.flagged_ts_utc
  , b.received_ts_utc
  , b.resolved_ts_utc
  , b.complaint_description
  , b.investigator_id
  , b.investigator_ldap
  , b.investigator_name
  , b.is_duplicate
  , b.escalated_to_external_partner
  , b.escalated_to_external_partner_ts_utc
  , b.external_complaint_number
  , b.legal_tag
  , b.primary_issue
  , b.primary_issue_root_cause
  , b.product_type
  , b.redress_paid
  , b.redress_requested
  , b.is_reopened
  , b.resulting_action
  , b.secondary_issue
  , b.secondary_issue_root_cause
  , b.severity_tier
  , b.is_handled_by_ccot
  , IFF(sa.dependent_customer_token IS NOT NULL, TRUE, FALSE)                          AS is_dependent_account
  , sb.sponsoring_bank                                                                 AS third_party_provider
FROM complaints_base b
LEFT JOIN responded_bbb_history rbh
  ON b.id = rbh.parent_id
  AND b.created_ts_utc::DATE >= '2021-01-04'
LEFT JOIN app_cash.app.sponsored_accounts sa --for identifying teen accounts:is_dependent_account
  ON b.customer_token = sa.dependent_customer_token
LEFT JOIN sponsoring_bank sb
  ON b.customer_token = sb.customer_token -- for identifying third party provider
ORDER BY created_ts_utc DESC
LIMIT 100
;