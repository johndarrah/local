SELECT
  c.workflow
  , c.id
  , c.complaint_number
  , c.created_date_utc::TIMESTAMP_NTZ                       AS created_ts_utc
  , c.created_by_id
  , c.case_id
  , c.case_number
  , c.state
  , c.country
  , c.business_unit
  , c.flagged_by
  , c.complaint_review_notes
  , COALESCE(ecd_o.employee_id::VARCHAR, q.queue_id)        AS owner_id              -- CF1 COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR QUEUE; IF EMPLOYEE_ID IS NULL, COALESCE GRABS THE QUEUE OWNER_ID; CAST NECESSARY SINCE EMPLOYEE_ID IS NUMBER TYPE
  , COALESCE(ecd_o.full_name, q.queue_name)                 AS owner_name            -- CF1 COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR QUEUE; IF FULL_NAME IS NULL, COALESCE GRABS THE QUEUE NAME
  , ecd_o.ldap_today                                        AS owner_ldap
  , datetime_owned::TIMESTAMP_NTZ                           AS owned_ts_utc
  , c.primary_complaint
  , c.escalated_to_legal
  , c.escalated_to_legal_date_utc::TIMESTAMP_NTZ            AS escalated_to_legal_ts_utc
  , c.root_cause_notes
  , c.substantiated
  , crh.resolved_by_name
  , crh.resolved_by_ldap
  , COALESCE(ecd_lm.employee_id::VARCHAR, ulm.id)           AS last_modified_by_id   -- CF1 COMPLAINT RECORD CAN BE MODIFIED BY EMPLOYEE OR SUPPORTAL BOT (ONLY OTHER KNOWN VALUE); IF EMPLOYEE_ID IS NULL, COALESCE GRABS THE ID FROM USERS TABLE; CAST NECESSARY SINCE EMPLOYEE_ID IS NUMBER TYPE
  , COALESCE(ecd_lm.full_name, ulm.name)                    AS last_modified_by_name -- CF1 COMPLAINT RECORD CAN BE MODIFIED BY EMPLOYEE OR SUPPORTAL BOT (ONLY OTHER KNOWN VALUE); IF EMPLOYEE_ID IS NULL, COALESCE GRABS THE NAME FROM USERS TABLE
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
  , c.date_complaint_acknowledged_utc::TIMESTAMP_NTZ        AS complaint_acknowledged_ts_utc
  , c.date_complaint_closed_utc::TIMESTAMP_NTZ              AS complaint_closed_ts_utc
  , c.date_complaint_investigated_utc::TIMESTAMP_NTZ        AS complaint_investigated_ts_utc
  , c.date_flagged_utc::TIMESTAMP_NTZ                       AS complaint_flagged_ts_utc
  , c.date_received::TIMESTAMP_NTZ                          AS complaint_received_ts_utc
  , c.date_record_resolved_utc::TIMESTAMP_NTZ               AS complaint_record_resolved_ts_utc
  , c.complaint_description
  , c.investigator_id
  , ecd_inv.ldap_today                                      AS investigator_ldap
  , ecd_inv.full_name                                       AS investigator_name
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
FROM cash_complaints.xanadu_testing.clean_complaints c
  ---------------------------------------- TABLE JOINS ----------------------------------------
  -- LEFT JOIN app_datamart_cco.sfdc_cfone.clean_record_type rt
  --   ON c.record_type_id = rt.id -- FOR RECORD TYPE NAME, NECESSARY TO IDENTIFY TYPE OF COMPLAINT (CASH APP, INVESTING, BORROW)
LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents ecd_lm
  ON (c.last_modified_by_id = ecd_lm.cfone_id_today
  AND TO_DATE(c.date_flagged_utc) BETWEEN ecd_lm.start_date AND ecd_lm.end_date) -- FOR CF1 LAST MODIFIED DATE WHEN IT IS EMPLOYEE; COMPLAINT RECORD CAN BE MODIFIED BY EMPLOYEE OR SUPPORTAL BOT (ONLY OTHER KNOWN VALUE)
LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents ecd_o
  ON c.owner_id = ecd_o.cfone_id_today
  AND TO_DATE(c.date_flagged_utc) BETWEEN ecd_o.start_date AND ecd_o.end_date -- FOR CF1 COMPLAINT RECORD OWNER NAME WHEN IT IS EMPLOYEE; COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR A QUEUE
LEFT JOIN app_datamart_cco.workday.cs_employees_and_agents ecd_inv
  ON c.investigator_id = ecd_inv.cfone_id_today
  AND TO_DATE(c.date_flagged_utc) BETWEEN ecd_inv.start_date AND ecd_inv.end_date -- for investigator
LEFT JOIN app_datamart_cco.sfdc_cfone.clean_user ulm
  ON c.last_modified_by_id = ulm.id -- FOR CF1 LAST MODIFIED DATE WHEN IT IS SUPPORTAL BOT OR ANYTHING OTHER THAN EMPLOYEE
LEFT JOIN app_datamart_cco.archive.team_queue_catalog_all_sfdc q
  ON c.owner_id = q.queue_id -- FOR CF1 COMPLAINT RECORD OWNER NAME WHEN IT IS QUEUE; COMPLAINT RECORD CAN BE OWNED BY EMPLOYEE OR A QUEUE
  ---------------------------------------- CTE JOINS ----------------------------------------
LEFT JOIN cash_complaints.xanadu_testing.resolved_complaint_record_history crh
  ON c.id = crh.parent_id
LEFT JOIN cash_complaints.xanadu_testing.owner_history oh
  ON c.id = oh.parent_id AND ecd_o.full_name = oh.owner_name_history
WHERE
  1 = 1
  AND c.complaint_status IS NOT NULL -- will always be non NULL for post-xanadu records

;

DESCRIBE TABLE cash_complaints.xanadu_testing.clean_complaints