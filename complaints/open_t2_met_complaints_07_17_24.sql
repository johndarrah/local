CREATE OR REPLACE TABLE personal_johndarrah.public.open_t2_met_complaints_07_17_24 AS
  WITH
    complaints_first_met_specialist AS (
      SELECT
        ch.case_id
        , ch.created_at_utc                                                         AS first_met_assigned_at_utc
        , e.full_name                                                               AS met_full_name
        , e.team_code                                                               AS met_team_code
        , COALESCE(c.initial_resolved_date_time_utc, c.last_resolved_date_time_utc) AS first_resolved_ts_utc
        , COALESCE(ch.created_at_utc > first_resolved_ts_utc, FALSE)                AS is_complaint_resolved_before_met_escalation
      FROM app_datamart_cco.sfdc_cfone.clean_case_history_owners ch
      JOIN app_datamart_cco.workday.cs_employees_and_agents e
        ON ch.new_value = e.cfone_id_today
        AND ch.created_at_utc BETWEEN e.start_date AND e.end_date
        AND e.team_code ILIKE ANY ('%met%', '%eret%')
      JOIN app_datamart_cco.public.cash_support_cases_wide c
        ON ch.case_id = c.case_id
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY ch.case_id ORDER BY ch.created_at_utc ASC ) = 1
    )
    , complaints_manager_escalation_queue AS (
      SELECT
        ch.case_id
        , ch.created_at_utc                                                         AS entered_escalation_queue_at_utc
        , COALESCE(c.initial_resolved_date_time_utc, c.last_resolved_date_time_utc) AS first_resolved_ts_utc
        , COALESCE(entered_escalation_queue_at_utc > first_resolved_ts_utc, FALSE)  AS is_complaint_resolved_before_escalation_queue
      FROM app_datamart_cco.sfdc_cfone.clean_case_history_owners ch
      JOIN app_datamart_cco.public.cash_support_cases_wide c
        ON ch.case_id = c.case_id
      WHERE
        ch.new_value = 'CS Email Manager Escalations'
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY ch.case_id ORDER BY ch.created_at_utc ASC ) = 1
    )
    , complaint_met_email_actions AS (
      SELECT
        em.case_id
        , em.email_created_at AS last_email_sent_ts_utc
        , e.cfone_id_today    AS sender_cfone_id
        , e.full_name         AS sender_full_name
        , e.team_code         AS sender_team_code
      FROM app_cash_cs.cfone_classic.email_message em
      JOIN app_datamart_cco.workday.cs_employees_and_agents e
        ON em.created_by_id = e.cfone_id_today
        AND em.email_created_at BETWEEN e.start_date AND e.end_date
      WHERE
        1 = 1
        AND direction = 'outgoing'
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY em.case_id ORDER BY e.team_code ILIKE ANY ('%met%', '%eret%') DESC,em.email_created_at DESC) = 1
    )
  SELECT
    CASE
      WHEN (c.is_handled_by_ccot)
        THEN 'No Action Required: Handled by CCOT'
      WHEN (((IFF(complaint_met_email_actions.sender_team_code ILIKE ANY ('%met%', '%eret%'), TRUE, FALSE)) = 'TRUE')
        AND (sc.status IN ('Closed', 'Resolved'))) AND (NOT ((c.resulting_action) IS NULL))
        THEN 'Bulk Update Status to Closed: Resolved case with MET email response & Resulting Action'
      WHEN ((IFF(complaint_met_email_actions.sender_team_code ILIKE ANY ('%met%', '%eret%'), TRUE, FALSE)) = 'TRUE')
        AND (sc.status IN ('Closed', 'Resolved'))
        THEN 'Bulk Update Status to Closed: Resolved case with MET email response'
      WHEN ((IFF(complaint_met_email_actions.sender_team_code ILIKE ANY ('%met%', '%eret%'), TRUE, FALSE)) = 'TRUE')
        AND (NOT ((sc.status IN ('Closed', 'Resolved'))))
        THEN 'TBD: Unresolved case with MET email response'
      WHEN ((IFF(complaint_met_email_actions.sender_team_code IS NOT NULL, TRUE, FALSE)) = 'TRUE') AND (sc.status IN ('Closed', 'Resolved'))
        THEN 'TBD: Resolved case with email response'
      WHEN (NOT (complaints_first_met_specialist.is_complaint_resolved_before_met_escalation IS NULL)) AND (sc.status IN ('Closed', 'Resolved'))
        THEN 'TBD: Resolved case with MET Ownership'
      WHEN (sc.status IN ('Closed', 'Resolved')) AND (c.closed_ts_utc IS NOT NULL)
        THEN 'Requires Review: Complaint has closed date and case is resolved'
      WHEN complaints_manager_escalation_queue.is_complaint_resolved_before_escalation_queue = 'TRUE'
        THEN 'Requires Review: Case Resolved Before Escalation Queue'
      ELSE 'Requires Review: No Bucket'
    END                                                                            AS remediation_bucket
    , sc.case_number
    , sc.case_id
    , sc.case_creation_date_time_utc
    , 'https://cf1.lightning.force.com/lightning/r/Case/' || sc.case_id || '/view' AS cf1_link
    , c.complaint_number
    , c.complaint_id
    , c.complaint_status
    , c.created_ts_utc                                                             AS complaint_created_ts_utc
    , b.complaint_review_notes
  FROM app_datamart_cco.public.complaints_base_public AS c
  JOIN cash_complaints.public.complaints_base b
    ON c.complaint_id = b.complaint_id
  LEFT JOIN app_datamart_cco.public.cash_support_cases_wide AS sc
    ON (c.case_id) = sc.case_id
  LEFT JOIN complaints_first_met_specialist
    ON (c.case_id) = complaints_first_met_specialist.case_id
  LEFT JOIN complaints_manager_escalation_queue
    ON (c.case_id) = complaints_manager_escalation_queue.case_id
  LEFT JOIN complaint_met_email_actions
    ON (c.case_id) = complaint_met_email_actions.case_id
  WHERE
    (c.complaint_status) = 'In Progress'
    AND (c.severity_tier) = 'Tier 2'
    AND (CASE
           WHEN NOT c.is_duplicate
             AND (c.workflow) NOT IN ('Lending', 'Investing', 'Global')
             AND (c.complaint_status) NOT IN ('Inquiry (Not a Complaint)', 'Withdrawn')
             AND (c.business_unit) = 'Cash App' --Excludes complaints for other business units (reported separately)
             AND NVL((c.third_party_source), '') NOT ILIKE '%investigation%'-- Excludes 'Investigation Only' Pre-Litigation complaints
             AND COALESCE((c.country), 'United States') IN ('United States', 'Unknown')
             THEN TRUE
           ELSE FALSE
         END)
;

-- grants
GRANT ALL ON DATABASE personal_johndarrah TO ROLE app_cash_cs__snowflake__read_only
;

GRANT ALL ON SCHEMA personal_johndarrah.public TO ROLE app_cash_cs__snowflake__read_only
;

GRANT ALL ON TABLE personal_johndarrah.public.open_t2_met_complaints_07_17_24 TO ROLE app_cash_cs__snowflake__read_only
;

-- test table
SELECT DISTINCT
  complaint_review_notes
FROM personal_johndarrah.public.open_t2_met_complaints_07_17_24
;