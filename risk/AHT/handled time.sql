SELECT
  parent_id
  , COUNT(*) AS handled_count
FROM cash_complaints.sfdc_cfone.clean_complaint_history h
WHERE
  field = 'Complaint_Status__c'
  AND parent_id = 'a1m5w000006tRYOAA2'
  AND new_value IN ('Closed',
                    'Inquiry (Not a Complaint)',
                    'Archived',
                    'Withdrawn')
GROUP BY 1
-- QUALIFY
-- COUNT(*) OVER (PARTITION BY parent_id) > 4
-- ORDER BY parent_id, status_change_ts_utc
;

WITH
  base AS (
    -- when advocate first takes ownership
    SELECT
      parent_id
      , field
      , old_value
      , new_value
      , created_at_utc::TIMESTAMP_NTZ AS ts
      , FALSE                         AS is_touch
    FROM cash_complaints.sfdc_cfone.clean_complaint_history h
    WHERE
      1 = 1
      AND field = 'Owner'
      AND parent_id = 'a1m5w000006tRYOAA2'
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY created_at_utc) = 1

    UNION ALL

    -- status changes
    SELECT
      parent_id
      , field
      , old_value
      , new_value
      , created_at_utc::TIMESTAMP_NTZ AS ts
      , CASE
          WHEN new_value IN ('Closed',
                             'Inquiry (Not a Complaint)',
                             'Archived',
                             'Withdrawn')
            THEN TRUE
          ELSE FALSE
        END                           AS is_touch
    FROM cash_complaints.sfdc_cfone.clean_complaint_history h
    WHERE
      1 = 1
      AND field = 'Complaint_Status__c'
      AND parent_id = 'a1m5w000006tRYOAA2'
  )

SELECT *
FROM base
ORDER BY ts

;
-- 133645476
SELECT
  parent_id
  , field
  , old_value
  , new_value
  , created_at_utc::TIMESTAMP_NTZ AS ts
  , CASE
      WHEN new_value IN ('Closed',
                         'Inquiry (Not a Complaint)',
                         'Archived',
                         'Withdrawn')
        THEN TRUE
      ELSE FALSE
    END                           AS is_touch
  , u.name
FROM cash_complaints.sfdc_cfone.clean_complaint_history h
  -- left join app_datamart_cco.public.team_queue_catalog tqc
  -- on h.new_value = tqc.queue_id
LEFT JOIN app_datamart_cco.sfdc_cfone.clean_user u
  ON h.created_by_id = u.id
WHERE
  1 = 1
  AND field = 'Owner'
  AND parent_id = 'a1m5w000006tRYOAA2'
  AND new_value NOT ILIKE '% %' -- only pull in the ID records, not names

UNION ALL
SELECT
  parent_id
  , field
  , old_value
  , new_value
  , created_at_utc::TIMESTAMP_NTZ AS ts
  , CASE
      WHEN new_value IN ('Closed',
                         'Inquiry (Not a Complaint)',
                         'Archived',
                         'Withdrawn')
        THEN TRUE
      ELSE FALSE
    END                           AS is_touch
  , new_value                     AS name
FROM cash_complaints.sfdc_cfone.clean_complaint_history h
WHERE
  1 = 1
  AND field = 'Complaint_Status__c'
  AND parent_id = 'a1m5w000006tRYOAA2'
ORDER BY ts
;

-- tqc
SELECT *
FROM app_datamart_cco.public.team_queue_catalog
WHERE
  queue_name ILIKE '%complaint%'
;

SELECT u.name,h.*
FROM cash_complaints.sfdc_cfone.clean_complaint_history h
LEFT JOIN app_datamart_cco.sfdc_cfone.clean_user u
  ON h.created_by_id = u.id

WHERE
  1 = 1

  AND parent_id = 'a1m5w000006tRYOAA2'