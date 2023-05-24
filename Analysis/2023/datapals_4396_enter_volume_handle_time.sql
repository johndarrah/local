-- author: john darrah
-- sources:
-- -- risk: app_cash_cs.public.combined_risk_daily_volume
-- -- https://github.com/squareup/app-datamart-cco/blob/main/jobs/combined_risk_daily_volume/combined_risk_daily_volume.sql
-- -- https://squarewave.sqprod.co/#/jobs/13232/sql
-- -- https://block.sourcegraph.com/github.com/squareup/app-datamart-cco/-/blob/jobs/cash_card_customizations/cash_card_customizations.sql
-- -- non-risk: app_datamart_cco.public.universal_touches

-- Action Items
-- missing handle time
-- teams for risk

-- CREATE OR REPLACE TABLE personal_johndarrah.public.touch_analysis_2023_05_22 AS
WITH
  dt AS (
    SELECT DISTINCT
      report_date AS dt
    FROM app_cash_cs.public.dim_date_time
    WHERE
      YEAR(report_date) >= 2022
      AND report_date <= CURRENT_DATE
  )
  , regulator_base AS (
  SELECT
    TO_VARCHAR(case_token, 'UTF-8')             AS case_token
    , ql.name                                   AS subroute
    , a.created_at
    , TO_VARCHAR(a.logical_event_name, 'UTF-8') AS logical_event_name
  FROM kases.raw_oltp.case_logical_events a
  LEFT JOIN kases.raw_oltp.cases rc
    ON a.case_token = rc.token
  LEFT JOIN kases.raw_oltp.queue_labels ql
    ON rc.queue_label_id = ql.id
  WHERE
    1 = 1
  --       AND TO_VARCHAR(case_token, 'UTF-8') = 'BABB044851'
)
  --    C_h42twqmxn missing from regulator
  , regulator_agg AS (
  SELECT
    case_token
    , subroute
    , p."'ASSIGN'"                            AS assign_ts
    , p."'CLOSED_COMPLETE'"                   AS closed_ts
    , DATEDIFF(SECONDS, assign_ts, closed_ts) AS handled_seconds
  FROM regulator_base
    PIVOT (MAX(created_at) FOR logical_event_name IN ('CREATE_CASE','ASSIGN' , 'CLOSED_COMPLETE')) AS p
)
  , cfone_handled_01 AS (
  SELECT
    d.dt                                          AS dt
    , ut.case_id                                  AS case_id
    , ut.channel                                  AS channel
    , ut.advocate_id                              AS advocate_id
    , ut.source                                   AS source
    , 'app_datamart_cco.public.universal_touches' AS data_source
    , tqc.team_name                               AS team_name
    , ut.handle_time                              AS handle_time
  FROM dt d
  JOIN app_datamart_cco.public.universal_touches ut
    ON ut.touch_end_time::DATE = d.dt
    AND ut.source = 'cfone'
  JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON ut.last_assigned_queue_id = tqc.queue_id
)
  , cfone_entered_01 AS (
  SELECT
    d.dt            AS dt
    , sc.case_id    AS case_id
    , sc.channel    AS channel
    , 'cfone'       AS source
    , tqc.team_name AS team_name
  FROM dt d
  JOIN app_cash_cs.public.support_cases sc
    ON sc.case_creation_date::DATE = d.dt
  JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON sc.first_assigned_queue = tqc.queue_name
)
  , voice_handled_02 AS (
  SELECT
    d.dt                                          AS dt
    , ut.case_id                                  AS case_id
    , ut.channel                                  AS channel
    , ut.advocate_id                              AS advocate_id
    , ut.source                                   AS source
    , 'app_datamart_cco.public.universal_touches' AS data_source
    , tqc.team_name                               AS team_name
    , ut.handle_time                              AS handle_time
  FROM dt d
  JOIN app_datamart_cco.public.universal_touches ut
    ON ut.touch_end_time::DATE = d.dt
    AND ut.source = 'awc'
  JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON ut.last_assigned_queue_id = tqc.queue_id
)
  , voice_entered_02 AS (
  SELECT
    d.dt                               AS dt
    , NVL(cr.case_id, cr.contact_id)   AS case_id
    , 'voice'                          AS channel
    , 'amazon connect'                 AS source
    , NVL(cr.team_name, cr.queue_name) AS team_name
  FROM dt d
  JOIN app_cash_cs.preprod.call_records cr
    ON cr.case_created_date::DATE = d.dt
  LEFT JOIN cfone_entered_01 cf
    ON cr.case_id = cf.case_id
  WHERE
    cf.case_id IS NULL
)
  , notary_handled_03 AS (
  SELECT
    d.dt                                          AS dt
    , ut.case_id                                  AS case_id
    , ut.channel                                  AS channel
    , ut.advocate_id                              AS advocate_id
    , ut.source                                   AS source
    , 'app_datamart_cco.public.universal_touches' AS data_source
    , tqc.team_name                               AS team_name
    , ut.handle_time                              AS handle_time
  FROM dt d
  JOIN app_datamart_cco.public.universal_touches ut
    ON ut.touch_end_time::DATE = d.dt
    AND ut.source = 'notary'
  JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON ut.last_assigned_queue_id = tqc.queue_id
)
  , notary_entered_03 AS (
  SELECT DISTINCT
    d.dt                            AS dt
    , naq.assignment_id             AS case_id
    , NULL                          AS channel
    , 'notary'                      AS source
    , NVL(naq.team_code, naq.queue) AS team_name
  FROM dt d
  JOIN app_cash_cs.public.notary_assignments_queue naq
    ON d.dt = naq.occurred_at
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY naq.assignment_id ORDER BY naq.occurred_at) = 1
)
  , banking_risk_handled_04 AS (
  SELECT
    d.dt                                            AS dt
    , brp.case_id                                   AS case_id
    , 'internal transfer'                           AS channel
    , brp.employee_id::STRING                       AS advocate_id
    , 'banking_risk'                                AS source
    , 'app_cash_cs.public.banking_risk_performance' AS data_source
    , brp.case_type                                 AS team_name
    , NULL                                          AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.banking_risk_performance brp
    ON brp.report_date = d.dt
)
  --    not complete
  , banking_risk_entered_04 AS (
  SELECT
    d.dt                  AS dt
    , brp.case_id         AS case_id
    , 'internal transfer' AS channel
    , 'banking_risk'      AS source
    , brp.case_type       AS team_name
  FROM dt d
  JOIN app_cash_cs.public.banking_risk_performance brp
    ON brp.report_date = d.dt
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY brp.case_id ORDER BY brp.created_date_pst) = 1
)
  -- regulator: determines the type of banking fraud is coming
  , banking_hashtag_handled_05 AS (
  SELECT
    d.dt                                    AS dt
    , bh.primary_key                        AS case_id
    , 'internal transfer'                   AS channel
    , bh.employee_id                        AS advocate_id
    , 'banking_hashtag'                     AS source
    , 'app_cash_cs.public.banking_hashtags' AS data_source
    --     , ra.subroute                           as team_name
    , CASE
        WHEN bh.team_code IS NOT NULL
          THEN bh.team_code
        WHEN bh.hashtag ILIKE '%RAR%'
          THEN 'rar'
        WHEN bh.hashtag ILIKE ANY ('%RDC_RETURN%', '%RDC_RF_RETURN%')
          THEN 'returns'
        WHEN bh.hashtag ILIKE '%DAN%'
          THEN 'dan'
        ELSE NULL
      END                                   AS team_name
    , ra.handled_seconds                    AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.banking_hashtags bh
    ON d.dt = bh.hashtag_at
  LEFT JOIN regulator_agg ra
    ON bh.target_token = ra.case_token
)
  , banking_hashtag_entered_05 AS (
  SELECT
    d.dt                  AS dt
    , bh.primary_key      AS case_id
    , 'internal transfer' AS channel
    , 'banking_hashtag'   AS source
    , CASE
        WHEN bh.team_code IS NOT NULL
          THEN bh.team_code
        WHEN bh.hashtag ILIKE '%RAR%'
          THEN 'rar'
        WHEN bh.hashtag ILIKE ANY ('%RDC_RETURN%', '%RDC_RF_RETURN%')
          THEN 'returns'
        WHEN bh.hashtag ILIKE '%DAN%'
          THEN 'dan'
        ELSE NULL
      END                 AS team_name
  FROM dt d
  JOIN app_cash_cs.public.banking_hashtags bh
    ON d.dt = bh.hashtag_at
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY bh.primary_key ORDER BY bh.hashtag) = 1
)
  --      regulator: customer orders cash card signature and checks that it's not a duplicate name
  , cash_card_handled_06 AS (
  SELECT
    d.dt                                                     AS dt
    , rcccf.customer_token::STRING                           AS case_id -- unique for the table
    , NULL                                                   AS channel
    , NULL                                                   AS advocate_id
    , 'cash_card_customization'                              AS source
    , 'app_cash_cs.public.risk_cash_card_customization_fact' AS data_source
    --     , rcccf.event_type                                       as team_name
    , NVL(rcccf.team_code, ra.subroute)                      AS team_name
    , ra.handled_seconds                                     AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.risk_cash_card_customization_fact rcccf
    ON d.dt = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rcccf.review_completed_at_pt)
  LEFT JOIN regulator_agg ra
    ON rcccf.token = ra.case_token
)
  , cash_card_entered_06 AS (
  SELECT
    d.dt                                     AS dt
    , rcccf.customer_token::STRING           AS case_id
    , NULL                                   AS channel
    , 'cash_card_customization'              AS source
    , NVL(rcccf.team_code, rcccf.event_type) AS team_name
  FROM dt d
  JOIN app_cash_cs.public.risk_cash_card_customization_fact rcccf
    ON d.dt = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rcccf.created_at_pt)
)
  , handled_volume AS (
  SELECT
    dt
    , case_id
    , channel
    , advocate_id
    , source
    , data_source
    , team_name
    , handle_time
  FROM cfone_handled_01

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , advocate_id
    , source
    , data_source
    , team_name
    , handle_time
  FROM voice_handled_02

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , advocate_id
    , source
    , data_source
    , team_name
    , handle_time
  FROM notary_handled_03

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , advocate_id
    , source
    , data_source
    , team_name
    , handle_time
  FROM banking_risk_handled_04

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , advocate_id
    , source
    , data_source
    , team_name
    , handle_time
  FROM banking_hashtag_handled_05

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , advocate_id
    , source
    , data_source
    , team_name
    , handle_time
  FROM cash_card_handled_06
)
  , entered_volume AS (
  SELECT
    dt
    , case_id
    , channel
    , source
    , team_name
  FROM cfone_entered_01

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , team_name
  FROM voice_entered_02

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , team_name
  FROM notary_entered_03

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , team_name
  FROM banking_risk_entered_04

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , team_name
  FROM banking_hashtag_entered_05

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , team_name
  FROM cash_card_entered_06
)
  , monthly_entering_volume AS (
  SELECT
    DATE_TRUNC(MONTH, ev.dt) AS month_dt
    , LOWER(tqc.team_name)   AS team_name
    , COUNT(ev.case_id)      AS entering_volume
  FROM entered_volume ev
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(ev.team_name) = LOWER(tqc.queue_name)
  GROUP BY 1, 2
)
  , monthly_handle_time AS (
  SELECT
    DATE_TRUNC(MONTH, hv.dt) AS month_dt
    , LOWER(tqc.team_name)   AS team_name
    , SUM(hv.handle_time)    AS handle_time_seconds
  FROM handled_volume hv
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(hv.team_name) = LOWER(tqc.queue_name)
  GROUP BY 1, 2
)
SELECT DISTINCT
  mev.month_dt
  , mev.team_name
  , mev.entering_volume
  , mhv.handle_time_seconds
FROM monthly_entering_volume mev
JOIN monthly_handle_time mhv
  ON mev.month_dt = mhv.month_dt
  AND mev.team_name = mhv.team_name
ORDER BY mev.month_dt DESC, mev.entering_volume DESC
;

-- -- -- Quality Checks
-- -- duplicate entering
-- SELECT *
-- FROM entered_sources
-- QUALIFY
--   COUNT(*) OVER (PARTITION BY case_id) > 1
-- ORDER BY case_id

-- -- entering cases also handled
-- SELECT
--   hs.case_id
--   , hs.dt
--   , es.case_id
-- FROM handled_sources hs
-- LEFT JOIN entered_sources es
--   ON hs.case_id = es.case_id
-- WHERE
--   es.case_id IS NULL
-- ORDER BY 1 DESC

-- -- null case_id
-- SELECT
--   hs.*
-- FROM handled_sources hs
-- LEFT JOIN entered_sources es
--   ON hs.case_id = es.case_id
-- WHERE
--   hs.case_id IS NULL
-- ORDER BY 1 DESC
-- -- null team name
-- SELECT *
-- FROM entered_sources
-- WHERE
--   source NOT IN ('cfone', 'voice')
-- AND team_name IS NULL