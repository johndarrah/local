-- author: john darrah
-- ticket: datapals-4396

-- description:
-- Case volume and handle times for CS & Risk cases.
-- Note that not all the risk cases have handle time.
-- Generally, if the case lives outside of CF1 or Notary then the handle time isn't calculated.
-- This may be because of the process for how the case is handled (i.e. worked in Google sheets) or limitations on what fields are available.

-- risk sources:
-- most trusted source: app_cash_cs.public.combined_risk_daily_volume
-- regulator cases: https://squarewave.sqprod.co/#/jobs/13232/sql
-- cash card customizations: https://github.com/squareup/app-datamart-cco/blob/main/jobs/cash_card_customizations/cash_card_customizations.sql
-- banking_risk_performance: https://squarewave.sqprod.co/#/jobs/12355/sql

-- non-risk sources:
-- cf1 views: https://github.com/squareup/app-datamart-cco/tree/main/jobs/cf1_touches_views
-- sfdc cf1 views: https://github.com/squareup/app-datamart-cco/tree/main/jobs/sfdc_cf1_touches
-- notary views: https://github.com/squareup/app-datamart-cco/blob/main/jobs/notary_views/notary_views.sql


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
    TO_VARCHAR(rc.target_token, 'UTF-8')        AS target_token
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
    target_token
    , subroute
    , p."'ASSIGN'"                            AS assign_ts
    , p."'CLOSED_COMPLETE'"                   AS closed_ts
    , DATEDIFF(SECONDS, assign_ts, closed_ts) AS handled_seconds
  FROM regulator_base
    PIVOT (MAX(created_at) FOR logical_event_name IN ('CREATE_CASE','ASSIGN' , 'CLOSED_COMPLETE')) AS p
)
  , universal_touches_01 AS (
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
    AND ut.source IN ('cfone', 'awc', 'notary')
  JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(ut.queue_name) = LOWER(tqc.queue_name)
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
    ON sc.last_assigned_queue_id = tqc.queue_id -- need to use the last queue bc it may not have started in the right queue
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
  , notary_entered_03 AS (
  SELECT DISTINCT
    d.dt                AS dt
    , naq.assignment_id AS case_id
    , NULL              AS channel
    , 'notary'          AS source
    , CASE
        WHEN
          NVL(naq.team_code, naq.queue) ILIKE ANY ('%risk_elder_abuse%', '%threat_of_harm%')
          THEN 'Abuse'
        WHEN NVL(naq.team_code, naq.queue) ILIKE '%scam_payment%'
          THEN 'Scams'
        WHEN NVL(naq.team_code, naq.queue) ILIKE '%instrument_verification%'
          THEN 'EV/IV'
        WHEN NVL(naq.team_code, naq.queue) ILIKE ANY ('%enhanced_verification%', '%account based reviews%')
          THEN 'EV/IV'
        WHEN NVL(naq.team_code, naq.queue) ILIKE ANY ('%risk_elder_abuse%', '%threat_of_harm%')
          THEN 'Abuse'
        WHEN NVL(naq.team_code, naq.queue) ILIKE '%ato%'
          THEN 'ATO'
        WHEN NVL(naq.team_code, naq.queue) ILIKE '%check_deposit_manual_review_cash%'
          THEN 'Remote Deposit Capture'
        WHEN NVL(naq.team_code, naq.queue) = 'Cash - Service Disputes'
          THEN 'Disputes'
        ELSE NVL(naq.team_code, naq.queue)
      END               AS team_name
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
    , '[No Handle Time] ' || tqc.queue_name         AS team_name
    , NULL                                          AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.banking_risk_performance brp
    ON brp.report_date = d.dt
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(brp.case_type) = LOWER(tqc.queue_name)
)
  , banking_risk_entered_04 AS (
  SELECT
    d.dt                                   AS dt
    , brp.case_id                          AS case_id
    , 'internal transfer'                  AS channel
    , 'banking_risk'                       AS source
    , '[No Handle Time] ' || tqc.team_name AS team_name
  FROM dt d
  JOIN app_cash_cs.public.banking_risk_performance brp
    ON brp.report_date = d.dt
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(brp.case_type) = LOWER(tqc.queue_name)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY brp.case_id ORDER BY brp.created_date_pst) = 1
)
  -- regulator: determines the type of banking fraud is coming
  , banking_hashtag_handled_05 AS (
  SELECT
    d.dt                                        AS dt
    , bh.primary_key                            AS case_id
    , 'internal transfer'                       AS channel
    , bh.employee_id                            AS advocate_id
    , 'banking_hashtag'                         AS source
    , 'app_cash_cs.public.banking_hashtags'     AS data_source
    , '[No Handle Time] Remote Deposit Capture' AS team_name
    , NULL                                      AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.banking_hashtags bh
    ON d.dt = bh.hashtag_at
  LEFT JOIN regulator_agg ra
    ON bh.target_token = ra.target_token
)
  , banking_hashtag_entered_05 AS (
  SELECT
    d.dt                                        AS dt
    , bh.primary_key                            AS case_id
    , 'internal transfer'                       AS channel
    , 'banking_hashtag'                         AS source
    , '[No Handle Time] Remote Deposit Capture' AS team_name
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
    , '[No Handle Time] AR'                                  AS team_name
    , NULL                                                   AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.risk_cash_card_customization_fact rcccf
    ON d.dt = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rcccf.review_completed_at_pt)
  LEFT JOIN regulator_agg ra
    ON rcccf.token = ra.target_token
)
  , cash_card_entered_06 AS (
  SELECT
    d.dt                           AS dt
    , rcccf.customer_token::STRING AS case_id
    , NULL                         AS channel
    , 'cash_card_customization'    AS source
    , '[No Handle Time] AR'        AS team_name
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
  FROM universal_touches_01

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
    DATE_TRUNC(MONTH, ev.dt)                                                      AS month_dt
    , LOWER(NVL(tqc.team_name, ev.team_name))                                     AS team_name
    , COUNT(ev.case_id)                                                           AS entering_volume
    , COUNT(IFF(LEFT(ev.team_name, 17) != '[No Handle Time] ', ev.case_id, NULL)) AS entering_volume_with_ht
    , COUNT(IFF(LEFT(ev.team_name, 17) = '[No Handle Time] ', ev.case_id, NULL))  AS entering_volume_without_ht
  FROM entered_volume ev
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(ev.team_name) = LOWER(tqc.queue_name)
  GROUP BY 1, 2
)
  , monthly_handle_time AS (
  SELECT
    DATE_TRUNC(MONTH, hv.dt)                  AS month_dt
    , LOWER(NVL(tqc.team_name, hv.team_name)) AS team_name
    , SUM(hv.handle_time)                     AS handle_time_seconds
    , SUM(hv.handle_time) / 60                AS handle_time_minutes
  FROM handled_volume hv
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(hv.team_name) = LOWER(tqc.queue_name)
  GROUP BY 1, 2
)

SELECT
  mev.month_dt
  , IFF(LEFT(mev.team_name, 17) = '[no handle time] ',
        TRIM(SUBSTR(mev.team_name, 17, LEN(mev.team_name))),
        mev.team_name)                  AS team_name
  , SUM(mev.entering_volume)            AS total_entering_volume
  , SUM(mev.entering_volume_with_ht)    AS entering_volume_with_ht
  , SUM(mev.entering_volume_without_ht) AS entering_volume_without_ht
  , SUM(mhv.handle_time_minutes)        AS handle_time_minutes
FROM monthly_entering_volume mev
LEFT JOIN monthly_handle_time mhv
  ON mev.month_dt = mhv.month_dt
  AND mev.team_name = mhv.team_name
GROUP BY 1, 2
ORDER BY 1 DESC, 2
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
-- -- team name analysis
-- SELECT DISTINCT
--   team_name
--   , source
-- FROM entered_volume
-- WHERE
--   source NOT IN ('cfone', 'voice', 'amazon connect', 'notary')
-- AND team_name IS NULL