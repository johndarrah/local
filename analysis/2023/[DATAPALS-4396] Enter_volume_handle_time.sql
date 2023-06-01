-- author: john darrah
-- ticket: datapals-4396
-- 

-- description:
-- Case volume and handle times for CS & Risk cases.
-- Note that not all the risk cases have handle time.
-- Generally, if the case lives outside of CF1 or Notary then the handle time isn't calculated.
-- This may be because of the process for how the case is handled (i.e. worked in Google sheets) or limitations on what fields are available.
-- teams will be based on the last queue assigned
-- Names above team_names will be references to what's in app_datemart_cco.public.risk_daily_volume


-- risk sources:
-- most trusted source: https://github.com/squareup/app-datamart-cco/blob/main/jobs/combined_risk_daily_volume/combined_risk_daily_volume.sql
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
)
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
  , handled_messaging_email_voice_01 AS (
  SELECT
    d.dt                                          AS dt
    , ut.case_id                                  AS case_id
    , ut.channel                                  AS channel
    , ut.source                                   AS source
    , 'app_datamart_cco.public.universal_touches' AS data_source
    , CASE
    --  entered_midv
        WHEN ut.queue_name ILIKE ANY ('%Bitcoin IDV%',
                                      '%IDV Florida Limits%',
                                      '%IDV Manual Verification%',
                                      '%Compliance Review%')
          THEN 'AR'
    --  entered_evmanual
        WHEN ut.queue_name ILIKE '%Risk EV Manual Verification%'
          THEN 'AR'
    --  entered_emailato, entered_messagingato
        WHEN ut.queue_name ILIKE ANY ('Risk ATO',
                                      'Risk ATO Voice',
                                      'ATO Brokerage and Lending',
                                      '%Messaging Risk ATO Specialty%')
          THEN 'ATO'
    -- entered_capdisputes
        WHEN ut.queue_name ILIKE '%Cash App Pay Disputes%'
          THEN 'Disputes'
    -- entered_disputescashcard
        WHEN ut.queue_name ILIKE '%Disputes Cash Card%'
          THEN 'Disputes'
    -- entered_disputesp2p
        WHEN ut.queue_name ILIKE '%Disputes P2P%'
          THEN 'Disputes'
    -- entered_disputesserviceclaim
        WHEN ut.queue_name ILIKE '%Disputes Service Claim%'
          THEN 'Disputes'
    -- entered_prioritydisputes
        WHEN ut.queue_name ILIKE '%Priority Disputes%'
          THEN 'Disputes'
    -- entered_disputesspecialty
        WHEN ut.queue_name ILIKE '%Disputes Specialty%'
          THEN 'Disputes'
    -- entered_claimdocs
        WHEN ut.queue_name ILIKE '%Claim Docs%'
          THEN 'Disputes'
    -- entered_mobilecheckdeposits
        WHEN ut.queue_name ILIKE '%Mobile Check Deposits%'
          THEN 'Remote Deposit Capture'
    -- entered_bankingcfone
        WHEN ut.queue_name ILIKE '%Banking%'
          THEN 'Banking'
    -- entered_papermoney
        WHEN ut.queue_name ILIKE '%Risk: Paper Money%'
          THEN 'Banking'
    -- entered_missdep
        WHEN ut.queue_name ILIKE '%Missing Deposits%'
          THEN 'Remote Deposit Capture'
    -- entered_ccsuspend
        WHEN ut.queue_name ILIKE '%Cash Card Suspension%'
          THEN 'Banking'
    -- entered_r06
        WHEN ut.queue_name ILIKE '%Standard Deposit R06 and Reversal%'
          THEN 'Remote Deposit Capture'
        WHEN ut.channel = 'Chat'
          THEN 'Messaging'
        ELSE ut.channel
      END                                         AS classification
    , ut.handle_time                              AS handle_time
  FROM dt d
  JOIN app_datamart_cco.public.universal_touches ut
    ON ut.touch_start_time::DATE = d.dt
    AND ut.source IN ('cfone', 'awc')
  JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(ut.last_assigned_queue_id) = LOWER(tqc.queue_id)
)
  , didv_handled_02 AS (
  SELECT
    d.dt                                 AS dt
    , rif.token::STRING                  AS case_id
    , NULL                               AS channel
    , 'idv'                              AS source
    , 'app_cash_cs.public.risk_idv_fact' AS data_source
    -- entered_didv
    , '[No Handle Time] AR'              AS classification
    , NULL                               AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.risk_idv_fact rif
    ON d.dt = rif.created_at::DATE
  WHERE
    method_of_review = 'ADVOCATE'
)
  , notary_handled_03 AS (
  SELECT DISTINCT
    d.dt                                            AS dt
    , naq.assignment_id                             AS case_id
    , NULL                                          AS channel
    , 'notary'                                      AS source
    , 'app_cash_cs.public.notary_assignments_queue' AS data_source
    , IFF(naq.handled_minutes IS NULL, '[No Handle Time] ', '') ||
    CASE
      --  entered_scams
      WHEN naq.queue ILIKE '%scam_payment%'
        THEN 'AR'
      --  entered_iv
      WHEN naq.queue ILIKE '%instrument_verification%'
        THEN 'AR'
      --  entered_ev
      WHEN naq.queue ILIKE '%enhanced_verification%'
        THEN 'AR'
      --  entered_abuse
      WHEN naq.queue ILIKE ANY ('%risk_elder_abuse%', '%threat_of_harm%', '%brokerage_elder_abuse%')
        THEN 'AR'
      --  entered_atolocks
      WHEN naq.queue ILIKE '%ato%'
        THEN 'ATO'
      --  entered_checkreviews
      WHEN naq.queue ILIKE '%check_deposit_manual_review_cash%'
        THEN 'Remote Deposit Capture'
      WHEN naq.queue ILIKE '%idv%'
        THEN 'AR'
      WHEN naq.queue ILIKE '%appeals%'
        THEN 'Appeals'
      ELSE naq.queue
    END                                             AS classification
    , naq.handled_minutes                           AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.notary_assignments_queue naq
    ON d.dt = naq.claimed_at::DATE
  QUALIFY
    ROW_NUMBER() OVER ( PARTITION BY naq.assignment_id ORDER BY naq.occurred_at) = 1
)
  , banking_risk_handled_04 AS (
  SELECT
    d.dt                                            AS dt
    , brp.case_id                                   AS case_id
    , 'internal transfer'                           AS channel
    , 'banking_risk'                                AS source
    , 'app_cash_cs.public.banking_risk_performance' AS data_source
    , CASE
    --  riskreviews_handled, bankingui_handled, ccfraud_handled
        WHEN brp.case_type = 'BANKING RDC RR'
          THEN '[No Handle Time] Remote Deposit Capture'
    --  achtransfers_handled
        ELSE '[No Handle Time] Banking'
      END                                           AS classification
    , NULL                                          AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.banking_risk_performance brp
    ON brp.report_date = d.dt
  LEFT JOIN app_datamart_cco.public.team_queue_catalog tqc
    ON LOWER(brp.case_type) = LOWER(tqc.queue_name)
)
  -- regulator: determines the type of banking fraud is coming
  , banking_hashtag_handled_05 AS (
  SELECT
    d.dt                                        AS dt
    , bh.primary_key                            AS case_id
    , 'internal transfer'                       AS channel
    , 'banking_hashtag'                         AS source
    , 'app_cash_cs.public.banking_hashtags'     AS data_source
    , '[No Handle Time] Remote Deposit Capture' AS classification
    , NULL                                      AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.banking_hashtags bh
    ON d.dt = bh.hashtag_at
  LEFT JOIN regulator_agg ra
    ON bh.target_token = ra.target_token
)
  -- regulator: customer orders cash card signature and checks that it's not a duplicate name
  , cash_card_handled_06 AS (
  SELECT
    d.dt                                                     AS dt
    , rcccf.customer_token::STRING                           AS case_id
    , NULL                                                   AS channel
    , 'cash_card_customization'                              AS source
    , 'app_cash_cs.public.risk_cash_card_customization_fact' AS data_source
    -- entering_ccs
    , '[No Handle Time] AR'                                  AS classification
    , NULL                                                   AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.risk_cash_card_customization_fact rcccf
    ON d.dt = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', rcccf.review_completed_at_pt)
  LEFT JOIN regulator_agg ra
    ON rcccf.token = ra.target_token
)
  , social_handled_07 AS (
  SELECT
    d.dt                                   AS dt
    , sc.sprinklr_case_id::STRING          AS case_id
    , sc.channel_type                      AS channel
    , 'Social'                             AS source
    , 'app_cash_cs.public.sprinklr_cases ' AS data_source
    , '[No Handle Time] Social'            AS classification
    , NULL                                 AS handle_time
  FROM dt d
  JOIN app_cash_cs.public.sprinklr_cases sc
    ON d.dt = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.created_time_pt)
)
  , handled_volume AS (
  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM handled_messaging_email_voice_01

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM didv_handled_02

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM notary_handled_03

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM banking_risk_handled_04

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM banking_hashtag_handled_05

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM cash_card_handled_06

  UNION ALL

  SELECT
    dt
    , case_id
    , channel
    , source
    , data_source
    , classification
    , handle_time
  FROM social_handled_07
)
SELECT DISTINCT
  classification
  , source
  , data_source
FROM handled_volume hv
WHERE
  source NOT IN ('cfone', 'awc')


-- SELECT
--   DATE_TRUNC(MONTH, hv.dt)   AS month_dt
--   , IFF(LEFT(hv.classification, 17) = '[No Handle Time] ',
--         TRIM(SUBSTR(hv.classification, 17, LEN(hv.classification))),
--         hv.classification
--   )                          AS classification
--   --   , hv.classification
--   , COUNT(DISTINCT
--           IFF(LEFT(hv.classification, 17) = '[No Handle Time] ',
--               case_id,
--               NULL
--             )
--   )                          AS entering_volume_no_ht
--   , COUNT(DISTINCT
--           IFF(LEFT(hv.classification, 17) != '[No Handle Time] ',
--               case_id,
--               NULL
--             )
--   )                          AS entering_volume_with_ht
--   , COUNT(DISTINCT case_id)  AS total_entering_volume
--   , SUM(hv.handle_time) / 60 AS handle_time_minutes
-- FROM handled_volume hv
-- WHERE
--   1 = 1
--   AND classification ILIKE '%AR%'
-- GROUP BY 1, 2
-- ORDER BY 1 DESC, 2
;

-- -- -- Quality Checks
-- -- duplicate entering
-- SELECT *
-- FROM entered_sources
-- QUALIFY
-- COUNT(*) OVER (PARTITION BY case_id) > 1
-- ORDER BY case_id

-- -- entering cases also handled
-- SELECT
-- hs.case_id
-- , hs.dt
-- , es.case_id
-- FROM handled_sources hs
-- LEFT JOIN entered_sources es
-- ON hs.case_id = es.case_id
-- WHERE
-- es.case_id IS NULL
-- ORDER BY 1 DESC

-- -- null case_id
-- SELECT
-- hs.*
-- FROM handled_sources hs
-- LEFT JOIN entered_sources es
-- ON hs.case_id = es.case_id
-- WHERE
-- hs.case_id IS NULL
-- ORDER BY 1 DESC
-- -- team name analysis
-- SELECT DISTINCT
-- team_name
-- , source
-- FROM entered_volume
-- WHERE
-- source NOT IN ('cfone', 'voice', 'amazon connect', 'notary')
-- AND team_name IS NULL