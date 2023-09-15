/************************************************************************************************************
Owner: John Darrah (@johndarrah)
Back Up: Mayuri Magdum (@mayurium)
Business Purpose: Incremental fact table used for alerting in Tableau
Date Range: 2018-01-01 onward
Time Zone: UTC
Primary key: ts

Other relationships:

Change Log:
2023-08-02 - table created by johndarrah for intraday tableau reporting - https://block.atlassian.net/browse/DATAPALS-4468

Notes:
Step 1: Create staging table metadata: app_cash_cs.public.hourly_alerts_fact_staging
Step 2: Delete previous 7 days of data from staging
Step 3: Incrementally load previous 7 days of data into staging
Step 4: Create production table by cloning staging table: app_cash_cs.public.hourly_alerts_fact
************************************************************************************************************/

-- Step 1
CREATE TABLE IF NOT EXISTS app_cash_cs.public.hourly_alerts_fact_staging (
  ts                              TIMESTAMP_NTZ COMMENT 'Touch hourly timestamp in UTC',
  cases_created                   NUMBER COMMENT 'Cases created',
  touches_in_sla                  NUMBER COMMENT 'Touches in SLA',
  qualified_sla_touches           NUMBER COMMENT 'Qualified SLA Touches. Either in business hours or other criteria',
  touch_sla_percent               FLOAT COMMENT 'Touch SLA % = touches in sla / qualified touches',
  prev_hr_interval_touch_sla_pct  FLOAT COMMENT 'Touch SLA for the previous hour',
  entering_touches                NUMBER COMMENT 'Entering Touches',
  handled_touches                 NUMBER COMMENT 'Handled Touches',
  alert_touch_sla_dropped_10pct   BOOLEAN COMMENT 'Touch SLA dropped 10% from previous interval',
  alert_12k_cases_created         BOOLEAN COMMENT '12k or more cases were created',
  alert_touch_sla_less_than_25pct BOOLEAN COMMENT 'Touch SLA is less than 25%'
)
;

-- Step 2
DELETE
FROM app_cash_cs.public.hourly_alerts_fact_staging
WHERE
  1 = 1
  {% if ds == '2023-08-08' %}
  AND ts::DATE <= CURRENT_DATE
  {% else %}
  AND ts::DATE >= DATEADD(DAY, -7, '{{ ds }}'::DATE)
  {% endif %}
;

-- Step 3
INSERT INTO
  app_cash_cs.public.hourly_alerts_fact_staging
WITH
  messaging_cases_created_union AS (
    -- CF1
    SELECT
      DATE_TRUNC(HOUR, created_date_utc)::TIMESTAMP_NTZ AS ts
      , COUNT(DISTINCT id)                              AS cases_created
    FROM app_cash_cs.cfone.cases
    WHERE
      current_channel_c = 'Chat'
      AND YEAR(created_date_utc) >= 2018
    GROUP BY 1

    UNION ALL

    -- rd_ast_cases
    SELECT
      DATE_TRUNC(HOURS, CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', parent_case_created_at)) AS ts
      , COUNT(DISTINCT parent_case_id)                                                          AS cases_created
    FROM app_cash_cs.public.live_agent_chat_escalations
    WHERE
      chat_record_type IN ('RD Chat', 'Internal Advocate Success')
      AND YEAR(parent_case_created_at) >= 2018
    GROUP BY 1
  )
  , messaging_cases_created AS (
  SELECT
    ts
    , SUM(cases_created) AS cases_created
  FROM messaging_cases_created_union
  GROUP BY 1
)
  , messaging_sla AS (
  SELECT
    ts
    , SUM(touches_in_sla)                                                          AS touches_in_sla
    , SUM(qualified_sla_touches)                                                   AS qualified_sla_touches
    , ROUND(SUM(touches_in_sla) / NULLIFZERO(SUM(qualified_sla_touches)) * 100, 2) AS touch_sla_percent
  FROM app_cash_cs.public.hourly_messaging
  GROUP BY 1
)
  , entering_messaging_touches AS (
  SELECT
    ts
    , SUM(entering_touches) AS entering_touches
  FROM app_cash_cs.public.hourly_messaging
  GROUP BY 1
)
  , handled_messaging_touches AS (
  SELECT
    ts
    , SUM(handled_touches) AS handled_touches
  FROM app_cash_cs.public.hourly_advocate_messaging
  GROUP BY 1
)
  , alerts AS (
  SELECT
    ms.ts
    , mcc.cases_created
    , ms.touches_in_sla
    , ms.qualified_sla_touches
    , ms.touch_sla_percent
    , LAG(ms.touch_sla_percent) OVER (ORDER BY mcc.ts)                              AS prev_hr_interval_touch_sla_pct
    , hmt.handled_touches
    , emt.entering_touches
    , IFF(prev_hr_interval_touch_sla_pct - ms.touch_sla_percent >= 10, TRUE, FALSE) AS alert_touch_sla_dropped_10pct
    , IFF(mcc.cases_created > 12000, TRUE, FALSE)                                   AS alert_12k_cases_created
    , IFF(ms.touch_sla_percent < 25, TRUE, FALSE)                                   AS alert_touch_sla_less_than_25pct
  FROM messaging_sla ms
  LEFT JOIN messaging_cases_created mcc
    ON ms.ts = mcc.ts
  LEFT JOIN entering_messaging_touches emt
    ON ms.ts = emt.ts
  LEFT JOIN handled_messaging_touches hmt
    ON ms.ts = hmt.ts
)
SELECT
  ts
  , cases_created
  , touches_in_sla
  , qualified_sla_touches
  , touch_sla_percent
  , prev_hr_interval_touch_sla_pct
  , entering_touches
  , handled_touches
  , alert_touch_sla_dropped_10pct
  , alert_12k_cases_created
  , alert_touch_sla_less_than_25pct
FROM alerts
WHERE
  1 = 1
  {% if ds == '2023-08-08' %}
  AND ts::DATE <= CURRENT_DATE
  {% else %}
  AND ts::DATE >= DATEADD(DAY, -7, '{{ ds }}'::DATE)
  {% endif %}
;

-- Step 4
CREATE OR REPLACE TABLE app_cash_cs.public.hourly_alerts_fact CLONE app_cash_cs.public.hourly_alerts_fact_staging