-- null channel, mostly notary and cfone
SELECT DISTINCT
  cfone_touch_id
  , awc_touch_id
FROM app_datamart_cco.public.universal_touches
WHERE
  channel IS NULL
;

-- id's are numbers not strings
-- missing comments
DESCRIBE TABLE app_datamart_cco.public.universal_touches

-- missing data
SELECT
    mt.case_id
     ,mt.touch_id
     ,ut.cfone_touch_id
    , mt.case_creation_time
    , mt.touch_start_time      AS legacy_touch_start_time
    , mt.touch_end_time        AS legacy_touch_end_time
    , mt.communication_channel AS legacy_communication_channel
    , mt.handle_time_seconds   AS legacy_handle_time_seconds
    , ut.handle_time           AS universal_handle_time
--   COUNT(DISTINCT mt.case_id)    AS total_cases
--   , COUNT(DISTINCT mt.touch_id) AS total_touches
--   , SUM(ut.handle_time / 3600)  AS total_handle_time
FROM app_cash_cs.preprod.messaging_touches mt
JOIN app_datamart_cco.public.universal_touches ut
  ON mt.case_id = ut.case_id
WHERE
  1 = 1
  AND mt.handle_time_seconds IS NULL
  --   AND mt.case_id = '5005w00002CNwprAAD' -- missing data
  AND YEAR(mt.case_creation_time) >= 2023
;

SELECT
  COUNT(DISTINCT mt.case_id)          AS total_cases
  , COUNT(DISTINCT ut.cfone_touch_id) AS total_touches
  , SUM(ut.handle_time / 3600)        AS total_handle_time
FROM app_cash_cs.preprod.messaging_touches mt
JOIN app_datamart_cco.public.universal_touches ut
  ON mt.case_id = ut.case_id
WHERE
  1 = 1
  AND mt.handle_time_seconds IS NULL
  AND YEAR(mt.case_creation_time) >= 2023
;

-- has data
SELECT
  touch_start_time
  , touch_end_time
  , channel
  , handle_time
FROM app_datamart_cco.public.universal_touches
WHERE
  1 = 1
  AND case_id = '5005w00002CNwprAAD'

LIMIT 100
;

SELECT
  ut.*
FROM app_datamart_cco.public.universal_touches ut
JOIN app_cash_cs.public.support_cases sc
  ON ut.case_id = sc.case_id
WHERE
  sc.case_number = '95124210'
;

SELECT *
FROM app_datamart_cco.sfdc_cfone.mess

SELECT
  touch_assignment_time
  , touch_end_time
  , DATEDIFF(SECONDS, touch_assignment_time, touch_end_time)
  , touch_lifetime_seconds
FROM app_cash_cs.preprod.messaging_touches
WHERE
  case_number = '95124210'
;

CREATE OR REPLACE TABLE personal_johndarrah.public.test2 AS
  SELECT
    NULL                                   AS name
    , case_number
    , ROW_NUMBER() OVER (ORDER BY case_id) AS rn
    , app_version
    , channel
    , case_creation_date
  FROM app_cash_cs.public.support_cases
  QUALIFY
    rn < 100
;

SELECT *
FROM personal_johndarrah.public.test2
ORDER BY 1
;

ALTER TABLE personal_johndarrah.public.test2
  RENAME COLUMN app_version TO test
;

ALTER TABLE personal_johndarrah.public.test2
  ADD COLUMN test2 VARCHAR
;

UPDATE personal_johndarrah.public.test2
SET
  name = 'newemail@example.com'
WHERE
  rn < 10
;


SELECT DISTINCT
  business_unit_name
FROM app_cash_cs.preprod.messaging_touches
;

SELECT *
FROM app_datamart_cco.public.team_queue_catalog
;

-- -- QA
-- null communication_channel
SELECT DISTINCT
  communication_channel
FROM app_cash_cs.preprod.messaging_touches
WHERE
  YEAR(touch_start_time) >= 2022
;

DESCRIBE TABLE app_cash_cs.preprod.messaging_touches
;

SELECT
  queu
FROM app_datamart_cco.public.universal_touches

-- should be two touches
SELECT *
FROM app_datamart_cco.public.universal_touches
WHERE
  case_id = '3495289'

;

SELECT DISTINCT *
FROM app_datamart_cco.public.universal_touches
WHERE
  case_id = '3103454'
;

-- null communication_channel
SELECT DISTINCT
  communication_channel
FROM app_cash_cs.preprod.messaging_touches
WHERE
  YEAR(touch_start_time) >= 2022
;


SELECT
  case_id
  , outcome
  , touch_start_time
  , touch_end_time
  , created_at
  , touch_assignment_time
  , advocate_name
  , *
FROM app_datamart_cco.public.universal_touches
WHERE
  case_id = '5005w00002FB4WoAAL'

;

SELECT
  case_id
  , touch_start_time
  , touch_end_time
  , handle_time_seconds as handle_time
     ,queue_name
, 'preprod' as db
FROM app_cash_cs.preprod.messaging_touches
WHERE
  case_id = '5005w00002FB4WoAAL';
union
;
SELECT
  case_id
  , touch_start_time
  , touch_end_time
  , handle_time
  , queue_name
, 'ut' as db
FROM app_datamart_cco.public.universal_touches
WHERE
  case_id = '5005w00002FB4WoAAL'