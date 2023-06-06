/**********************
Owner: Allison Kruse @akruse
Back Up: Ali Connor
Business Purpose: Create a record of queue/transfer movement per case for both classic and cf1 salesforce cases.
Helpful for capturing how long it takes a case to reach certain queues, who is sending incorrect transfers, etc.

Change Log:
08/17/2022: @blucero update formatting according to SQL best practices
09/21/2022: @blucero update according to ETL best practices
For more details on the updates by @blucero check out the Confluence page https://wiki.sqprod.co/display/ISWIKI/Optimized+ETL+-+Cases%2C+Queues+and+Transfers
03/02/2023: @ianpeck changed meta.yaml to be tolerant of stale data. Job runs every 30 minutes so it should not cause any problems at all.

-- test
Current State: In Production
Date Changed: 11/09/2020
Timezone: PST

Dependiences:
APP_CASH_CS.CFONE_CLASSIC.CASE_HISTORY
app_datamart_cco.archive.team_queue_catalog_all_sfdc
APP_CASH_CS.CFONE_CLASSIC.BASE_CASES
APP_CASH_CS.PUBLIC.EMPLOYEE_CASH_DIM_Temp - View of employees dim

Primary Key:  case_id, queue_start_time
Level of Granularity: One row per case_id and queue start time

Notes: This query captures a case's movement through queues only. It should not be used to report touches.

***************/

CREATE TABLE IF NOT EXISTS app_cash_cs.public.cases_queues (
	cq_hash_key                 VARCHAR() PRIMARY KEY           COMMENT 'The primary key, (case_id, queue_start_time )'
    , source                     VARCHAR()                      COMMENT 'Salesforce version case is from'
    , case_id                    VARCHAR()                      COMMENT 'Salesforce case id'
    , case_number                VARCHAR()                      COMMENT 'Salesforce case number'
    , queue_id                   VARCHAR()                      COMMENT 'Owner ID of the queue case is currently in'
    , queue_name                 VARCHAR()                      COMMENT 'Queue name of the queue tha case is curretly in'
    , queue_type                 VARCHAR()                      COMMENT 'If the queue is considered CS, RISK, etc.'
    , employee_id                VARCHAR()                      COMMENT 'Employee id of the person making the action on the case for the point in time'
    , user_id                    VARCHAR()                      COMMENT 'Salesforce user id of the person making the action on the case for the point in time'
    , user_name                  VARCHAR()                      COMMENT 'User name of the person making the action on the case for the point in time'
    , user_is_supportrobot       BOOLEAN                        COMMENT 'T/F is user a support bot'
    , transfer_reason            VARCHAR()                      COMMENT 'Drop down selection of why case is being transferred'
    , transfer_comment           VARCHAR()                      COMMENT 'Open text comment about the transfer'
    , queue_number               NUMBER(30, 0)                   COMMENT 'Captures what is the current count of queue in case lifetime'
    , queue_start_time           TIMESTAMP_NTZ(0)               COMMENT 'When the case entered the queue'
    , queue_end_time             TIMESTAMP_NTZ(0)               COMMENT 'When the case left the queue'
);

INSERT OVERWRITE INTO app_cash_cs.public.cases_queues
WITH
    transfer_comment_reason AS ( -- Pulls transfer comment and reason from CF1
        SELECT
            source
            , case_id
            , created_by_id
            , CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created_date::TIMESTAMP_NTZ) AS queue_start_time
            , MAX(IFF(field = 'Transfer_Comment_Reporting__c', new_value, NULL))          AS transfer_comment
            , MAX(IFF(field = 'Transfer_Reason_Reporting__c', new_value, NULL))           AS transfer_reason
        FROM app_cash_cs.cfone_classic.case_history
        WHERE
            field IN ('Transfer_Comment_Reporting__c', 'Transfer_Reason_Reporting__c')
            AND source = 'CF1'
        GROUP BY 1, 2, 3, 4
    )
, queues_1 AS (
    SELECT
        ch.source
        , ch.case_id
        , qmv.queue_id                                                                             AS queue_id
        , qmv.queue_name                                                                                 AS queue_name
        , qmv.business_unit_name                                                                                 AS queue_type
        , ch.created_by_id                                                                         AS user_id
        , ch.created_date --CF1 uses created date field to capture actions/movement
        , CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ch.created_date::TIMESTAMP_NTZ)           AS queue_start_time
    FROM app_cash_cs.cfone_classic.case_history AS ch
    INNER JOIN app_datamart_cco.archive.team_queue_catalog_all_sfdc AS qmv
        ON ch.new_value = qmv.queue_id
    WHERE ch.source = 'CF1'
    UNION ALL
    SELECT
        'CF1'                                                                                      AS source
        , id                                                                                       AS case_id
        , '00G5w000006vwEAEAY'                                                                     AS queue_id
        , 'Cash Phone Internal'                                                                    AS queue_name
        , 'CS'                                                                                     AS queue_type
        , '0055w00000DS6LOAA1'                                                                     AS user_id
        , created_date_utc --CF1 uses created date field to capture actions/movement
        , CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created_date_utc::TIMESTAMP_NTZ)              AS queue_start_time
    FROM app_cash_cs.cfone.cases
    WHERE origin = 'Phone'
)

, queues AS (
    SELECT    ---Captues all transfers through time for CF1 --may need to add dedupping here once we have more data if issues persist in new system
        c.source
        , c.case_id
        , c.queue_id
        , c.queue_name
        , c.queue_type
        , e.employee_id
        , c.user_id
        , IFF(e.full_name IS NULL, u.name, e.full_name)                                                                                            AS user_name
        , IFF(u.last_name = 'Bot', 'TRUE', 'False')                                                                                                AS user_is_supportrobot -- identifies bots in CF1
        , cr.transfer_reason
        , cr.transfer_comment
        , CASE WHEN c.queue_id = LAG(c.queue_id) OVER (PARTITION BY c.case_id ORDER BY c.queue_start_time) THEN NULL ELSE c.queue_start_time END AS queue_start_time --this groups each case that has duplicate queues and assigns a null value in place of queue_start_time for each duplicate row. ORDER BY automatically orders ASC and doesn't need to be defined.
    FROM queues_1 c
    LEFT JOIN app_datamart_cco.sfdc_cfone.users AS u --used to identify bots as last name will be 'Bot'
        ON c.user_id = u.id
    LEFT JOIN app_cash_cs.public.employee_cash_dim AS e
        ON c.user_id = e.cfone_id_today
        AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', c.created_date::TIMESTAMP_NTZ)) BETWEEN e.start_date AND e.end_date
    LEFT JOIN transfer_comment_reason AS cr
        ON cr.queue_start_time = CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', c.created_date::TIMESTAMP_NTZ)
        AND cr.created_by_id = c.user_id
        AND cr.case_id = c.case_id
)

SELECT
	CONCAT(q.case_id, TO_VARCHAR(q.queue_start_time))                                                             AS cq_hash_key
	, q.source --need to bring names through
	, q.case_id
	, bs.case_number
	, q.queue_id
	, q.queue_name
	, q.queue_type
	, q.employee_id
	, q.user_id
	, q.user_name
	, q.user_is_supportrobot
	, q.transfer_reason
	, q.transfer_comment
	, ROW_NUMBER() OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time)                                    AS queue_number --this provides the queue number for each case
	, q.queue_start_time
	, DATEADD(SECOND, -1, LAG(q.queue_start_time) OVER(PARTITION BY q.case_id ORDER BY q.queue_start_time DESC))  AS queue_end_time --this is the last datetime in which an email or transfer can be associated to the queue
FROM queues AS q
LEFT JOIN app_cash_cs.cfone_classic.base_cases AS bs
	ON bs.case_id = q.case_id
WHERE q.queue_start_time IS NOT NULL; -- removes rows from queues cte where queue_start_time IS NULL

/**********************
Owner: Allison Kruse @akruse
Back Up: Ali Connor
Business Purpose: Record all cases transfers inclusive of support robot and advocate transfers recorded at the queue level
Helpful for capturing how long it take a case to reach a certain queues, who is sending incorrect transfers, etc.

08/17/2022 @blucero update formatting according to SQL best practices
09/21/2022 @blucero update according to ETL best practices
For more details on the updates by @blucero check out the Confluence page https://wiki.sqprod.co/display/ISWIKI/Optimized+ETL+-+Cases%2C+Queues+and+Transfers

Current State: In Production
Date Changed: 11/09/2020
Date Range: 2019-06-01 start record
Timezone: PST

Dependiences:
APP_CASH_CS.PUBLIC.CASES_QUEUES
APP_CASH_CS.CFONE_CLASSIC.BASE_CASES

Primary Key:  case_id, queue_start_time
Level of Granularity: One row per case_id and queue start time

Notes: 'Previous' fields are predominatley used to track who is sending incorrect transfers

***************/

CREATE TABLE IF NOT EXISTS app_cash_cs.public.cases_queues_transfers  (
	cq_hash_key                       VARCHAR() PRIMARY KEY   COMMENT 'The primary key, (case_id, queue_start_time )'
    , source                           VARCHAR()               COMMENT 'Salesforce version case is from'
    , case_id                          VARCHAR()               COMMENT 'Salesforce case id'
    , case_number                      VARCHAR()               COMMENT 'Salesforce case number'
    , case_creation_date               DATE                    COMMENT 'When the case was created'
    , employee_id                      VARCHAR()               COMMENT 'Employee id of the person making the action on the case for the point in time'
    , user_id                          VARCHAR()               COMMENT 'Salesforce user id of the person making the action on the case for the point in time'
    , user_name                        VARCHAR()               COMMENT 'User name of the person making the action on the case for the point in time'
    , user_is_supportrobot             BOOLEAN                 COMMENT 'T/F is user a support bot'
    , queue_id                         VARCHAR()               COMMENT 'Owner ID of the queue case is currently in'
    , queue_number                     NUMBER(30, 0)            COMMENT 'Captures what is the current count of queue in case lifetime'
    , queue_name                       VARCHAR()               COMMENT 'Queue name of the queue tha case is curretly in'
    , first_time_in_queue              VARCHAR()               COMMENT 'Is this the first time the case has entered this particular queue'
    , queue_type                       VARCHAR()               COMMENT 'If the queue is considered CS, RISK, etc.'
    , trans_from_queue_type            VARCHAR()               COMMENT 'Did the transfer come from a cs or risk queue'
    , trans_team_type                  VARCHAR()               COMMENT 'Did the transfer come from an external team (ex: cs to risk, disputes to ato) and/or external team (ex: ato to lock, DISPTU'
    , previous_trans_from_name         VARCHAR()               COMMENT 'Who transferred the queue from its previous location'
    , previous_trans_from_user_id      VARCHAR()               COMMENT 'Salesforce ID of who transferred the queue from its previous location '
    , previous_trans_from_queue_name   VARCHAR()               COMMENT 'What was the queue the case was previously in'
    , previous_trans_from_queue_id     VARCHAR()               COMMENT 'What was the ID of the queue the case was previously in'
    , transfer_reason                  VARCHAR()               COMMENT 'Drop down selection of why case is being transferred'
    , transfer_comment                 VARCHAR()               COMMENT 'Open text comment about the transfer'
    , queue_start_time                 TIMESTAMP_NTZ(0)        COMMENT 'When the case entered the queue'
    , queue_end_time                   TIMESTAMP_NTZ(0)        COMMENT 'When the case left the queue'
    , time_in_queue_minutes            NUMBER(30, 0)            COMMENT 'How long was the case in the queue'
    , case_life_till_queue             NUMBER(30, 0)            COMMENT 'What is the age of the case from its creation till it reached the queue in minutes'
    , all_queues                       VARCHAR()               COMMENT 'All queue names in case lieftime'
    , prior_queues                     VARCHAR()               COMMENT 'All queue names before current queue'
    , later_queues                     VARCHAR()               COMMENT 'All queue names after current queue'
);

INSERT OVERWRITE INTO app_cash_cs.public.cases_queues_transfers
WITH base AS (
    SELECT
        q.cq_hash_key
        , q.source
        , q.case_id
        , q.case_number
        , c.case_created_at
        , q.employee_id
        , q.user_id
        , IFF(q.user_name = 'Bot', 'Support Robot', q.user_name) AS user_name
        , q.user_is_supportrobot
        , q.queue_id
        , q.queue_number
        , q.queue_name
        , q.transfer_reason
        , q.transfer_comment
        , CASE WHEN (ROW_NUMBER() OVER (PARTITION BY q.case_id, q.queue_name ORDER BY q.queue_start_time)) = 1 THEN 1 ELSE 0 END AS first_time_in_queue --denotes if first time the case has been in a queue in case lifecycle
        , TRIM(q.queue_type) AS queue_type --current queue cs or risk/TRIM removes any whitespace before or after field
        , CASE WHEN LAG(q.queue_name, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time) = 'CASH EMAIL AUTO RESPONSE' THEN 'OMNI' --this pulls the previous queue name
            WHEN q.queue_number = 1 THEN 'FIRST QUEUE'
            ELSE LAG(q.queue_type, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time) END AS trans_from_queue_type -- did the transfer come from a cs or risk queue
        , CASE WHEN trans_from_queue_type = 'FIRST QUEUE' THEN 'FIRST QUEUE' --cs to cs is internal, risk same queue to queue or ato to lock is internal, everything else is external
            WHEN trans_from_queue_type = 'OMNI' THEN 'OMNI'
            WHEN trans_from_queue_type = 'CS' AND queue_type = LAG(q.queue_type, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time) THEN 'INTERNAL TRANSFER'
            WHEN trans_from_queue_type = 'RISK' AND q.queue_name = LAG(q.queue_name, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time) THEN 'INTERNAL TRANSFER'
            WHEN (q.queue_name LIKE '%ATO%' OR q.queue_name LIKE '%LOCK%') AND (LAG(q.queue_name, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time) LIKE '%ATO%' OR LAG(q.queue_name, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time) LIKE '%LOCK%') THEN 'INTERNAL TRANSFER'
            ELSE 'EXTERNAL TRANSFER' END AS trans_team_type --did the transfer come from an external team (cs to risk, disputes to ato) OR AND INTERNAL TEAM (EX.ATO TO LOCK, DISPTUES TO DISPUTES)
        , LAG(q.user_name, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time)                             AS previous_trans_from_name  --who sent the transfer from the previous queue/we use this to hold people doing bad transfers responsible
        , LAG(q.user_id, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time)                               AS previous_trans_from_user_id --who sent the transfer's SF ID  from the previous queue 
        , LAG(q.queue_name, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time)                            AS previous_trans_from_queue_name --previous queue name
        , LAG(q.queue_id, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time)                              AS previous_trans_from_queue_id --previous queue id
        , q.queue_start_time
        , q.queue_end_time
        , DATEDIFF(MINUTE, q.queue_start_time, q.queue_end_time)                                                         AS time_in_queue_minutes --how long case was in queue
        , IFNULL(LAG(time_in_queue_minutes, 1, NULL) OVER (PARTITION BY q.case_id ORDER BY q.queue_start_time), 0)         AS prior_minutes --how long the case was in the previous queue 
        , ARRAY_AGG(q.queue_name) WITHIN GROUP (ORDER BY q.queue_number) OVER (PARTITION BY q.case_id)                  AS all_queues_array --all queues in case life
        , LAST_VALUE(q.queue_number) OVER (PARTITION BY q.case_id ORDER BY q.queue_number)                              AS last_queue_number
    FROM app_cash_cs.public.cases_queues AS q
    LEFT JOIN app_cash_cs.cfone_classic.base_cases AS c
        ON q.case_id = c.case_id
    WHERE c.case_created_at >= '2019-06-01'
)

SELECT
    cq_hash_key
    , source
    , case_id
    , case_number
    , TO_DATE(case_created_at) AS case_creation_date
    , employee_id
    , user_id
    , user_name
    , user_is_supportrobot
    , queue_id
    , queue_number
    , queue_name
    , first_time_in_queue
    , queue_type
    , trans_from_queue_type
    , trans_team_type
    , previous_trans_from_name
    , previous_trans_from_user_id
    , previous_trans_from_queue_name
    , previous_trans_from_queue_id
    , transfer_reason
    , transfer_comment
    , queue_start_time
    , queue_end_time
    , time_in_queue_minutes
    , SUM(prior_minutes)  OVER (PARTITION BY case_id ORDER BY queue_start_time)                 AS case_life_till_queue --total time from case creation to when it arrived in queue
    , ARRAY_TO_STRING(all_queues_array, ', ')                                                   AS all_queues
    , IFF(ARRAY_SIZE(ARRAY_SLICE(all_queues_array, 0, queue_number - 1)) = 0
        , NULL
        , ARRAY_TO_STRING(ARRAY_SLICE(all_queues_array, 0, queue_number - 1), ', '))             AS prior_queues --all queues prior to current/array index starts at 0 
    , IFF(ARRAY_SIZE(ARRAY_SLICE(all_queues_array, queue_number, last_queue_number)) = 0
        , NULL
        , ARRAY_TO_STRING(ARRAY_SLICE(all_queues_array, queue_number, last_queue_number), ', ')) AS later_queues --all after current
FROM base;
