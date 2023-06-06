SELECT
  tbu.case_id
  , ecd.full_name
  , ecd.ldap_today
  , ecd.manager
  , ecd.managers_manager
  , tbu.start_timestamp                                               AS touch_start_time
  , tbu.stop_timestamp                                                AS touch_end_time
  , TIME_SLICE(tbu.start_timestamp, 30, 'MINUTE')                     AS touch_start_interval
  , DATEADD(MINUTE, 30, TIME_SLICE(tbu.stop_timestamp, 30, 'MINUTE')) AS touch_end_interval
  , tbu.time_spent                                                    AS handle_time_seconds
  , SUM(tbu.time_spent) OVER (PARTITION BY tbu.case_id)               AS total_handle_time_seconds
  , sc.problem_tag
FROM app_cash_cs.cfone.time_by_users tbu
JOIN app_cash_cs.public.support_cases sc
  ON tbu.case_id = sc.case_id
LEFT JOIN app_datamart_cco.sfdc_cfone.users u
  ON tbu.user_id = u.id
LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
  ON LOWER(u.email) = LOWER(ecd.email_today)
  AND ecd.role_status = 'Current'
WHERE
  1 = 1
  AND tbu.case_id = '5005w00002DjHuMAAV'
ORDER BY tbu.start_timestamp
;

SELECT *
FROM app_datamart_cco.sfdc_cfone.users
;

