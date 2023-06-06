SELECT
  sc.case_number
  , 'https://cf1.lightning.force.com/lightning/r/Case/' || tbu.case_id || '/view' AS cf1_link
  , sc.case_creation_date_time::DATE                                              AS case_creation_date
  , ecd.full_name
  , ecd.ldap_today
  , ecd.manager
  , ecd.managers_manager
  , tbu.start_timestamp                                                           AS touch_start_time
  , tbu.stop_timestamp                                                            AS touch_end_time
  , TIME_SLICE(tbu.start_timestamp, 30, 'MINUTE')                                 AS touch_start_interval
  , DATEADD(MINUTE, 30, TIME_SLICE(tbu.stop_timestamp, 30, 'MINUTE'))             AS touch_end_interval
  , tbu.time_spent                                                                AS handle_time_seconds
  , SUM(tbu.time_spent) OVER (PARTITION BY tbu.case_id)                           AS total_case_handle_time_seconds
  , SUM(tbu.time_spent) OVER (PARTITION BY tbu.user_id)                           AS total_advocate_handle_time_seconds
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
  --   case creation date filter
  AND sc.case_creation_date_time::DATE >= '2023-06-01'
  --   touch start date filter
  AND tbu.start_timestamp::DATE >= '2023-06-05'
  --   touch end date filter
  AND tbu.stop_timestamp::DATE >= CURRENT_DATE
  AND ecd.full_name IN (
  --                         'Fatoumata Conteh',
  --                         'Jasmin Nixon',
  --                         'Vanessa Cheers',
                        'Kayla Fogarasi',
                        NULL)
ORDER BY tbu.start_timestamp
;