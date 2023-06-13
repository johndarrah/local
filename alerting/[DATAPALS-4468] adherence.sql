-- author: johndarrah
-- description: adherence
-- Resources



SELECT
  a.period_ltz
  , a.date_ltz
  , CONVERT_TIMEZONE(a.local_timezone, 'UTC', period_ltz) AS ts_utc
  , ts_utc::DATE                                          AS dt_utc
  , b.full_name
  , b.team_code
  , b.is_bpo
  , b.is_advocate
  , b.city
  , b.manager
  , a.in_adherence_seconds
  , a.scheduled_seconds
  , in_adherence_seconds / scheduled_seconds              AS adherence
FROM app_cash_cs.public.agent_adherence_attr_detail AS a
LEFT JOIN app_cash_cs.public.employee_cash_dim AS b
  ON a.wfm_id = b.adjusted_employee_id
  AND a.period_ltz BETWEEN b.start_date AND b.end_date
WHERE
  attribute = 'Available'
  AND b.full_name = 'Juan Jose Cortez Mendoza'
  AND period_ltz::DATE = '2022-06-30'