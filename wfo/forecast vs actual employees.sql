WITH
  base AS (
    SELECT DISTINCT
      date_start
      , COUNT(DISTINCT
              CASE
                WHEN shift_name ILIKE ANY ('%vacation%', '%sick%', '%absence%', '%bereavement%') -- identify which shifts were unplanned
                  THEN employee_id
                ELSE NULL
              END)                            AS total_ooo_employees
      , COUNT(DISTINCT employee_id)           AS total_employees
      , total_employees - total_ooo_employees AS total_in_office_employees
    FROM app_cash_cs.public.advocate_schedule_shrinkage_day
    WHERE
      1 = 1
      AND date_start = '2024-03-13'
    GROUP BY 1
    ORDER BY 1 DESC
  )
-- for the sake of the exercise we'll an advocate can handle 15 cases per day. In the future, this can be determined by queue
SELECT
  total_in_office_employees * 15 AS actual_volume -- only using for this exercise, normally we'd use UT or something similar
  , total_employees * 15         AS forecasted_volume
FROM base