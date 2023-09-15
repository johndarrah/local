SELECT DISTINCT
  ji.job_id
  , j.name
  , j.cron
  , j.state
  , u_create.user_name AS create_user_name
  , u_update.user_name AS update_user_name
  , j.created_at::DATE AS created_at
  , j.updated_at::DATE AS updated_at
  , ji.job_version
  , j.project_id
FROM squarewave.raw_oltp.job_item ji
LEFT JOIN squarewave.raw_oltp.job j
  ON ji.job_id = j.id
LEFT JOIN squarewave.raw_oltp.user u_create
  ON j.create_user_id = u_create.id
LEFT JOIN squarewave.raw_oltp.user u_update
  ON j.update_user_id = u_update.id
WHERE
  1 = 1
  AND j.project_id IN (41, 132)
  AND j.state NOT IN ('DELETED', 'ARCHIVED')
  -- update the line below
  AND j.name ILIKE '%support%'
QUALIFY
  MAX(ji.job_version) OVER (PARTITION BY ji.job_id) = ji.job_version