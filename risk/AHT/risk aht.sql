SELECT
  TO_VARCHAR(a.case_token, 'UTF-8')              AS case_token
  , ql.name                                      AS subroute
  , TO_DATE(a.created_at)                        AS date
  , a.created_at                                 AS assigned_at
  , b.created_at                                 AS completed_at
  , TIMEDIFF(SECOND, a.created_at, b.created_at) AS handle_time_seconds
FROM kases.raw_oltp.case_logical_events a
JOIN kases.raw_oltp.case_logical_events b
  ON a.case_token = b.case_token AND a.case_queue_id = b.case_queue_id
LEFT JOIN kases.raw_oltp.cases rc
  ON a.case_token = rc.token
LEFT JOIN kases.raw_oltp.queue_labels ql
  ON rc.queue_label_id = ql.id
WHERE
  TO_VARCHAR(a.logical_event_name, 'UTF-8') = 'ASSIGN'
  AND TO_VARCHAR(b.logical_event_name, 'UTF-8') = 'CLOSED_COMPLETE'
  AND TO_DATE(a.created_at) > '2023-05-08'
  AND ql.name IN ('CASH_UK_IDV', 'CASH_BTC_IDV', 'CASH_IDV')