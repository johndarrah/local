-- AI
--   add cash_queue filter
-- add connected_to_agent_timestamp
-- remove junk from handle time
-- fix HOOPs
-- update abandoned rate
-- talk time can be null or zero
-- awc filtering based on end date and no hoops

WITH
  new_logic AS (
    SELECT
      contact_id
      , CASE
          WHEN initiation_method = 'INBOUND'
            AND wait_start_time IS NOT NULL
            AND is_handled = FALSE
            AND rejected_queued = FALSE
            AND requested_callback IS NULL
            THEN TRUE
          WHEN initiation_method IN ('TRANSFER', 'DISCONNECT', 'CALLBACK')
            AND wait_start_time IS NOT NULL
            AND LAG(initiation_method) OVER (PARTITION BY initial_contact_id ORDER BY call_start_time_utc DESC) IS NULL
            AND NVL(handle_time, 0) < 1
            THEN TRUE
          WHEN wait_start_time IS NOT NULL AND LOWER(initiation_method) IN ('transfer')
            AND NVL(handle_time, 0) < 1
            AND LOWER(disconnect_reason) NOT IN ('contact_flow_disconnect_reason')
            THEN TRUE
          ELSE FALSE
        END                      AS is_abandoned_new
      , COALESCE(talk_time, 0)
        + COALESCE(wrap_time, 0)
        + COALESCE(hold_time, 0) AS handle_time_new
    FROM app_cash_cs.preprod.call_records
    WHERE
      1 = 1
      AND queue_name = 'US-EN Cash General'
      AND YEAR(call_end_time_utc) >= 2024
  )

SELECT DISTINCT
  call_end_time_utc::DATE         AS start_ds
  , COUNT_IF(initiation_method = 'INBOUND'
    AND wait_start_time IS NOT NULL -- doesn't get through IVR or make it to a queue, although the queue may have been determined in the queue:  cr.attributes:queueName
    )                             AS contacts_incoming
  , COUNT_IF(handle_time_new > 0) AS contacts_handled_new
  , COUNT_IF(is_handled)          AS contacts_handled_current
  , COUNT_IF(l.is_abandoned_new)  AS contacts_abandoned
FROM app_cash_cs.preprod.call_records cr
JOIN new_logic l
  ON cr.contact_id = l.contact_id
WHERE
  1 = 1
  AND YEAR(call_end_time_utc) >= 2024
  -- AND call_end_time_utc::DATE <= '2024-02-13'
  AND queue_name = 'US-EN Cash General'
GROUP BY 1
ORDER BY start_ds DESC