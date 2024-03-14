WITH
  outbound AS (
    SELECT
      system_endpoint
      , call_start_time_utc::DATE AS ds
      , customer_endpoint
      , contact_id
    FROM app_cash_cs.preprod.call_records
    WHERE
      1 = 1
      AND YEAR(call_end_time_utc) >= 2024
      AND initiation_method = 'OUTBOUND'
  )

  , non_outbound AS (
    SELECT
      system_endpoint
      , customer_endpoint
      , contact_id
      , disconnect_reason
    FROM app_cash_cs.preprod.call_records
    WHERE
      1 = 1
      AND YEAR(call_end_time_utc) >= 2024
      AND initiation_method != 'OUTBOUND'
  )

SELECT
  DATE_TRUNC(YEAR, ds)                                                                AS year
  -- , disconnect_reason
  , COUNT(DISTINCT IFF(nb.customer_endpoint IS NOT NULL, nb.customer_endpoint, NULL)) AS total_outbound_with_incoming
  , COUNT(DISTINCT o.customer_endpoint)                                               AS total_outbound
  , total_outbound - total_outbound_with_incoming                                     AS delta
  , ROUND(total_outbound_with_incoming / NULLIFZERO(total_outbound) * 100, 2)         AS pct_outbound_with_incoming
FROM outbound o
LEFT JOIN non_outbound nb
  ON o.customer_endpoint = nb.customer_endpoint
WHERE
  1 = 1
  AND nb.customer_endpoint IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC