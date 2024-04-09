SELECT
  c.case_id
  , c.csat_sent_at
  , h.new_value      AS csat_channel
  , h.created_at_utc AS entered_channel_at
FROM app_cash_cs.public.support_cases c
JOIN app_datamart_cco.sfdc_cfone.clean_case_history_current_channel h
  ON c.case_id = h.case_id
  AND c.csat_sent_at >= h.created_at_utc
WHERE
  1 = 1
  -- AND c.origin != c.channel
  -- AND c.csat_sent_at IS NOT NULL
  AND c.case_id = '5005w00001g5ijKAAQ'
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY c.case_id ORDER BY h.created_at_utc DESC) = 1
ORDER BY 1
;