-- add logic for ato queues, problem tag, Risk ATO
SELECT *
FROM app_datamart_cco.sfdc_cfone.case_transactions
WHERE
  1 = 1
  AND case_id = '5005w00002D374gAAB'
;

SELECT
  case_number
  , case_id
FROM app_cash_cs.public.support_cases
WHERE
  case_number = '132680577'
  OR case_number = '84918366'