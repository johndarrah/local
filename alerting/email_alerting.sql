with entering_volume as ( --aggregating data about chat touches
       SELECT ut.touch_start_time::date                                                 as entering_date
            , ecd.employee_id
            , ecd.full_name
            , ecd.city
            , team_name as vertical
            , communication_channel as channel
            , business_unit_name
            , COUNT(distinct touch_start_id)                                               as total_entering
            , COUNT(distinct case when backlog_handled then touch_start_id end)            as total_backlog_entering
            , COUNT(distinct case_id)                                                      as total_cases_entering
       FROM app_datamart_cco.public.universal_touches ut
                LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
                         ON ut.advocate_id= ecd.cfone_id_today
                         AND to_date(ut.touch_start_time) between ecd.START_DATE and ecd.END_DATE
       where to_date(ut.touch_start_time) >= '2022-01-01'   --note that some chats may be resolved without interaction by M
--          rework this to pull from UT
       and ut.business_unit_name in ('CUSTOMER SUCCESS - SPECIALTY','CUSTOMER SUCCESS - CORE','Other')
       GROUP BY 1,2,3,4,5,6,7
       )
,
handled_volume as ( --aggregating data about chat touches
       SELECT to_date(mt.TOUCH_ASSIGNMENT_TIME)                                                        as handled_date
            , ecd.employee_id
            , ecd.full_name
            , ecd.city
            , team_name as vertical
            , communication_channel as channel
            , business_unit_name
            , COUNT(distinct touch_id)                                                                 as total_handled
            , COUNT(distinct case when backlog_handled then touch_id end)                              as total_backlog_handled
            , COUNT(DISTINCT case when (response_time_seconds/60)<=7
                       and IN_BUSINESS_HOURS = TRUE
                       then touch_id else NULL END)                                                    as total_in_sl
            , SUM(case when IN_BUSINESS_HOURS = TRUE then RESPONSE_TIME_SECONDS/60 else null end)      as total_response_time_min                               , COUNT(distinct case when IN_BUSINESS_HOURS = TRUE then touch_id else null end)          as  touches_ART
            , SUM(HANDLE_TIME_SECONDS)/60                                                              as total_handle_time_min
            , COUNT(distinct case
                             when in_business_hours = TRUE then touch_id
                             else null end)                                                            as total_handled_sla
       FROM app_cash_cs.preprod.messaging_touches mt
       LEFT JOIN app_datamart_cco.sfdc_cfone.dim_queues dq on dq.queue_name = mt.queue_name
                LEFT JOIN app_cash_cs.public.employee_cash_dim ecd
                         ON mt.employee_id= ecd.employee_id
                         AND to_date(mt.touch_assignment_time) between ecd.START_DATE and ecd.END_DATE
       where ecd.employee_id is not null
       GROUP BY 1,2,3,4,5,6,7
),
messaging_final as
(
   select
         coalesce(e.entering_date, h.handled_date) as date_pt
       , h.employee_id
       , h.full_name
       , h.city
       , COALESCE(h.vertical, e.vertical) as vertical
       , COALESCE(h.channel, e.channel) as channel
       , COALESCE(h.business_unit_name, e.business_unit_name) as business_unit_name
       , e.total_entering as touches_entering
       , h.total_handled as touches_handled
       , h.total_in_sl as total_in_sl
       , h.total_response_time_min
       , h.total_handle_time_min
       , h.total_handled_sla as total_touches_sla
       , h.touches_ART
   FROM entering_volume e
   LEFT JOIN handled_volume h ON e.entering_date = h.handled_date
   AND e.employee_id = h.employee_id
   AND e.vertical = h.vertical
),




entering_rd_ast_volume as
(
   select
   date(chat_created_at) as entering_date,
   chat_advocate_employee_id as employee_id,
   chat_advocate as full_name,
   chat_advocate_city as city,
   CASE WHEN chat_record_type = 'RD Chat' then 'RD' else 'AST' end as vertical,
   'CHAT' as channel,
   'CUSTOMER SUCCESS - CORE' as business_unit_name,
   count(distinct chat_transcript_id) as entering_volume
   from app_cash_cs.public.live_agent_chat_escalations
   where chat_record_type in ('RD Chat','Internal Advocate Success')
   and date(chat_created_at) >= '2022-01-01'
   group by 1,2,3,4,5,6
),
handled_rd_ast_volume as
(
select
   date(chat_start_time) as handle_date,
   chat_advocate_employee_id as employee_id,
   chat_advocate as full_name,
   chat_advocate_city as city,
   CASE WHEN chat_record_type = 'RD Chat' then 'RD' else 'AST' end as vertical,
   'CHAT' as channel,
   'CUSTOMER SUCCESS - CORE' as business_unit_name,
   count(distinct chat_transcript_id) as total_handled,
   sum(chat_handle_time/60) as total_handle_time_min,
  COUNT( distinct CASE
       WHEN chat_record_type = 'RD Chat' and chat_wait_time <= 60 and chat_handle_time > 0  then chat_transcript_id
       WHEN chat_record_type = 'Internal Advocate Success' and chat_wait_time <=180  and chat_handle_time > 0 then chat_transcript_id
       ELSE NULL end ) as total_in_sl,
   COUNT( distinct CASE
       WHEN chat_record_type = 'RD Chat' and chat_wait_time <= 60 and chat_handle_time = 0  then chat_transcript_id
       WHEN chat_record_type = 'Internal Advocate Success' and chat_wait_time <=180  and chat_handle_time = 0 then chat_transcript_id
       ELSE NULL end) as abandoned_chats,
   total_handled - abandoned_chats as total_handled_sla
   from app_cash_cs.public.live_agent_chat_escalations
   where chat_record_type in ('RD Chat','Internal Advocate Success')
   and date(chat_created_at) >= '2022-01-01'
   group by 1,2,3,4,5,6
),
ast_rd_final as
(
   select
    coalesce(e.entering_date, h.handle_date) as date_pt,
   h.employee_id,
   h.full_name,
   h.city,
   COALESCE(h.vertical, e.vertical) as vertical,
   COALESCE(h.channel, e.channel) as channel,
   COALESCE(h.business_unit_name, e.business_unit_name) as business_unit_name,
   e.entering_volume as touches_entering,
   h.total_handled as touches_handled,
  null as  total_response_time_min,
   h.total_handle_time_min,
   h.total_in_sl,
   h.total_handled_sla as total_touches_sla,
   null as touches_ART
   from entering_rd_ast_volume as e
   left join handled_rd_ast_volume as h
   on e.entering_date = h.handle_date
   and e.vertical = h.vertical
   and e.employee_id = h.employee_id
)
select * from messaging_final
UNION
select * from ast_rd_final
