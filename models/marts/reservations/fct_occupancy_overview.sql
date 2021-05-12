-- see both busy & available periods
select begin_ts, end_ts, type
from (

         select begin_ts, end_ts, type
         from (
                  select end_ts as begin_ts,
                         lead(begin_ts) over (order by begin_ts) end_ts,
                         'AVAILABLE' type
                  from {{ ref('stg_reservations') }}
              )
         where  begin_ts < end_ts
         UNION ALL
         select begin_ts, end_ts, 'BUSY' type
         from {{ ref('stg_reservations') }}
     )
order by begin_ts