
select *
from (
         select end_ts as begin_ts,
                lead(begin_ts) over (order by begin_ts) as end_ts
         from {{ ref('stg_reservations') }}
     )
where  begin_ts < end_ts