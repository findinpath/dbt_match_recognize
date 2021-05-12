select start_ts,
       datediff(minute, start_ts, end_ts) as minutes,
       log_count
from {{ ref('stg_logs') }}
match_recognize(
   order by ts
   measures
        count(*) as log_count,
        first(ts) as start_ts,
        last(ts) as end_ts
   one row per match
   pattern ( new_session_event next_event* )
   define
        next_event as ts < dateadd(minutes, 30, lag(ts))
)