select count(*)                                as sessions,
       avg(datediff(minute, start_ts, end_ts)) as avg_duration
from {{ ref('stg_logs') }}
match_recognize(
                         order by ts
   measures
      count(*) as rows_in_sequence,
                         first(ts) as start_ts,
                         last(ts) as end_ts
   one row per match
   pattern (new_session_event next_event*)
   define
     next_event as ts < dateadd(minutes, 30, lag(ts))
)