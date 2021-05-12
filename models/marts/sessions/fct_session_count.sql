select count(*) as sessions
from {{ ref('stg_logs') }}
match_recognize(
                         order by ts
   all rows per match
   pattern (begin)
   define begin as ts > dateadd(minutes, 30,
                              coalesce (lag(ts), '1900-01-01'::timestamp)
                             )
)