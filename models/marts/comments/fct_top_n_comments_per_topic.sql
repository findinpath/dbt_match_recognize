select *
from {{ ref('stg_comments') }}
match_recognize (
    partition by topic
    order by ts
    measures
          -- In the MEASURES subclause, when the ALL ROWS PER MATCH subclause is used, RUNNING is the default.
          running count(*) as matched_row_number
    all rows per match
   -- {n, m}    n to m.
   pattern ( ^a{1,3} )
   define
      a as true
)