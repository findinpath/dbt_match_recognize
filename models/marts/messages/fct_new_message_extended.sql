select id, marker
from {{ ref('stg_messages') }}
match_recognize (
  order by id
  all rows per match
  pattern ((^read)? new+ (read new+)* (read$)?)
  define
     new as (marker='X')
)
order by id