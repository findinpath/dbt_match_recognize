select *
from {{ ref('stg_reservations') }}
match_recognize(
    order by begin_ts
    measures
        a.end_ts as begin_ts,
        b.begin_ts as end_ts
    one row per match
    -- see https://docs.snowflake.com/en/sql-reference/constructs/match_recognize.html#after-match-skip-specifying-where-to-continue-after-a-match
    after match skip to next row
    pattern ( a b )
    define
        b as begin_ts >= lag(end_ts)
)