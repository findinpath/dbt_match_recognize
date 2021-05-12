select id, marker, gap_length
from {{ ref('stg_messages') }}
match_recognize (
    order by id
    measures
        -- { RUNNING | FINAL }
        -- The frame ends at the last row of the match.
        final count(more.*) as gap_length
    all rows per match
    -- {- ... -}
    --
    -- Exclusion. Excludes the contained symbols or operations from the output.
    -- For example, {- S3 -} excludes operator S3 from the output.
    -- Excluded rows will not appear in the output, but will be included in the evaluation of MEASURES expressions.
    --
    -- ... | ...
    --
    -- Alternative. Specifies that either the first symbol or operation or the other one should occur.
    -- For example, ( S3 S4 ) | PERMUTE(S1, S2). The alternative operator has precedence over the concatenation operator.
    pattern ( new+ (read new+)* | more {- more* -})
    define
        new  as (marker ='X'),
        more as (marker = '|')
)
order by id