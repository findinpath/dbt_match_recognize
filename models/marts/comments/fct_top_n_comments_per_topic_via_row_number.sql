select *
from (
         select *, row_number() over (partition by topic order by ts) as rn
         from {{ ref('stg_comments') }}
     )
where rn <= 3