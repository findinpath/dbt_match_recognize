select topic, txt, ts
from {{ ref('raw_comments') }}