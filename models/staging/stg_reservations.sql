select id, begin_ts, end_ts
from {{ ref('raw_reservations') }}