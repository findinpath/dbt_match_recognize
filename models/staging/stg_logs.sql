select ts
from {{ ref('raw_logs') }}