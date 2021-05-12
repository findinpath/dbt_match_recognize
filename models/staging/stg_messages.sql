select id, marker
from {{ ref('raw_messages') }}