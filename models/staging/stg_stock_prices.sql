select company, price_date, price
from {{ ref('raw_stock_prices') }}