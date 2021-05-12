-- Report One Summary Row for Each V Shape
select *
from {{ ref('stg_stock_prices') }}
match_recognize(
    partition by company
    order by price_date
    measures
        match_number() as match_number,
        first(price) as start_v_price,
        first(price_date) as start_v_date,
        -- Note the correlation in with the pattern row_with_price_decrease
        -- used for obtaining the bottom stock price in the V shape.
        last(row_with_price_decrease.price) as bottom_v_price,
        last(row_with_price_decrease.price_date) as bottom_v_price_date,
        last(price) as end_v_price,
        last(price_date) as end_v_date,
        count(*) as rows_in_sequence,
        count(row_with_price_decrease.*) as num_decreases,
        count(row_with_price_increase.*) as num_increases
    one row per match
    after match skip to last row_with_price_increase
    pattern(row_before_decrease row_with_price_decrease+ row_with_price_increase+)
    define
        row_with_price_decrease as price < lag(price),
        row_with_price_increase as price > lag(price)
)
order by company, match_number