{% docs fct_v_shape_stock_price %}
This model is almost identical as the one presented on the page
https://docs.snowflake.com/en/sql-reference/constructs/match_recognize.html#report-one-summary-row-for-each-v-shape

The pattern used for recognizing the "V" shape developments of the stock prices is:

```
pattern(row_before_decrease row_with_price_decrease+ row_with_price_increase+)
  define
    row_with_price_decrease as price < lag(price),
    row_with_price_increase as price > lag(price)
```

The pattern is rather simplistic, but the interesting aspect are the functions
used in the `MEASURES` clause of `MATCH_RECOGNIZE` in order to obtain a summary
of the "V" shape development of the stock price.


NOTE that this model contains the starting, bottom and end of the "V" shape
of the price development pattern matched.
{% enddocs %}