{% docs raw_logs %}
This table simulates a log of the visits made on a website.

The model is intentionally pretty simplistic because it contains only the
timestamp of the visit. No user identifier is present in order to keep
the `MATCH_RECOGNIZE` statement rather simple without additional partitioning per user.
`MATCH_RECOGNIZE` is used on top of this table for discovering stats
about how much minutes a session has taken.

This example has been retrieved from https://www.slideshare.net/MarkusWinand/row-pattern-matching-in-sql2016
{% enddocs %}