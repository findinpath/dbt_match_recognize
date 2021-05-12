# Primer in using `MATCH_RECOGNIZE` in Snowflake with dbt

This project is thought as a primer in using `MATCH_RECOGNIZE` `SQL:2016` query.

As described in the Snowflake [documentation](https://docs.snowflake.com/en/sql-reference/constructs/match_recognize.html)
`MATCH_RECOGNIZE`:

> Recognizes matches of a pattern in a set of rows. It accepts a set of rows 
> (from a table, view, subquery, or other source) as input, and returns all matches 
> for a given row pattern within this set. 
> The pattern is defined similarly to a regular expression.
> The clause can return either:
>     All the rows belonging to each match. 
>     One summary row per match.


Below can be found a teaser for `MATCH_RECOGNIZE` usage that is being employed to find
stats about the sessions of a visitor on a fictional website: 


```sql
select start_ts,
       datediff(minute, start_ts, end_ts) as minutes,
       log_count
from logs
match_recognize(
   order by visit_ts
   measures
        count(*) as log_count,
        first(visit_ts) as start_ts,
        last(visit_ts) as end_ts
   one row per match
   pattern ( new_session_event next_event* )
   define
        next_event as visit_ts < dateadd(minutes, 30, lag(visit_ts))
)
```

NOTE in the query above that by using `MATCH_RECOGNIZE` can be identified:

- beginning of a session
- duration of the session
- number of visits made during the session

Row pattern matching allows a number of use cases which were either very difficult or rather inefficient
to implement previously in SQL.


A detailed description of the ROw Pattern Recognition in SQL can be found on the [ISO](https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip) website.


The initial inspiration in writing this primer came while browsing the webpage https://modern-sql.com/ .
The page [match_recognize - Regular Expression over Rows](https://modern-sql.com/feature/match_recognize) provides 
a very detailed hands-on visual demonstration of the areas where `MATCH_RECOGNIZE` can be applied.
The associated presentation for the query `MATCH_RECOGNIZE`
can be found on [slideshare](https://www.slideshare.net/MarkusWinand/row-pattern-matching-in-sql2016?ref=https://modern-sql.com/).

The `MATCHED_RECOGNIZE` queries from the presentation are tailored for Oracle database.
This project rewrites most of the presented queries with slight modifications made in order to achieve compatibility
with Snowflake database.

In order to spare the user of the project of executing lots of `CREATE TABLE`, `INSERT` and `SELECT` statements,
this project makes use of [dbt](https://www.getdbt.com/) for seeding and filling models that use `MATCH_RECOGNIZE`
query.

[dbt](https://www.getdbt.com/) offers also the advantage of being able to add HTML presentation for each
of the models (you know, to remember what the internals of the model are about in a few months/years).


The base tables definitions on top of which are applied the `MATCH_RECOGNIZE` queries can be found under the
[sources](./sources) directory.

The `dbt` models containing showcases of usage for `MATCH_RECOGNIZE` can be found under [marts](./models/marts) directory.

## Getting started with dbt

As described in the [introduction to dbt](https://docs.getdbt.com/docs/introduction) :

> dbt (data build tool) enables analytics engineers to transform data in their warehouses by simply writing select statements. 
> dbt handles turning these select statements into tables and views.
  
> dbt does the T in ELT (Extract, Load, Transform) processes – it doesn't extract or load data, 
> but it’s extremely good at transforming data that’s already loaded into your warehouse.

The [jaffle_shop](https://github.com/fishtown-analytics/jaffle_shop)
project is a useful minimum viable dbt project to get new [dbt](https://www.getdbt.com/) users 
up and running with their first dbt project. It includes [seed](https://docs.getdbt.com/docs/building-a-dbt-project/seeds)
files with generated data so that a user can run this project on their own warehouse.

---
For more information on dbt:

* Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
* Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
---

## Demo

Use [virtualenv](https://pypi.org/project/virtualenv/) for creating a `virtual` python environment:

```bash
pip3 install virtualenv
virtualenv venv
source venv/bin/activate
```

Once virtualenv is set, proceed to install the requirements for the project:

```bash
(venv) ➜ pip3 install -r requirements.txt
```

Place in `~/.dbt/profiles.yml` file the following content for interacting via dbt with [Snowflake](https://www.snowflake.com/) database:
**NOTE** be sure to change the coordinates of the database according to your Snowflake account. 

```
# For more information on how to configure this file, please see:
# https://docs.getdbt.com/docs/profile
match_recognize:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your-account.your-snowflake-region
      port: 443
      user: "your-username"
      password: "your-password"
      role: accountadmin
      threads: 4
      database: playground
      warehouse: your-warehouse-name
      schema: dbt_match_recognize
config:
  send_anonymous_usage_stats: False
```

If everything is setup correctly, dbt can be used to seed the database with test data and also to fill the models:

```bash
(venv) ➜ dbt seed --profile match_recognize
(venv) ➜ dbt run  --profile match_recognize
```

By using the Snowflake Query Browser, can be easily consulted the output of each
`MATCH_RECOGNIZE` query in the corresponding models.

```sql
SELECT * from playground.dbt_match_recognize.fct_v_shape_stock_price;
```

Consult the dbt documentation of the models by doing:

```bash
(venv) ➜ dbt docs generate
(venv) ➜ dbt docs serve
```

and visit http://localhost:8080




Once the demo session is over, make sure to deactivate the Python virtual environment

```bash
(venv) ➜ deactivate
```


## Conclusion

The `MATCH_RECOGNIZE` is a wonderful addition to the SQL:2016 standard.
By using this query, there can be avoided to some extent whole [Apache Spark](https://spark.apache.org/)
batch jobs.

Feel free to provide feedback or alternative implementations to any of the topics presented in this project.
`MATCH_RECOGNIZE` applies to a truly wide array of problems. In case you have already solved an interesting
problem with this query, feel free to create an issue containing the SQL statement or even a PR in
order to enrich the array of examples presented in this primer.