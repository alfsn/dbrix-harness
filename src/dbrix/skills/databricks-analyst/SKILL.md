---
name: databricks-analyst
description: How to query the prod_catalog medallion tables with Spark SQL or PySpark, join the influencer-marketing tables, read full results, and fix policy errors.
---

# Databricks analyst

## Spark SQL or PySpark
- Use `run_sql` for joins, filters, and aggregates. It is shorter and easier to check.
- Use `run_pyspark` when you need several steps, Python logic, or window functions built in a loop. Put the final
  DataFrame in `result`.
- Table names are always 3-part: `prod_catalog.silver.posts`, also inside `spark.table(...)` and `spark.sql(...)`.

## Common joins (influencer marketing)
- Brand to campaigns: `silver.campaigns.brand_id = silver.brands.brand_id`.
- Campaign to posts: `silver.posts.campaign_id = silver.campaigns.campaign_id`. Organic posts have a null
  `campaign_id`; use a LEFT JOIN when they matter.
- Post to creator: `silver.posts.account_id -> silver.social_accounts.account_id -> silver.creators.creator_id`.
- Daily numbers: `silver.post_metrics_daily` (one row per post and day). Prefer the gold tables
  (`gold.campaign_performance`, `gold.creator_brand_summary`, `gold.brand_daily_engagement`) when they already
  have what you need.
- Check `/catalog/<schema>/<table>.md` for the exact columns before you write a query.

## Results
- The tool answer shows the first rows. The full result is at `/results/<id>.csv`; read it when you need more
  (in parts, for long files).

## Errors
- `SecurityException` errors name a rule and give a hint. Common ones:
  - 1- or 2-part table name: use `prod_catalog.<schema>.<table>`.
  - File paths (`spark.read.parquet(...)`, `` delta.`/path` ``): read tables by name instead.
  - Imports: only `pyspark.sql` helpers and a few standard modules (`math`, `datetime`, `json`, `re`, ...).
    Names starting with `_` are not available.
  - A write in a read-only session: answer without writing.
- Other errors are Spark's own messages; fix the query and run it again.
