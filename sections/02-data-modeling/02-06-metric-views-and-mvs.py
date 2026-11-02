# Databricks notebook source
# MAGIC %md
# MAGIC # 2.6 — One definition, pre-computed: metric views + materialized views
# MAGIC
# MAGIC Two problems every star schema grows, and two objects that solve them:
# MAGIC
# MAGIC | Problem | Object | Built here |
# MAGIC |---|---|---|
# MAGIC | Every dashboard computes "net revenue" its own way | **Unity Catalog metric view** | `gold.revenue_metrics` |
# MAGIC | Every dashboard recomputes the same joins | **Materialized view** | `gold.mv_category_month_revenue` |
# MAGIC
# MAGIC > Prereq: fast-forward (1.4) and the target model (2.5) have run. Safe to re-run.

# COMMAND ----------

for table in ("gold.fact_order_items", "gold.dim_product",
              "silver.payments", "silver.refunds"):
    assert spark.catalog.tableExists(f"lakeshore.{table}"), f"{table}: run 1.4 and 2.5 first"
print("✓ prerequisites in place")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The problem: two dashboards, two revenues
# MAGIC
# MAGIC Both queries below are sensible. Finance reads the second, Marketing reads the
# MAGIC first, and the Monday meeting is about why they differ.

# COMMAND ----------

marketing = spark.sql("""
    SELECT sum(line_total) FROM lakeshore.gold.fact_order_items
""").first()[0]

finance = spark.sql("""
    SELECT (SELECT sum(amount) FROM lakeshore.silver.payments WHERE status = 'completed')
         - (SELECT sum(refund_amount) FROM lakeshore.silver.refunds)
""").first()[0]

print(f"Marketing's revenue: {marketing:,.2f}   (everything ordered)")
print(f"Finance's revenue:   {finance:,.2f}   (completed payments, minus refunds)")
assert marketing > finance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The metric view: fields, measures, one definition
# MAGIC
# MAGIC A metric view is a Unity Catalog object defined in YAML. **Fields** are what
# MAGIC you group and filter by. **Measures** are aggregations, with no grouping fixed
# MAGIC in advance. A plain view locks its `GROUP BY` at creation; a metric view lets
# MAGIC the query choose it and generates the right computation.
# MAGIC
# MAGIC Read the joins carefully. Refunds live at order level and the fact lives at
# MAGIC line level, so `settlement` is built to hold **one row per order**, carrying
# MAGIC the share of the order that was refunded. Join refunds straight to the lines
# MAGIC and every line of a refunded order would subtract the whole refund.
# MAGIC
# MAGIC `'on'` needs its quotes: unquoted, YAML reads it as the boolean `true`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lakeshore.gold.revenue_metrics
# MAGIC WITH METRICS LANGUAGE YAML AS
# MAGIC $$
# MAGIC   version: 1.1
# MAGIC   comment: "Lakeshore revenue: the one definition of net revenue"
# MAGIC   source: lakeshore.gold.fact_order_items
# MAGIC   joins:
# MAGIC     - name: product
# MAGIC       source: lakeshore.gold.dim_product
# MAGIC       'on': source.product_id = product.product_id
# MAGIC       rely:
# MAGIC         at_most_one_match: true
# MAGIC     - name: settlement
# MAGIC       source: |
# MAGIC         SELECT p.order_id, p.status,
# MAGIC                coalesce(sum(r.refund_amount), 0) / p.amount AS refund_share
# MAGIC         FROM lakeshore.silver.payments p
# MAGIC         LEFT JOIN lakeshore.silver.refunds r USING (payment_id)
# MAGIC         GROUP BY p.order_id, p.status, p.amount
# MAGIC       'on': source.order_id = settlement.order_id
# MAGIC       rely:
# MAGIC         at_most_one_match: true
# MAGIC   fields:
# MAGIC     - name: category
# MAGIC       expr: product.category
# MAGIC     - name: order_month
# MAGIC       expr: DATE_TRUNC('MONTH', source.order_date)
# MAGIC   measures:
# MAGIC     - name: booked_revenue
# MAGIC       expr: SUM(source.line_total)
# MAGIC     - name: net_revenue
# MAGIC       expr: SUM(source.line_total * (1 - settlement.refund_share)) FILTER (WHERE settlement.status = 'completed')
# MAGIC     - name: net_share
# MAGIC       expr: MEASURE(net_revenue) / MEASURE(booked_revenue)
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Querying it: the MEASURE function
# MAGIC
# MAGIC One definition, any grain. First by category:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category,
# MAGIC        MEASURE(booked_revenue) AS booked_revenue,
# MAGIC        MEASURE(net_revenue)    AS net_revenue,
# MAGIC        MEASURE(net_share)      AS net_share
# MAGIC FROM lakeshore.gold.revenue_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Then by month for one category. Same measure, no new SQL logic, no way to get
# MAGIC the refund arithmetic wrong:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_month, MEASURE(net_revenue) AS net_revenue
# MAGIC FROM lakeshore.gold.revenue_metrics
# MAGIC WHERE category = 'Water'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY order_month;

# COMMAND ----------

# The definition must agree with Finance's number: that's what makes it THE definition
net = spark.sql("""
    SELECT MEASURE(net_revenue) FROM lakeshore.gold.revenue_metrics
""").first()[0]
print(f"metric view net revenue: {net:,.2f}  |  Finance: {finance:,.2f}")
assert abs(net - finance) < 1, "the refund allocation lost money somewhere"

# COMMAND ----------

# MAGIC %md
# MAGIC Two refusals worth seeing once, because they make good wrong answers:

# COMMAND ----------

refusals = {
    "SELECT * on a metric view":
        "SELECT * FROM lakeshore.gold.revenue_metrics",
    "joining a metric view directly":
        """SELECT m.category FROM lakeshore.gold.revenue_metrics m
           JOIN lakeshore.gold.dim_product p ON m.category = p.category""",
}
for label, query in refusals.items():
    try:
        spark.sql(query).collect()
        raise RuntimeError(f"{label} was expected to fail")
    except Exception as err:
        assert "METRIC_VIEW" in str(err), err
        print(f"{label}: {str(err).split(']')[0].strip('[')}")

# COMMAND ----------

# MAGIC %md
# MAGIC To join a metric view to something else, query it in a CTE first and join the
# MAGIC CTE's result.
# MAGIC
# MAGIC ## 4. The materialized view: the pre-computed half
# MAGIC
# MAGIC The metric view fixes consistency, not speed: by default each query still reads
# MAGIC the source. So the dashboard reads a materialized view (MV), and that MV is
# MAGIC computed **from the metric view**. There is still one definition.
# MAGIC
# MAGIC Lesson 2.4 made its MV in the SQL editor. Here a serverless notebook does it,
# MAGIC which the docs label Beta. If you hit `RESOURCE_EXHAUSTED`, wait a few minutes
# MAGIC and re-run the cell.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW lakeshore.gold.mv_category_month_revenue
# MAGIC COMMENT 'Pre-computed from gold.revenue_metrics: the table dashboards read'
# MAGIC AS SELECT category, order_month,
# MAGIC           MEASURE(booked_revenue) AS booked_revenue,
# MAGIC           MEASURE(net_revenue)    AS net_revenue
# MAGIC    FROM lakeshore.gold.revenue_metrics
# MAGIC    GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What a dashboard dataset runs: no joins, no refund logic, plain columns
# MAGIC SELECT category, round(sum(net_revenue), 2) AS net_revenue
# MAGIC FROM lakeshore.gold.mv_category_month_revenue
# MAGIC GROUP BY category
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The trade you just accepted: freshness
# MAGIC
# MAGIC A metric view answers from current data. An MV answers from its last refresh.
# MAGIC Watch them disagree: one extra order line, then a refresh.

# COMMAND ----------

def booked():
    live = spark.sql("""SELECT MEASURE(booked_revenue)
                        FROM lakeshore.gold.revenue_metrics""").first()[0]
    stored = spark.sql("""SELECT sum(booked_revenue)
                          FROM lakeshore.gold.mv_category_month_revenue""").first()[0]
    return live, stored

STALE_ORDER = "O_STALE"
spark.sql(f"DELETE FROM lakeshore.gold.fact_order_items WHERE order_id = '{STALE_ORDER}'")
spark.sql("REFRESH MATERIALIZED VIEW lakeshore.gold.mv_category_month_revenue")
before = booked()

spark.sql(f"""INSERT INTO lakeshore.gold.fact_order_items
              SELECT '{STALE_ORDER}', order_date, customer_id, product_id, 1, 100.0, 100.0
              FROM lakeshore.gold.fact_order_items LIMIT 1""")
live, stored = booked()
print(f"after insert   metric view: {live:,.2f}   MV: {stored:,.2f}")
assert abs(live - before[0] - 100) < 0.01, "the metric view reads current data"
assert abs(stored - before[1]) < 0.01, "the MV keeps its last refresh"

spark.sql("REFRESH MATERIALIZED VIEW lakeshore.gold.mv_category_month_revenue")
live, stored = booked()
print(f"after refresh  metric view: {live:,.2f}   MV: {stored:,.2f}")
assert abs(live - stored) < 0.01, "a refresh brings them back together"

# COMMAND ----------

# Put the fact back the way we found it, and the MV with it
spark.sql(f"DELETE FROM lakeshore.gold.fact_order_items WHERE order_id = '{STALE_ORDER}'")
spark.sql("REFRESH MATERIALIZED VIEW lakeshore.gold.mv_category_month_revenue")
live, stored = booked()
assert abs(live - before[0]) < 0.01 and abs(stored - before[1]) < 0.01
print("✓ fact and MV restored")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Two different objects in Unity Catalog, governed like everything else there:
# MAGIC -- GRANT SELECT on the metric view works the way it does on a view.
# MAGIC SELECT table_name, table_type
# MAGIC FROM lakeshore.information_schema.tables
# MAGIC WHERE table_schema = 'gold'
# MAGIC   AND table_name IN ('revenue_metrics', 'mv_category_month_revenue');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The built-in version (not run here)
# MAGIC
# MAGIC A metric view can also carry its own `materialization:` block. Databricks then
# MAGIC creates a managed Lakeflow pipeline, and queries on the metric view are
# MAGIC rewritten to use the pre-computed results when they match:
# MAGIC
# MAGIC ```yaml
# MAGIC   materialization:
# MAGIC     schedule: every 1 day
# MAGIC     mode: relaxed
# MAGIC     materialized_views:
# MAGIC       - name: by_category_month
# MAGIC         type: aggregated
# MAGIC         dimensions: [category, order_month]
# MAGIC         measures: [net_revenue]
# MAGIC ```
# MAGIC
# MAGIC We don't leave one running: it's a scheduled pipeline on a free tier, and the
# MAGIC separate MV is easier to see. Two limits to remember: `relaxed` mode doesn't
# MAGIC check freshness, and you can't materialize a metric view whose sources use row
# MAGIC filters, column masks or ABAC policies.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `gold.revenue_metrics` holds the one definition. `gold.mv_category_month_revenue`
# MAGIC holds the pre-computed answer. Next lesson: how big tables should be laid out on
# MAGIC disk, in Delta and in Iceberg.

# COMMAND ----------

dbutils.notebook.exit("metric-views-complete")
