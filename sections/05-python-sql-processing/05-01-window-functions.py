# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.1 — Windows, joins and aggregations, in SQL and PySpark
# MAGIC
# MAGIC Section 4 closed the streaming arc. Now the language itself, starting with
# MAGIC the guide's own sentence: "Apply advanced data transformations, including
# MAGIC window functions, joins, and aggregations, using Spark SQL and PySpark to
# MAGIC process large datasets." Both languages, so each idea runs in both.
# MAGIC
# MAGIC > Prereqs: fast-forward + S2 (`gold.fact_order_items`, `gold.dim_product`).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

items = spark.table("lakeshore.gold.fact_order_items")
products = spark.table("lakeshore.gold.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ranking family — and why ties matter
# MAGIC
# MAGIC `ROW_NUMBER` (arbitrary tie-break), `RANK` (gaps), `DENSE_RANK` (no
# MAGIC gaps). The exam gives you tied data and asks what each returns. Real
# MAGIC customer spend rarely ties, so we round it to the nearest hundred to force
# MAGIC ties you can see.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH spend AS (
# MAGIC   SELECT customer_id, round(sum(line_total), -2) AS spend_band
# MAGIC   FROM lakeshore.gold.fact_order_items GROUP BY customer_id)
# MAGIC SELECT customer_id, spend_band,
# MAGIC        ROW_NUMBER() OVER (ORDER BY spend_band DESC, customer_id) AS row_num,
# MAGIC        RANK()       OVER (ORDER BY spend_band DESC) AS rnk,
# MAGIC        DENSE_RANK() OVER (ORDER BY spend_band DESC) AS dense_rnk
# MAGIC FROM spend ORDER BY spend_band DESC, customer_id LIMIT 12;

# COMMAND ----------

# The same query in PySpark: a Window spec, then .over()
by_band = Window.orderBy(F.col("spend_band").desc())
ranked = (items.groupBy("customer_id")
          .agg(F.round(F.sum("line_total"), -2).alias("spend_band"))
          .withColumn("row_num", F.row_number().over(
              Window.orderBy(F.col("spend_band").desc(), "customer_id")))
          .withColumn("rnk", F.rank().over(by_band))
          .withColumn("dense_rnk", F.dense_rank().over(by_band)))
display(ranked.orderBy(F.col("spend_band").desc(), "customer_id").limit(12))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Frames — ROWS vs RANGE, on real ties
# MAGIC
# MAGIC `ROWS BETWEEN` counts physical rows. `RANGE BETWEEN` includes every peer with
# MAGIC an equal ORDER BY value. And with ORDER BY and no frame, you get RANGE. One
# MAGIC order with several lines is exactly a tie on `order_date`, so here are both
# MAGIC side by side on one customer's lines.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH lines AS (
# MAGIC   SELECT order_date, order_id, product_id, line_total
# MAGIC   FROM lakeshore.gold.fact_order_items
# MAGIC   WHERE customer_id = (SELECT customer_id FROM lakeshore.gold.fact_order_items
# MAGIC                        GROUP BY customer_id, order_date
# MAGIC                        ORDER BY count(*) DESC, customer_id LIMIT 1))
# MAGIC SELECT order_date, order_id, line_total,
# MAGIC   round(sum(line_total) OVER (ORDER BY order_date, order_id, product_id
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS rows_total,
# MAGIC   round(sum(line_total) OVER (ORDER BY order_date
# MAGIC     RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS range_total,
# MAGIC   round(sum(line_total) OVER (ORDER BY order_date), 2)    AS default_frame
# MAGIC FROM lines ORDER BY order_date, order_id, product_id LIMIT 12;

# COMMAND ----------

# MAGIC %md
# MAGIC Read `range_total` next to `rows_total`: on tied dates RANGE shows the same
# MAGIC number on every peer, and `default_frame` always equals `range_total`. Now
# MAGIC prove it instead of eyeballing it, in PySpark this time, and add a moving
# MAGIC average, which is what ROWS is for.

# COMMAND ----------

by_date = Window.partitionBy("customer_id").orderBy("order_date")
rows_w = (Window.partitionBy("customer_id")
          .orderBy("order_date", "order_id", "product_id")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))
range_w = by_date.rangeBetween(Window.unboundedPreceding, Window.currentRow)

framed = (items
          .withColumn("rows_total", F.sum("line_total").over(rows_w))
          .withColumn("range_total", F.sum("line_total").over(range_w))
          .withColumn("default_frame", F.sum("line_total").over(by_date))
          .withColumn("ma_3_lines", F.avg("line_total").over(
              Window.partitionBy("customer_id").orderBy("order_date", "order_id", "product_id")
                    .rowsBetween(-2, Window.currentRow))))

differ = framed.filter(F.abs(F.col("rows_total") - F.col("range_total")) > 0.001).count()
default_is_range = framed.filter(
    F.abs(F.col("default_frame") - F.col("range_total")) > 0.001).count()
print(f"rows where ROWS and RANGE disagree: {differ:,}")
print(f"rows where the default frame differs from RANGE: {default_is_range}")
assert differ > 0, "ties must make ROWS and RANGE disagree"
assert default_is_range == 0, "with ORDER BY and no frame, the default is RANGE"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. LAG / LEAD — time between events

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH orders AS (
# MAGIC   SELECT DISTINCT customer_id, order_id, order_date
# MAGIC   FROM lakeshore.gold.fact_order_items)
# MAGIC SELECT customer_id, order_id, order_date,
# MAGIC        LAG(order_date) OVER w                       AS prev_order,
# MAGIC        datediff(order_date, LAG(order_date) OVER w) AS days_since_prev
# MAGIC FROM orders
# MAGIC WINDOW w AS (PARTITION BY customer_id ORDER BY order_date, order_id)
# MAGIC ORDER BY customer_id, order_date LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC The `WINDOW` clause names a window once and reuses it — cleaner than
# MAGIC repeating OVER(...) and exam-relevant syntax.
# MAGIC
# MAGIC ## 4. QUALIFY — filter on a window without a subquery
# MAGIC
# MAGIC > **↩️ Associate recap:** QUALIFY deduplicated our customers in silver.
# MAGIC > Professional register: top-N-per-group in one clause.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Each category's top revenue day
# MAGIC SELECT category, order_date, revenue FROM (
# MAGIC   SELECT p.category, f.order_date, round(sum(f.line_total), 2) AS revenue
# MAGIC   FROM lakeshore.gold.fact_order_items f
# MAGIC   JOIN lakeshore.gold.dim_product p USING (product_id)
# MAGIC   GROUP BY p.category, f.order_date)
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) = 1
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# PySpark has no .qualify(): the twin is a rank column, a filter, and a drop
top_day = (items.join(products, "product_id")
           .groupBy("category", "order_date")
           .agg(F.round(F.sum("line_total"), 2).alias("revenue"))
           .withColumn("rn", F.row_number().over(
               Window.partitionBy("category").orderBy(F.col("revenue").desc())))
           .filter("rn = 1").drop("rn"))
display(top_day.orderBy(F.col("revenue").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Multi-level aggregation: ROLLUP
# MAGIC
# MAGIC One scan, three grains: per (category, date), per category, grand
# MAGIC total. `GROUPING SETS` and `CUBE` are the same family; the exam checks
# MAGIC you can read the NULL "all" rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.category, f.order_date,
# MAGIC        round(sum(f.line_total), 2) AS revenue,
# MAGIC        count(DISTINCT f.order_id)  AS orders
# MAGIC FROM lakeshore.gold.fact_order_items f
# MAGIC JOIN lakeshore.gold.dim_product p USING (product_id)
# MAGIC GROUP BY ROLLUP (p.category, f.order_date)
# MAGIC ORDER BY p.category NULLS FIRST, f.order_date NULLS FIRST LIMIT 12;

# COMMAND ----------

# The same rollup in PySpark, and the row the NULLs promise: the grand total
rolled = (items.join(products, "product_id")
          .rollup("category", "order_date")
          .agg(F.round(F.sum("line_total"), 2).alias("revenue")))
rollup_grand = rolled.filter("category IS NULL AND order_date IS NULL").first()["revenue"]
plain_total = (items.join(products, "product_id")
               .agg(F.round(F.sum("line_total"), 2)).first()[0])
assert rollup_grand == plain_total, (rollup_grand, plain_total)
print(f"✓ the rollup's grand-total row reconciles: {rollup_grand}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Joins — types first
# MAGIC
# MAGIC The guide says joins, and the exam asks which type answers the sentence. All
# MAGIC of these are one word apart and return different rows. The question:
# MAGIC *which products have never been ordered?*

# COMMAND ----------

ordered_products = items.select("product_id").distinct()

counts = {
    "inner  (products ordered)": products.join(ordered_products, "product_id", "inner").count(),
    "left   (all products)": products.join(ordered_products, "product_id", "left").count(),
    "semi   (ordered, no right columns)": products.join(ordered_products, "product_id", "left_semi").count(),
    "anti   (NEVER ordered)": products.join(ordered_products, "product_id", "left_anti").count(),
    "cross  (every pair)": products.crossJoin(ordered_products).count(),
}
for name, n in counts.items():
    print(f"{name:<38} {n:>5}")

n_products, n_ordered = products.count(), ordered_products.count()
assert counts["semi   (ordered, no right columns)"] + counts["anti   (NEVER ordered)"] == n_products
assert counts["cross  (every pair)"] == n_products * n_ordered

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The SQL spelling of the anti join
# MAGIC SELECT p.product_id, p.name
# MAGIC FROM lakeshore.gold.dim_product p
# MAGIC LEFT ANTI JOIN lakeshore.gold.fact_order_items f USING (product_id)
# MAGIC ORDER BY p.product_id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Semi and anti never multiply rows and never bring columns from the right side.
# MAGIC That makes them the answer to "exists" and "doesn't exist" questions, where an
# MAGIC inner join plus DISTINCT works and costs more.
# MAGIC
# MAGIC ## 7. Join strategies — ask for a broadcast, then read the plan
# MAGIC
# MAGIC A broadcast copies the small side to every executor, so nothing shuffles.
# MAGIC Adaptive query execution (AQE) can already switch a sort merge join to a
# MAGIC broadcast at runtime when the real size turns out small. A hint states your
# MAGIC intent. `explain()` shows the strategy here; the query profile shows it on a
# MAGIC finished run, and lesson 7.5 reads it there.

# COMMAND ----------

hinted = items.join(F.broadcast(products), "product_id").groupBy("category").sum("line_total")
hinted.explain()
assert hinted.count() > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The same hint in SQL. Open the query profile of this cell's run to see the join.
# MAGIC SELECT /*+ BROADCAST(p) */ p.category, round(sum(f.line_total), 2) AS revenue
# MAGIC FROM lakeshore.gold.fact_order_items f
# MAGIC JOIN lakeshore.gold.dim_product p USING (product_id)
# MAGIC GROUP BY p.category ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Skew, for the record.** AQE splits skewed tasks in sort merge and shuffle hash
# MAGIC joins, automatically; it never does in broadcast joins, and its skew handling
# MAGIC doesn't cover aggregations. For an aggregation where a few keys hold most of
# MAGIC the data, the docs' advice is salting: append a random bucket suffix to the
# MAGIC skewed keys, then group and aggregate in two stages. Beyond the guide's wording,
# MAGIC though a query profile question can lead here. Nothing to run on data this size.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Ranking (know your ties), frames (ROWS vs RANGE, default RANGE), LAG/LEAD,
# MAGIC named WINDOW, QUALIFY, ROLLUP, join types, broadcast. Each ran in SQL and
# MAGIC PySpark, because stems show both as *given code to interpret*. Next: the
# MAGIC functions that work INSIDE arrays.

# COMMAND ----------

dbutils.notebook.exit("windows-complete")
