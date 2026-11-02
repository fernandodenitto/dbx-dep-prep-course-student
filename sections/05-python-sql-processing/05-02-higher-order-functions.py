# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.2 — Higher-order functions for nested data
# MAGIC
# MAGIC Our orders carry an `items` ARRAY. The reflex is `explode` → work →
# MAGIC re-aggregate. Higher-order functions (HOFs) work **inside** the array —
# MAGIC no explode, no shuffle, no re-agg. You met `exists()` guarding quality
# MAGIC in S3; here's the whole family.
# MAGIC
# MAGIC > Prereqs: S3 (`silver.bus_orders` with typed items arrays).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. transform — map over elements

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        transform(items, i -> round(i.quantity * i.unit_price, 2))
# MAGIC          AS line_totals,
# MAGIC        transform(items, i -> i.product_id) AS product_ids
# MAGIC FROM lakeshore.silver.bus_orders LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC The lambda `i -> expression` runs per element, returns a same-length
# MAGIC array. No rows were multiplied — compare mentally with explode.
# MAGIC
# MAGIC ## 2. filter & exists — predicates inside

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        filter(items, i -> i.unit_price >= 150)         AS premium_items,
# MAGIC        exists(items, i -> i.product_id = 'P019')       AS has_kayak,
# MAGIC        size(filter(items, i -> i.unit_price >= 150))   AS n_premium
# MAGIC FROM lakeshore.silver.bus_orders
# MAGIC WHERE exists(items, i -> i.unit_price >= 150)
# MAGIC ORDER BY n_premium DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. aggregate — fold the array to one value
# MAGIC
# MAGIC `aggregate(array, start, merge [, finish])`: an accumulator walks the
# MAGIC elements. Cart total without ever leaving the row:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        round(aggregate(items, 0.0D,
# MAGIC                        (acc, i) -> acc + i.quantity * i.unit_price), 2)
# MAGIC          AS cart_total,
# MAGIC        aggregate(items, 0, (acc, i) -> acc + i.quantity) AS units
# MAGIC FROM lakeshore.silver.bus_orders LIMIT 5;

# COMMAND ----------

# The equivalence proof: HOF cart totals == explode-and-sum cart totals
hof = spark.sql("""
    SELECT order_id, round(aggregate(items, 0.0D,
           (acc, i) -> acc + i.quantity * i.unit_price), 2) AS total
    FROM lakeshore.silver.bus_orders""")
exploded = spark.sql("""
    SELECT order_id, round(sum(item.quantity * item.unit_price), 2) AS total
    FROM (SELECT order_id, explode(items) AS item
          FROM lakeshore.silver.bus_orders)
    GROUP BY order_id""")
diff = hof.join(exploded, "order_id") \
          .filter(hof.total != exploded.total).count()
assert diff == 0, f"{diff} orders disagree"
print(f"✓ HOF and explode agree on all {hof.count()} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Same numbers — different physics. `explode` multiplies rows (then a
# MAGIC shuffle re-groups them); `aggregate` stays row-local. On a 2-billion-row
# MAGIC fact with 4-item carts, that's an 8-billion-row shuffle you didn't run.
# MAGIC
# MAGIC ## 4. The rest of the family, rapid fire

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        array_sort(transform(items, i -> i.unit_price))  AS prices_sorted,
# MAGIC        array_max(transform(items, i -> i.unit_price))   AS priciest,
# MAGIC        array_distinct(transform(items, i -> i.product_id)) AS unique_products,
# MAGIC        zip_with(transform(items, i -> i.product_id),
# MAGIC                 transform(items, i -> i.quantity),
# MAGIC                 (p, q) -> concat(p, ' x', q))            AS labels
# MAGIC FROM lakeshore.silver.bus_orders LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap — when HOF, when explode
# MAGIC
# MAGIC | Need | Tool |
# MAGIC |---|---|
# MAGIC | Per-element compute, result stays with the row | `transform` / `aggregate` |
# MAGIC | Row-level predicate about elements | `exists` / `filter` |
# MAGIC | Elements must become ROWS (join them, group across orders) | `explode` |
# MAGIC
# MAGIC The fork is physical: HOFs are row-local; explode multiplies rows and
# MAGIC usually buys a shuffle. Next: when the built-ins run out — UDFs, and
# MAGIC the discipline of not reaching for them too early.

# COMMAND ----------

dbutils.notebook.exit("hof-complete")
