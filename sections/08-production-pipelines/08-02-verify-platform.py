# Databricks notebook source
# MAGIC %md
# MAGIC # 8.2 — Verify the platform pipeline
# MAGIC
# MAGIC Run this after the first update of `lakeshore-platform-sdp` finishes. Lessons 8.2 and 8.3 read
# MAGIC from it. Two independent implementations, the Section 3 consumer and today's pipeline, read the
# MAGIC same bus with the same rules. If they agree, that's your best evidence that both work.

# COMMAND ----------

valid = spark.table("lakeshore.silver.sdp_orders_valid").count()
quarantined = spark.table("lakeshore.silver.sdp_orders_quarantine").count()
print(f"valid orders: {valid} · quarantined: {quarantined}")
assert valid > 0, "no valid orders: did the first update finish?"
assert quarantined > 0, "the bus holds bad records on purpose, so quarantine can't be empty"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The quarantine twin: one row per reject, with its reason

# COMMAND ----------

display(spark.sql("""SELECT reason, count(*) AS records
                     FROM lakeshore.silver.sdp_orders_quarantine
                     GROUP BY reason ORDER BY reason"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: a materialized view, recomputed to correctness

# COMMAND ----------

display(spark.sql("""SELECT * FROM lakeshore.gold.sdp_category_day
                     ORDER BY order_date, revenue DESC LIMIT 20"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconcile with Section 3
# MAGIC
# MAGIC `silver.bus_orders` is the hand-built consumer. It keeps pace with the bus only as far as you ran
# MAGIC the Section 3 and 4 notebooks, so a difference here means "the feed moved on since", not "broken".

# COMMAND ----------

if spark.catalog.tableExists("lakeshore.silver.bus_orders"):
    by_hand = spark.table("lakeshore.silver.bus_orders").count()
    print(f"imperative silver.bus_orders: {by_hand} · declarative: {valid} · "
          f"{'they agree' if by_hand == valid else 'they differ: re-run Section 4, then compare again'}")
else:
    print("silver.bus_orders isn't here yet: run lesson 3.3 to get the comparison.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Took the Associate course? The old pipeline is still there
# MAGIC
# MAGIC `lakeshore-orders-sdp` keeps owning `silver.sdp_orders`. That's why the new table has its own name:
# MAGIC a table belongs to one pipeline. If you started in this course, this cell has nothing to report.

# COMMAND ----------

if spark.catalog.tableExists("lakeshore.silver.sdp_orders"):
    print(f"Associate table silver.sdp_orders is untouched: "
          f"{spark.table('lakeshore.silver.sdp_orders').count()} rows")
else:
    print("No Associate pipeline tables in this catalog.")

# COMMAND ----------

dbutils.notebook.exit(f"platform-verified:{valid}:{quarantined}")
