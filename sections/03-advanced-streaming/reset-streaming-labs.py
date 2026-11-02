# Databricks notebook source
# MAGIC %md
# MAGIC # Reset the streaming labs (Sections 3 and 4)
# MAGIC
# MAGIC Not a lesson. Run this only when you want to re-take Section 3 or 4 and see
# MAGIC the same numbers as the video: `+43` in lesson 3.2, `97` unique orders in
# MAGIC lesson 3.5, a full change list in lesson 4.3.
# MAGIC
# MAGIC Why it exists: streams remember. Checkpoints hold offsets and state, and a
# MAGIC re-run of a lesson is correctly a no-op. Those same lessons also land new
# MAGIC batches, so counts move.
# MAGIC
# MAGIC **What it removes:** the tables Sections 3 and 4 create, their checkpoints,
# MAGIC and every landed file after the batches lesson 1.5 emits (bus batches 1–2,
# MAGIC CDC batch 1). **What it leaves alone:** everything else in `lakeshore`.
# MAGIC
# MAGIC **After running it:** run lessons in order from 3.2, at least as far as the
# MAGIC lesson you were on. Sections 5, 7, 10 and 12 read `silver.bus_orders` and
# MAGIC `bronze.bus_raw`, so run through lesson 4.7 before going back to them.

# COMMAND ----------

import re

ROOT = "/Volumes/lakeshore/landing/files"

TABLES = ["lakeshore.gold.category_revenue_30m",
          "lakeshore.gold.customer_current",
          "lakeshore.silver.bus_order_payments",
          "lakeshore.silver.bus_orders",
          "lakeshore.silver.bus_orders_quarantine",
          "lakeshore.silver.dim_customer_scd2",
          "lakeshore.bronze.bus_raw"]

CHECKPOINTS = ["_autoloader/bus",
               "_checkpoints/bus_orders",
               "_checkpoints/cdc_customers",
               "_checkpoints/customer_current",
               "_checkpoints/order_payments",
               "_checkpoints/category_revenue"]

for table in TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print("dropped", table)

for path in CHECKPOINTS:
    dbutils.fs.rm(f"{ROOT}/{path}", True)
    print("removed", path)

# COMMAND ----------

def remove_batches_after(folder, pattern, keep_through):
    """Delete feed files whose batch number is past `keep_through`."""
    for f in dbutils.fs.ls(f"{ROOT}/{folder}"):
        match = re.search(pattern, f.name)
        if match and int(match.group(1)) > keep_through:
            dbutils.fs.rm(f.path)
            print("removed", f.name)

remove_batches_after("topics", r"bus_batch_(\d+)\.json$", 2)
remove_batches_after("cdc/customers", r"cdc_customers_batch_(\d+)\.json$", 1)

# COMMAND ----------

# The empty SCD2 shell lesson 3.6 expects (it is lesson 2.5's DDL), so lessons
# that read the dimension before 3.6 refills it find a table, not an error.
spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.silver.dim_customer_scd2 (
               customer_id STRING NOT NULL,
               first_name  STRING, last_name STRING, email STRING,
               valid_from  TIMESTAMP NOT NULL,
               valid_to    TIMESTAMP,
               is_current  BOOLEAN NOT NULL)
             CLUSTER BY (customer_id)
             COMMENT 'Customer dimension, SCD Type 2 — maintained by the S3 CDC stream'""")

print("reset complete: run the lessons in order from 3.2")

# COMMAND ----------

dbutils.notebook.exit("streaming-labs-reset")
