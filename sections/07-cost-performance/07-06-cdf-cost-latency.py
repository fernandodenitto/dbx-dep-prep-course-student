# Databricks notebook source
# MAGIC %md
# MAGIC # 7.6 — CDF as a cost lever
# MAGIC
# MAGIC In S4, Change Data Feed solved a *correctness* problem (streaming from
# MAGIC merged tables). The guide ALSO files it under cost & performance: "Apply
# MAGIC Change Data Feed (CDF) to expose row-level changes (updates/deletes) for
# MAGIC efficient incremental downstream processing." Today we do the cost math
# MAGIC on our own tables, and look at those row-level changes.
# MAGIC
# MAGIC > Prereqs: S4 complete (CDF enabled on the dimension, mirror live).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The two bills for staying fresh
# MAGIC
# MAGIC A downstream table must reflect upstream changes. Two ways to pay:
# MAGIC - **Recompute**: rebuild from the full source each cycle. Cost scales
# MAGIC   with SOURCE SIZE.
# MAGIC - **CDF incremental**: process `table_changes` since last time. Cost
# MAGIC   scales with CHANGE VOLUME.
# MAGIC
# MAGIC When the change rate is a fraction of table size — i.e., almost
# MAGIC always — the ratio IS the saving. Let's measure ours.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

CDC_PATH = "/Volumes/lakeshore/landing/files/cdc/customers"

dim_rows = spark.table("lakeshore.silver.dim_customer_scd2").count()
last_v = spark.sql(
    "DESCRIBE HISTORY lakeshore.silver.dim_customer_scd2 LIMIT 1"
).first()["version"]
print(f"dimension: {dim_rows} rows at version {last_v}")

# COMMAND ----------

# a fresh CDC batch arrives (numbered max+1 — re-run safe) and flows
# through the (S4) pipeline
import re

existing = [int(m.group(1)) for f in dbutils.fs.ls(CDC_PATH)
            if (m := re.search(r"batch_(\d+)\.json$", f.name))]
nxt = max(existing) + 1
feed.emit_batch("cdc", CDC_PATH, nxt, entity="customers")
print(f"emitted cdc batch {nxt}")

# --- compact S4 plumbing (last inline appearance — S8 goes declarative) ---
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

CDC_SCHEMA = ("operation STRING, sequence_num BIGINT, event_ts TIMESTAMP, "
              "customer_id STRING, first_name STRING, last_name STRING, "
              "email STRING")

def scd2_apply_v2(batch_df, batch_id):
    latest = (batch_df.withColumn("rn", row_number().over(
        Window.partitionBy("customer_id").orderBy(col("sequence_num").desc())))
        .filter("rn = 1").drop("rn"))
    current = (batch_df.sparkSession.table("lakeshore.silver.dim_customer_scd2")
               .filter("is_current")
               .select("customer_id", col("first_name").alias("cf"),
                       col("last_name").alias("cl"), col("email").alias("ce"),
                       col("valid_from").alias("cv")))
    changed = (latest.join(current, "customer_id", "left")
               .filter("cv IS NULL OR event_ts > cv")
               .filter("operation = 'DELETE' OR ce IS NULL "
                       "OR email <> ce OR last_name <> cl OR first_name <> cf")
               .drop("cf", "cl", "ce", "cv"))
    closes = (changed.withColumn("merge_key", col("customer_id"))
              .withColumn("stage_role", lit("close")))
    inserts = (changed.filter("operation != 'DELETE'")
               .withColumn("merge_key", lit(None).cast("string"))
               .withColumn("stage_role", lit("insert")))
    closes.unionByName(inserts).createOrReplaceTempView("staged_changes")
    batch_df.sparkSession.sql("""
        MERGE INTO lakeshore.silver.dim_customer_scd2 AS t
        USING staged_changes AS s
        ON t.customer_id = s.merge_key AND t.is_current = true
        WHEN MATCHED AND (s.operation = 'DELETE' OR t.email <> s.email
                          OR t.last_name <> s.last_name
                          OR t.first_name <> s.first_name) THEN
          UPDATE SET valid_to = s.event_ts, is_current = false
        WHEN NOT MATCHED AND s.stage_role = 'insert' THEN
          INSERT (customer_id, first_name, last_name, email,
                  valid_from, valid_to, is_current)
          VALUES (s.customer_id, s.first_name, s.last_name, s.email,
                  s.event_ts, NULL, true)""")

(spark.readStream.format("cloudFiles")
 .option("cloudFiles.format", "json").schema(CDC_SCHEMA).load(CDC_PATH)
 .writeStream.foreachBatch(scd2_apply_v2)
 .option("checkpointLocation",
         "/Volumes/lakeshore/landing/files/_checkpoints/cdc_customers")
 .trigger(availableNow=True).start().awaitTermination())
print(f"cdc batch {nxt} applied to the dimension")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The math, on real numbers

# COMMAND ----------

changed_rows = spark.sql(f"""
    SELECT count(*) FROM table_changes(
      'lakeshore.silver.dim_customer_scd2', {last_v + 1})""").first()[0]
total_rows = spark.table("lakeshore.silver.dim_customer_scd2").count()

print(f"recompute bill:  scan {total_rows} rows (the whole dimension)")
print(f"CDF bill:        process {changed_rows} change rows")
print(f"ratio:           {total_rows / max(changed_rows, 1):.0f}x")
assert 0 < changed_rows < total_rows

# What a consumer actually receives: one row per change, labeled
(spark.sql(f"""
    SELECT _change_type, count(*) AS change_rows FROM table_changes(
      'lakeshore.silver.dim_customer_scd2', {last_v + 1})
    GROUP BY _change_type ORDER BY _change_type""").show())

# COMMAND ----------

# MAGIC %md
# MAGIC Tens of change rows versus the whole table — and OUR dimension is
# MAGIC toy-sized. At 100M customers with 0.1% daily change, the ratio is
# MAGIC 1000x, every cycle, forever. That's the "enhance latency" clause too:
# MAGIC processing less means finishing sooner — freshness you don't buy with
# MAGIC compute.
# MAGIC
# MAGIC ## 3. Why not just stream it?
# MAGIC
# MAGIC Streaming tables ingest APPEND-ONLY sources brilliantly (S2's fork).
# MAGIC They never revisit processed rows, and a changed defining query applies
# MAGIC only to NEW rows until a full refresh, which reprocesses EVERYTHING.
# MAGIC Downstream of a table that updates and deletes, the change rows are what
# MAGIC you want: `insert`, `update_preimage`, `update_postimage`, `delete`.
# MAGIC Our SCD2 dimension closes rows instead of deleting them, so this run shows
# MAGIC no `delete` rows. A consumer that filters them out on a table that does
# MAGIC delete keeps rows the source removed.
# MAGIC
# MAGIC ## 4. Two CDFs, same read API
# MAGIC
# MAGIC | | Legacy CDF | Automatic CDF |
# MAGIC |---|---|---|
# MAGIC | Turned on by | `delta.enableChangeDataFeed = true`, per table | row tracking; nothing per table |
# MAGIC | Changes computed | at write, as extra change files | at read, from row lineage |
# MAGIC | Write cost | some, on `MERGE` and `UPDATE` | none extra |
# MAGIC | Works on | Delta | Delta, Iceberg v3 |
# MAGIC | Read with | `table_changes`, `readChangeFeed` | the same two |
# MAGIC | Needs | | Databricks Runtime 19 or above |
# MAGIC
# MAGIC This course's S4 demo uses the property, because that's the form you'll meet in
# MAGIC existing tables and the guide just says "CDF". The docs recommend migrating.
# MAGIC Beyond the exam, as far as the guide says: it doesn't distinguish the two.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Recompute scales with size; CDF scales with change. Our measured
# MAGIC ratio said it all. So far in cost and performance: layout (files),
# MAGIC deletion vectors (writes), stats (reads), profile (diagnosis), CDF
# MAGIC (freshness). One lever left, and it's the one you don't configure: the
# MAGIC cache, in lesson 7.8.

# COMMAND ----------

dbutils.notebook.exit("cdf-cost-complete")
