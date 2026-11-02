# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 4.2 — CDC under disorder: rank + stale guard
# MAGIC
# MAGIC Two ways a CDC feed lies about order:
# MAGIC 1. **Within a file** — events shuffled; file order ≠ commit order.
# MAGIC 2. **Across files** — an old batch shows up AFTER newer ones (replays,
# MAGIC    recovered partitions, vendor "redeliveries").
# MAGIC
# MAGIC Our S3 pipeline already survives (1) — the window rank. Today we prove
# MAGIC it, then harden against (2) with a **stale guard**, and watch a replayed
# MAGIC old file bounce off harmlessly.
# MAGIC
# MAGIC > Prereqs: S3 complete (`dim_customer_scd2` live, CDC batches 1–2
# MAGIC > applied). Run this notebook from the course Git folder.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import datagen
import feed

CDC_PATH = "/Volumes/lakeshore/landing/files/cdc/customers"
CHECKPOINT = "/Volumes/lakeshore/landing/files/_checkpoints/cdc_customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch 3 arrives scrambled

# COMMAND ----------

feed.emit_batch("cdc", CDC_PATH, 3, entity="customers", with_reordered_ops=True)

import json
with open(f"{CDC_PATH}/cdc_customers_batch_0003.json") as f:
    seqs = [json.loads(line)["sequence_num"] for line in f]
print("file order  :", seqs[:10])
print("commit order:", sorted(seqs)[:10])

# COMMAND ----------

# MAGIC %md
# MAGIC File order lies; `sequence_num` doesn't. Our rank step
# MAGIC (`row_number() … ORDER BY sequence_num DESC`) already restores truth
# MAGIC *within* the batch. The upgrade below adds the **stale guard** for
# MAGIC across-batch disorder: an event may only touch a version that is
# MAGIC OLDER than the event itself.

# COMMAND ----------

from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

CDC_SCHEMA = ("operation STRING, sequence_num BIGINT, event_ts TIMESTAMP, "
              "customer_id STRING, first_name STRING, last_name STRING, "
              "email STRING")

def scd2_apply_v2(batch_df, batch_id):
    """S3's choreography + stale guard: old events cannot rewrite new state."""
    latest = (batch_df
              .withColumn("rn", row_number().over(
                  Window.partitionBy("customer_id")
                        .orderBy(col("sequence_num").desc())))
              .filter("rn = 1").drop("rn"))

    current = (batch_df.sparkSession
               .table("lakeshore.silver.dim_customer_scd2")
               .filter("is_current")
               .select(col("customer_id"),
                       col("first_name").alias("cur_first"),
                       col("last_name").alias("cur_last"),
                       col("email").alias("cur_email"),
                       col("valid_from").alias("cur_valid_from")))

    changed = (latest.join(current, "customer_id", "left")
               # STALE GUARD: only events newer than the open version apply
               .filter("cur_valid_from IS NULL OR event_ts > cur_valid_from")
               # no-op filter, as in S3
               .filter("operation = 'DELETE' OR cur_email IS NULL "
                       "OR email <> cur_email OR last_name <> cur_last "
                       "OR first_name <> cur_first")
               .drop("cur_first", "cur_last", "cur_email", "cur_valid_from"))

    closes = (changed
              .withColumn("merge_key", col("customer_id"))
              .withColumn("stage_role", lit("close")))
    inserts = (changed.filter("operation != 'DELETE'")
               .withColumn("merge_key", lit(None).cast("string"))
               .withColumn("stage_role", lit("insert")))
    closes.unionByName(inserts).createOrReplaceTempView("staged_changes")

    batch_df.sparkSession.sql("""
        MERGE INTO lakeshore.silver.dim_customer_scd2 AS t
        USING staged_changes AS s
        ON t.customer_id = s.merge_key AND t.is_current = true
        WHEN MATCHED AND (s.operation = 'DELETE'
                          OR t.email      <> s.email
                          OR t.last_name  <> s.last_name
                          OR t.first_name <> s.first_name) THEN
          UPDATE SET valid_to = s.event_ts, is_current = false
        WHEN NOT MATCHED AND s.stage_role = 'insert' THEN
          INSERT (customer_id, first_name, last_name, email,
                  valid_from, valid_to, is_current)
          VALUES (s.customer_id, s.first_name, s.last_name, s.email,
                  s.event_ts, NULL, true)
    """)

def run_scd2_v2():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(CDC_SCHEMA)
            .load(CDC_PATH)
            .writeStream
            .foreachBatch(scd2_apply_v2)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start())

run_scd2_v2().awaitTermination()
print("scrambled batch 3 applied")

# COMMAND ----------

# The invariants still hold — disorder within the file changed nothing
def scd2_invariants():
    dup = spark.sql("""SELECT count(*) FROM (
        SELECT customer_id FROM lakeshore.silver.dim_customer_scd2
        WHERE is_current GROUP BY customer_id HAVING count(*) > 1)""").first()[0]
    open_closed = spark.sql("""SELECT count(*)
        FROM lakeshore.silver.dim_customer_scd2
        WHERE NOT is_current AND valid_to IS NULL""").first()[0]
    assert dup == 0 and open_closed == 0, (dup, open_closed)
    return spark.table("lakeshore.silver.dim_customer_scd2").count()

rows_after_b3 = scd2_invariants()
print(f"✓ invariants hold — {rows_after_b3} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The nasty case: an OLD file arrives LAST
# MAGIC
# MAGIC A replay of five batch-2 events lands as a brand-new file. Without the
# MAGIC stale guard these would close CURRENT versions with OLD data — silent
# MAGIC history corruption. Watch them bounce:

# COMMAND ----------

stale_events = datagen.generate_cdc_batch("customers", 2)[:5]   # yesterday's news
datagen.write_json(stale_events,
                   f"{CDC_PATH}/cdc_customers_batch_0004.json")

before = spark.table("lakeshore.silver.dim_customer_scd2").count()
run_scd2_v2().awaitTermination()
after = scd2_invariants()
print(f"rows: {before} → {after} (stale replay applied nothing)")
assert after == before, "stale events must not create versions"

# COMMAND ----------

# MAGIC %md
# MAGIC Zero new versions, invariants intact. The guard line to remember:
# MAGIC
# MAGIC ```
# MAGIC cur_valid_from IS NULL OR event_ts > cur_valid_from
# MAGIC ```
# MAGIC
# MAGIC An event may only advance state, never rewind it.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Within-file disorder → **rank by sequence**. Across-file staleness →
# MAGIC **stale guard**. Together they make the pipeline order-proof — and
# MAGIC they are precisely what `SEQUENCE BY` does inside AUTO CDC
# MAGIC (formerly APPLY CHANGES): you've now hand-built the semantics the
# MAGIC exam asks you to recognize in one clause. Next: the dimension must feed
# MAGIC consumers of its own — and MERGE-maintained tables refuse to stream.
# MAGIC Enter Change Data Feed.

# COMMAND ----------

dbutils.notebook.exit("cdc-disorder-complete")
