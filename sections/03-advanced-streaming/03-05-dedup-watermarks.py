# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.5 — Streaming deduplication & watermarks
# MAGIC
# MAGIC A bus promises **at-least-once** delivery — redelivery is a feature, not
# MAGIC a bug. Today it redelivers, our quality gate happily accepts the
# MAGIC duplicates (they're VALID records!), and we build **consumer v3**:
# MAGIC deduplication with bounded state, courtesy of a watermark.
# MAGIC
# MAGIC > Prereqs: lesson 3.4 run (v2 gate + quarantine live).

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

TOPICS_PATH = "/Volumes/lakeshore/landing/files/topics"
CHECKPOINT = "/Volumes/lakeshore/landing/files/_checkpoints/bus_orders"

from pyspark.sql.functions import col, expr, from_json, timestamp_millis, to_date

ORDER_PAYLOAD = ("order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")
BUS_SCHEMA = ("key STRING, value STRING, topic STRING, "
              "partition BIGINT, offset BIGINT, timestamp BIGINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch 5: 43 new records + 10 redeliveries
# MAGIC
# MAGIC `redeliver_frac=0.25` re-appends 10 of batch 4's records **verbatim** —
# MAGIC same key, same value, same offset. Note they also carry their ORIGINAL
# MAGIC event timestamps: redelivered data is late data.

# COMMAND ----------

feed.emit_batch("bus", TOPICS_PATH, 5, redeliver_frac=0.25)

def bronze_ingest():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(BUS_SCHEMA)
            .load(TOPICS_PATH)
            .withColumn("event_ts", timestamp_millis(col("timestamp")))
            .withColumn("event_date", to_date(timestamp_millis(col("timestamp"))))
            .withColumn("source_file", col("_metadata.file_path"))
            .writeStream
            .option("checkpointLocation",
                    "/Volumes/lakeshore/landing/files/_autoloader/bus")
            .trigger(availableNow=True)
            .toTable("lakeshore.bronze.bus_raw"))

bronze_ingest().awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze took all 53 — correctly! Bronze records what the bus delivered,
# MAGIC duplicates included; it's the *auditable* truth of the transport. The
# MAGIC dedup belongs downstream. Prove the duplicates are real:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT key, topic, offset, count(*) AS deliveries
# MAGIC FROM lakeshore.bronze.bus_raw
# MAGIC GROUP BY key, topic, offset HAVING count(*) > 1
# MAGIC ORDER BY topic, key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Watch v2 accept them
# MAGIC
# MAGIC The v2 gate checks VALIDITY, not novelty. A redelivered order is a
# MAGIC perfectly valid order. We replay v2 from bronze so this lesson shows the
# MAGIC same thing on a re-take: a stateless v2 shouldn't resume on a checkpoint where
# MAGIC v3 has already left dedup state, because changing the stateful operators
# MAGIC is one of the changes the docs say generally needs a new checkpoint.

# COMMAND ----------

def reset_consumer():
    """The rebuild ritual: outputs + checkpoint, and gold, which streams from silver."""
    spark.sql("DROP TABLE IF EXISTS lakeshore.silver.bus_orders")
    spark.sql("DROP TABLE IF EXISTS lakeshore.silver.bus_orders_quarantine")
    spark.sql("""CREATE TABLE lakeshore.silver.bus_orders_quarantine (
                   reason STRING NOT NULL, value STRING, topic STRING,
                   offset BIGINT, event_ts TIMESTAMP, source_file STRING)
                 COMMENT 'Diverted bus records that failed the orders quality gate'""")
    dbutils.fs.rm(CHECKPOINT, True)
    spark.sql("DROP TABLE IF EXISTS lakeshore.gold.category_revenue_30m")
    dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/category_revenue", True)

def route_orders(batch_df, batch_id):
    parsed = (batch_df
              .withColumn("payload",
                          from_json(col("value").cast("string"), ORDER_PAYLOAD)))
    checked = parsed.withColumn(
        "reject_reason",
        expr("""CASE
                  WHEN payload IS NULL OR payload.order_id IS NULL
                       THEN 'unparseable payload'
                  WHEN payload.customer_id IS NULL
                       THEN 'missing customer_id'
                  WHEN exists(payload.items, i -> i.quantity <= 0)
                       THEN 'non-positive quantity'
                END"""))
    (checked.filter("reject_reason IS NULL")
     .select("payload.order_id", "payload.customer_id", "payload.order_ts",
             "payload.items", "event_ts", "offset", "source_file")
     .write.mode("append")
     .option("txnAppId", "orders_gate_silver").option("txnVersion", batch_id)
     .saveAsTable("lakeshore.silver.bus_orders"))
    (checked.filter("reject_reason IS NOT NULL")
     .select(col("reject_reason").alias("reason"), "value", "topic",
             "offset", "event_ts", "source_file")
     .write.mode("append")
     .option("txnAppId", "orders_gate_quarantine").option("txnVersion", batch_id)
     .saveAsTable("lakeshore.silver.bus_orders_quarantine"))

def run_v2():
    return (spark.readStream.table("lakeshore.bronze.bus_raw")
            .filter(col("topic") == "orders")
            .writeStream.foreachBatch(route_orders)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start())

reset_consumer()
run_v2().awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, count(*) AS copies
# MAGIC FROM lakeshore.silver.bus_orders
# MAGIC GROUP BY order_id HAVING count(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Orders twice each (four on a first pass). Every revenue number downstream
# MAGIC is now wrong.
# MAGIC
# MAGIC ## 3. The fix — and the state problem
# MAGIC
# MAGIC `dropDuplicates(["key"])` would work — by remembering **every key
# MAGIC forever**. Unbounded state: the stream that slowly eats the cluster.
# MAGIC
# MAGIC `dropDuplicatesWithinWatermark(["key"])` remembers keys only as long as
# MAGIC the **watermark** says duplicates can still arrive. A watermark on
# MAGIC `event_ts` with a 2-hour threshold means: "once I've seen events up to
# MAGIC time T, I stop waiting for anything older than T − 2h" — so dedup state
# MAGIC older than that is dropped. Redeliveries arrive ~30–60 minutes late in
# MAGIC our feed → 2 hours covers them with margin. Bounded state, guaranteed
# MAGIC dedup inside the window.

# COMMAND ----------

# Consumer v3 = watermarked dedup BEFORE the gate. Full rebuild:
reset_consumer()

(spark.readStream.table("lakeshore.bronze.bus_raw")
 .filter(col("topic") == "orders")
 .withWatermark("event_ts", "2 hours")
 .dropDuplicatesWithinWatermark(["key"])
 .writeStream.foreachBatch(route_orders)
 .option("checkpointLocation", CHECKPOINT)
 .trigger(availableNow=True)
 .start()
 .awaitTermination())

# COMMAND ----------

dupes = spark.sql("""SELECT count(*) FROM (
                       SELECT order_id FROM lakeshore.silver.bus_orders
                       GROUP BY order_id HAVING count(*) > 1)""").first()[0]
good = spark.table("lakeshore.silver.bus_orders").count()
bad = spark.table("lakeshore.silver.bus_orders_quarantine").count()
delivered = spark.sql("""SELECT count(DISTINCT key) FROM lakeshore.bronze.bus_raw
                         WHERE topic = 'orders'""").first()[0]
print(f"silver: {good} unique orders · quarantine: {bad} · duplicates: {dupes}")
print(f"distinct order keys in bronze: {delivered}")
assert dupes == 0, "dedup failed"
assert bad == 3, f"expected the 3 corrupted records in quarantine, got {bad}"
assert good + bad == delivered, f"{good} + {bad} must account for all {delivered} keys"

# COMMAND ----------

# MAGIC %md
# MAGIC **97 unique orders** (100 delivered minus 3 quarantined; more if you are
# MAGIC re-taking this after lesson 4.5 landed further batches), zero duplicates,
# MAGIC replayed from bronze in one pass — with dedup state that expires instead
# MAGIC of accumulating.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Bronze keeps every delivery (audit); silver keeps every EVENT once
# MAGIC (truth). The watermark is the contract that makes dedup affordable:
# MAGIC guaranteed inside the threshold, best-effort beyond it, state bounded
# MAGIC always. Consumer v3 is done — and the customers topic has been waiting
# MAGIC patiently. Next: it becomes our SCD2 dimension, by hand.

# COMMAND ----------

dbutils.notebook.exit("dedup-watermark-complete")
