# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.4 — Quality enforcement & quarantine
# MAGIC
# MAGIC Production sends three kinds of garbage sooner or later: payloads that
# MAGIC don't parse, payloads missing required fields, payloads with impossible
# MAGIC values. Our v1 consumer trusts all of them. Let's watch it fail, then
# MAGIC build **consumer v2**: a quality gate that routes bad records to a
# MAGIC **quarantine table** instead of silver — or the trash.
# MAGIC
# MAGIC > Prereqs: lessons 3.2–3.3 run (`bronze.bus_raw`, `silver.bus_orders` v1).
# MAGIC > Run this notebook from the course Git folder.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

TOPICS_PATH = "/Volumes/lakeshore/landing/files/topics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The bus sends batch 4 — with production-grade dirt
# MAGIC
# MAGIC `with_bad_records=True`: one order with quantity 0, one with a null
# MAGIC customer_id, one with a TRUNCATED (unparseable) JSON value. Three bad
# MAGIC records out of 43 — realistic odds.

# COMMAND ----------

feed.emit_batch("bus", TOPICS_PATH, 4, with_bad_records=True)

# Re-run the v1 pipeline exactly as lesson 3.3 built it, replayed from bronze so
# the lesson gives the same result on a re-take. (Silver is about to be replayed,
# so the gold stream that reads it is reset too: see lesson 3.3.)
spark.sql("DROP TABLE IF EXISTS lakeshore.silver.bus_orders")
spark.sql("DROP TABLE IF EXISTS lakeshore.gold.category_revenue_30m")
dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/bus_orders", True)
dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/category_revenue", True)

from pyspark.sql.functions import col, from_json

ORDER_PAYLOAD = ("order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")

def bronze_ingest():
    """Bronze pass: land whatever is new in topics/ (lesson 3.2 pipeline)."""
    from pyspark.sql.functions import timestamp_millis, to_date
    BUS_SCHEMA = ("key STRING, value STRING, topic STRING, "
                  "partition BIGINT, offset BIGINT, timestamp BIGINT")
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

def orders_v1():
    return (spark.readStream.table("lakeshore.bronze.bus_raw")
            .filter(col("topic") == "orders")
            .withColumn("payload",
                        from_json(col("value").cast("string"), ORDER_PAYLOAD))
            .select("payload.order_id", "payload.customer_id",
                    "payload.order_ts", "payload.items",
                    "event_ts", "offset", "source_file")
            .writeStream
            .option("checkpointLocation",
                    "/Volumes/lakeshore/landing/files/_checkpoints/bus_orders")
            .trigger(availableNow=True)
            .toTable("lakeshore.silver.bus_orders"))

bronze_ingest().awaitTermination()
orders_v1().awaitTermination()
print("v1 ingested batch 4 — let's inspect the damage")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count_if(order_id IS NULL)                        AS unparseable_rows,
# MAGIC   count_if(customer_id IS NULL AND order_id IS NOT NULL) AS null_customer,
# MAGIC   count_if(exists(items, i -> i.quantity <= 0))     AS impossible_quantity
# MAGIC FROM lakeshore.silver.bus_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC Three wounds, all silent: `from_json` turned the truncated payload into
# MAGIC an all-NULL row; the null customer and the zero quantity sailed straight
# MAGIC through. Nothing failed. **That's the worst part — bad data that doesn't
# MAGIC crash.** (`exists(items, …)` is a higher-order function — it tests array
# MAGIC elements without exploding; full treatment in Section 5.)
# MAGIC
# MAGIC ## 2. Consumer v2: gate + quarantine
# MAGIC
# MAGIC Strategy fork, said out loud:
# MAGIC - **Flag**: keep bad rows in silver with an `is_quarantined` column
# MAGIC - **Divert**: bad rows go to a separate quarantine table
# MAGIC
# MAGIC We divert — silver stays trustworthy by construction, and the
# MAGIC quarantine table keeps the RAW value so nothing is lost. Rebuild = drop
# MAGIC table + checkpoint, replay from bronze. Bronze replay makes fixes cheap.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full clean rebuild (also on re-runs): drop v1 outputs AND the old
# MAGIC -- quarantine, recreate the quarantine empty
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.bus_orders;
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.bus_orders_quarantine;
# MAGIC CREATE TABLE lakeshore.silver.bus_orders_quarantine (
# MAGIC   reason      STRING NOT NULL,
# MAGIC   value       STRING,
# MAGIC   topic       STRING,
# MAGIC   offset      BIGINT,
# MAGIC   event_ts    TIMESTAMP,
# MAGIC   source_file STRING
# MAGIC ) COMMENT 'Diverted bus records that failed the orders quality gate';

# COMMAND ----------

dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/bus_orders", True)
print("v1 table and checkpoint gone — clean rebuild")

# COMMAND ----------

from pyspark.sql.functions import expr

def route_orders(batch_df, batch_id):
    """Quality gate: valid rows → silver, bad rows → quarantine with reason."""
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
     .write.mode("append").saveAsTable("lakeshore.silver.bus_orders"))

    (checked.filter("reject_reason IS NOT NULL")
     .select(col("reject_reason").alias("reason"), "value", "topic",
             "offset", "event_ts", "source_file")
     .write.mode("append")
     .saveAsTable("lakeshore.silver.bus_orders_quarantine"))

(spark.readStream.table("lakeshore.bronze.bus_raw")
 .filter(col("topic") == "orders")
 .writeStream
 .foreachBatch(route_orders)
 .option("checkpointLocation",
         "/Volumes/lakeshore/landing/files/_checkpoints/bus_orders")
 .trigger(availableNow=True)
 .start()
 .awaitTermination())

# COMMAND ----------

good = spark.table("lakeshore.silver.bus_orders").count()
bad = spark.table("lakeshore.silver.bus_orders_quarantine").count()
print(f"silver: {good} · quarantine: {bad}")
assert bad >= 3, "expected the 3 corrupted records in quarantine"
assert good + bad >= 80, "batches 1-4 should be fully accounted for"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT reason, offset, left(value, 60) AS value_preview, source_file
# MAGIC FROM lakeshore.silver.bus_orders_quarantine ORDER BY offset;

# COMMAND ----------

# MAGIC %md
# MAGIC Every rejected record: WHY it failed, the RAW payload untouched, and the
# MAGIC file it came from. Quarantine is not a trash can — it's a repair queue:
# MAGIC fix the producer, replay the records, nothing lost.
# MAGIC
# MAGIC ## 3. `foreachBatch` is at-least-once: prove the cure
# MAGIC
# MAGIC The docs are blunt: "`foreachBatch()` provides only at-least-once write
# MAGIC guarantees." If a micro-batch retries, our two appends run again and
# MAGIC double-write. The cure for Delta is two write options, with `txnVersion`
# MAGIC bound to the `batch_id` the function receives. Delta remembers the highest
# MAGIC version per `txnAppId` and skips a write it has already seen. Lesson 3.6
# MAGIC meets the other cure: a MERGE, idempotent by comparison.
# MAGIC
# MAGIC Here is a retry, simulated on a scratch table so silver stays clean.

# COMMAND ----------

SCRATCH = "lakeshore.silver.zz_foreachbatch_retry_lab"
spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")

def append_batch(df, batch_id):
    (df.write.mode("append")
     .option("txnAppId", "orders_gate")      # unique per writing query
     .option("txnVersion", batch_id)         # monotonic: the micro-batch id
     .saveAsTable(SCRATCH))

sample = spark.table("lakeshore.silver.bus_orders").select("order_id").limit(3)
append_batch(sample, 7)                      # batch 7 commits
append_batch(sample, 7)                      # the retry: same batch id, write skipped
after_retry = spark.table(SCRATCH).count()
append_batch(sample, 8)                      # the next real batch goes through
after_next = spark.table(SCRATCH).count()
print(f"after the retry: {after_retry} rows · after batch 8: {after_next} rows")
assert (after_retry, after_next) == (3, 6), "the retry must not double-write"
spark.sql(f"DROP TABLE {SCRATCH}")

# COMMAND ----------

# MAGIC %md
# MAGIC Three rows after the retry, six after the next real batch. Consumer v3 in
# MAGIC lesson 3.5 ships with these two options on both of its writes.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Silent bad data → explicit routing: gate rules with reasons, silver
# MAGIC trustworthy by construction, quarantine as repair queue, bronze replay
# MAGIC as the rebuild mechanism, and `txnAppId`/`txnVersion` as the retry guard.
# MAGIC In Section 8, the same idea becomes three declarative lines: expectations
# MAGIC in Lakeflow pipelines (built on Apache Spark Declarative Pipelines;
# MAGIC formerly Delta Live Tables), which is where the exam guide asks for it.
# MAGIC Next lesson: the bus redelivers — duplicates meet watermarks.

# COMMAND ----------

dbutils.notebook.exit("quality-gate-complete")
