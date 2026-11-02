# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 4.5 — Stream-static joins & the late-data gold
# MAGIC
# MAGIC Finale of the streaming arc: a **gold aggregate fed by a stream** —
# MAGIC 30-minute revenue windows per product category. Two new pieces:
# MAGIC - a **stream-static join** (stream ⋈ `gold.dim_product`) — stateless,
# MAGIC   cheap, and subtly non-deterministic;
# MAGIC - **append-mode windows + watermark** — where everyone's revenue
# MAGIC   "disappears" the first time. We'll watch it happen ON PURPOSE.
# MAGIC
# MAGIC > Prereqs: S3 + lessons 4.2–4.4. Run this notebook from the course
# MAGIC > Git folder.

# COMMAND ----------

import re
import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

TOPICS = "/Volumes/lakeshore/landing/files/topics"

# ---- plumbing from Section 3, compacted (module-izing this is S12's job) --
from pyspark.sql.functions import col, expr, from_json, timestamp_millis, to_date

BUS_SCHEMA = ("key STRING, value STRING, topic STRING, "
              "partition BIGINT, offset BIGINT, timestamp BIGINT")
ORDER_PAYLOAD = ("order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")

def bronze_ingest():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json").schema(BUS_SCHEMA)
            .load(TOPICS)
            .withColumn("event_ts", timestamp_millis(col("timestamp")))
            .withColumn("event_date", to_date(timestamp_millis(col("timestamp"))))
            .withColumn("source_file", col("_metadata.file_path"))
            .writeStream
            .option("checkpointLocation",
                    "/Volumes/lakeshore/landing/files/_autoloader/bus")
            .trigger(availableNow=True).toTable("lakeshore.bronze.bus_raw"))

def route_orders(batch_df, batch_id):
    parsed = batch_df.withColumn(
        "payload", from_json(col("value").cast("string"), ORDER_PAYLOAD))
    checked = parsed.withColumn("reject_reason", expr(
        """CASE WHEN payload IS NULL OR payload.order_id IS NULL
                     THEN 'unparseable payload'
                WHEN payload.customer_id IS NULL THEN 'missing customer_id'
                WHEN exists(payload.items, i -> i.quantity <= 0)
                     THEN 'non-positive quantity' END"""))
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

def orders_v3():
    return (spark.readStream.table("lakeshore.bronze.bus_raw")
            .filter(col("topic") == "orders")
            .withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["key"])
            .writeStream.foreachBatch(route_orders)
            .option("checkpointLocation",
                    "/Volumes/lakeshore/landing/files/_checkpoints/bus_orders")
            .trigger(availableNow=True).start())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The gold stream: explode → static join → windowed agg
# MAGIC
# MAGIC The static side (`gold.dim_product`) is read fresh each micro-batch:
# MAGIC "the latest valid version of data from the static Delta table joins
# MAGIC with the records present in the current micro-batch." Stateless — no
# MAGIC watermark needed *for the join*. (Corollary the exam likes: if the
# MAGIC static table changes between runs, reprocessing the same stream gives
# MAGIC DIFFERENT results — stream-static is non-deterministic by design.)
# MAGIC
# MAGIC The aggregation is another story: event-time windows NEED the
# MAGIC watermark, and in **append mode** a window is written only once —
# MAGIC after the watermark passes its end.

# COMMAND ----------

from pyspark.sql.functions import explode, sum as fsum, window

def gold_stream():
    exploded = (spark.readStream.table("lakeshore.silver.bus_orders")
                .withWatermark("order_ts", "30 minutes")
                .select("order_id", "order_ts",
                        explode(col("items")).alias("item")))
    dim = spark.table("lakeshore.gold.dim_product")          # STATIC side
    return (exploded
            .join(dim, exploded.item.product_id == dim.product_id)  # stream⋈static
            .groupBy(window(col("order_ts"), "30 minutes").alias("w"),
                     col("category"))
            .agg(fsum(col("item.quantity") * col("item.unit_price"))
                 .alias("revenue"),
                 fsum(col("item.quantity")).alias("units"))
            .select(col("w.start").alias("window_start"),
                    col("w.end").alias("window_end"),
                    "category", "revenue", "units")
            .writeStream
            .option("checkpointLocation",
                    "/Volumes/lakeshore/landing/files/_checkpoints/category_revenue")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable("lakeshore.gold.category_revenue_30m"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run it — and go looking for the newest revenue

# COMMAND ----------

from pyspark.sql.functions import max as fmax

def newest_window_status():
    """Is the window containing the newest order present in gold?"""
    newest_order = (spark.table("lakeshore.silver.bus_orders")
                    .agg(fmax("order_ts")).first()[0])
    ws = newest_order.replace(minute=(newest_order.minute // 30) * 30,
                              second=0, microsecond=0)
    present = spark.sql(f"""SELECT count(*)
                            FROM lakeshore.gold.category_revenue_30m
                            WHERE window_start = TIMESTAMP'{ws}'""").first()[0]
    return newest_order, ws, present > 0

spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.gold.category_revenue_30m (
               window_start TIMESTAMP, window_end TIMESTAMP,
               category STRING, revenue DOUBLE, units BIGINT)
             COMMENT '30-min category revenue windows (append mode)'""")
before = spark.table("lakeshore.gold.category_revenue_30m").count()
gold_stream().awaitTermination()
after_first = spark.table("lakeshore.gold.category_revenue_30m").count()
newest_order, ws, present = newest_window_status()
print(f"windows rows: {before} → {after_first}")
print(f"newest order: {newest_order} → its window {ws}: "
      f"{'PRESENT' if present else 'MISSING'}")
assert not present, "the newest window must still be open state"

# COMMAND ----------

# MAGIC %md
# MAGIC **The newest window is MISSING** — the revenue everyone goes looking
# MAGIC for. Two mechanics, say them slowly:
# MAGIC 1. **Append mode** writes a window only when the watermark passes its
# MAGIC    end — until then it's open state, not output.
# MAGIC 2. The watermark **advances between micro-batches** — so what a run
# MAGIC    can emit is decided by the PREVIOUS batch boundary, and the newest
# MAGIC    window is never behind it.
# MAGIC
# MAGIC (Whether *older* windows appeared in this first pass depends on how
# MAGIC the backlog split into micro-batches — an implementation detail. The
# MAGIC invariant you can rely on, and the exam tests, is the one we asserted:
# MAGIC nothing newer than the last batch-boundary watermark ever emits.)
# MAGIC
# MAGIC This is the most-asked streaming behavior in support channels — and a
# MAGIC Professional exam favorite. The cure is what cures everything in
# MAGIC streaming: **more data**.

# COMMAND ----------

# Next bus batch number = highest landed + 1. Each run of this notebook lands one
# more batch on purpose, because only new data can release a window, so a re-take
# moves bronze and silver counts forward. `reset-streaming-labs` in the Section 3
# folder puts them back.
existing = [int(m.group(1)) for f in dbutils.fs.ls(TOPICS)
            if (m := re.search(r"bus_batch_(\d+)\.json$", f.name))]
nxt = max(existing) + 1
feed.emit_batch("bus", TOPICS, nxt)
print(f"emitted bus batch {nxt}")

bronze_ingest().awaitTermination()      # bus file → bronze
orders_v3().awaitTermination()          # bronze → silver (gate + dedup)
gold_stream().awaitTermination()        # silver → gold windows
print("second pass complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT window_start, category, round(revenue, 2) AS revenue, units
# MAGIC FROM lakeshore.gold.category_revenue_30m
# MAGIC ORDER BY window_start, revenue DESC;

# COMMAND ----------

# New windows appeared — and the (new) NEWEST window is still open
after_second = spark.table("lakeshore.gold.category_revenue_30m").count()
newest_order, ws, present = newest_window_status()
n_windows = spark.sql("""SELECT count(DISTINCT window_start)
                         FROM lakeshore.gold.category_revenue_30m""").first()[0]
print(f"windows rows: {after_first} → {after_second} · distinct windows: {n_windows}")
print(f"newest order now: {newest_order} → its window {ws}: "
      f"{'PRESENT' if present else 'MISSING'}")
assert after_second > after_first, "new data must release previously-open windows"
assert not present, "the newest window must still be open state"

# COMMAND ----------

# MAGIC %md
# MAGIC Finalized windows are in the table; the newest window — where data
# MAGIC could still arrive — is deliberately NOT. That's append mode keeping
# MAGIC its promise: **every window is written exactly once, complete.** If
# MAGIC the business wants running values for the open window, that's update
# MAGIC mode, and on Delta that means `foreachBatch` with a MERGE. Lesson 4.7
# MAGIC runs all three modes side by side.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Stream⋈static: latest dim version per micro-batch, stateless,
# MAGIC non-deterministic on reprocess. Windowed gold in append mode: emits
# MAGIC once per window, after the watermark — "missing" recent windows are
# MAGIC correctness. The full chain now runs: bus → bronze → gated deduped
# MAGIC silver → SCD2 + CDF mirrors → joined and windowed gold. Next: lesson 4.7
# MAGIC asks what happens to all that state when the query restarts.

# COMMAND ----------

dbutils.notebook.exit("stream-static-gold-complete")
