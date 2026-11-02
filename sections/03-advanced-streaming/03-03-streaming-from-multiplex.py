# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.3 — Streaming from multiplex: the first silver consumer
# MAGIC
# MAGIC Bronze holds every topic, payloads unparsed. Now the first consumer:
# MAGIC **orders** — filter the topic, parse the payload with a DECLARED schema,
# MAGIC write `lakeshore.silver.bus_orders`.
# MAGIC
# MAGIC This is consumer **v1 — naive on purpose**: no quality gate, no dedup.
# MAGIC The next two lessons break it with production data and fix it properly.
# MAGIC
# MAGIC > Prereqs: `bronze.bus_raw` populated (lesson 3.2).

# COMMAND ----------

# Start from a clean consumer, so re-taking this lesson gives the same result.
# A later lesson may have left a different query shape on this checkpoint, and
# gold streams FROM this table: replaying it invalidates them too.
CHECKPOINT = "/Volumes/lakeshore/landing/files/_checkpoints/bus_orders"
spark.sql("DROP TABLE IF EXISTS lakeshore.silver.bus_orders")
spark.sql("DROP TABLE IF EXISTS lakeshore.gold.category_revenue_30m")
dbutils.fs.rm(CHECKPOINT, True)
dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/category_revenue", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read bronze as a stream
# MAGIC
# MAGIC A Delta table written by an append stream is itself a valid streaming
# MAGIC source — that's the multiplex fan-out: bronze absorbs the bus once,
# MAGIC every consumer streams from the TABLE.

# COMMAND ----------

from pyspark.sql.functions import col, from_json

bus = spark.readStream.table("lakeshore.bronze.bus_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Filter the topic, parse the payload
# MAGIC
# MAGIC Two moves, in order. The topic filter rides the liquid clustering from
# MAGIC last lesson. Then `from_json` with a schema WE declare — at silver,
# MAGIC types are a contract. (On real Kafka `value` arrives as BINARY, so the
# MAGIC cast below is load-bearing; on our simulated feed it's already a
# MAGIC string — we keep the cast so the muscle memory is right.)

# COMMAND ----------

ORDER_PAYLOAD = ("order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")

orders_stream = (bus
                 .filter(col("topic") == "orders")
                 .withColumn("payload",
                             from_json(col("value").cast("string"), ORDER_PAYLOAD))
                 .select("payload.order_id", "payload.customer_id",
                         "payload.order_ts", "payload.items",
                         "event_ts", "offset", "source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC What we kept beyond the payload: `event_ts` (bus event time — the
# MAGIC watermark lesson needs it), `offset` and `source_file` (lineage back to
# MAGIC the transport). What happens if `value` can't be parsed? `from_json`
# MAGIC returns NULL — silently. Hold that thought for the next lesson.
# MAGIC
# MAGIC ## 3. Write silver — availableNow, own checkpoint

# COMMAND ----------

(orders_stream.writeStream
 .option("checkpointLocation", CHECKPOINT)
 .trigger(availableNow=True)
 .toTable("lakeshore.silver.bus_orders")
 .awaitTermination())

n = spark.table("lakeshore.silver.bus_orders").count()
print(f"silver.bus_orders: {n} orders")
assert n >= 60, f"expected ≥60 orders (batches 1-3), got {n}"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, order_ts, size(items) AS n_items, offset
# MAGIC FROM lakeshore.silver.bus_orders
# MAGIC ORDER BY order_ts DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Typed columns, real array of structs, transport lineage — from a string
# MAGIC payload. And note the count: 60 orders from batches 1–3 (more if you are
# MAGIC re-taking this after later lessons landed more batches) — the payments
# MAGIC and customer topics flowed past our filter untouched, still waiting in
# MAGIC bronze for THEIR consumers.
# MAGIC
# MAGIC ## 4. Prove the fan-out costs nothing extra

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze still holds everything — consuming orders removed nothing
# MAGIC SELECT topic, count(*) FROM lakeshore.bronze.bus_raw GROUP BY topic;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC The consumer pattern: `readStream.table(bronze)` → topic filter →
# MAGIC `from_json` with declared schema → own checkpoint → silver. It's v1,
# MAGIC and it's naive: it trusts every payload and every delivery. Next
# MAGIC lesson, the bus sends what production actually sends — and we build the
# MAGIC quality gate.

# COMMAND ----------

dbutils.notebook.exit("bus-orders-v1-complete")
