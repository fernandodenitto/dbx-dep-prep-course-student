# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 4.4 — Stream-stream joins
# MAGIC
# MAGIC Orders arrive on one topic, payments on another. The business question —
# MAGIC "which orders are paid, and how fast?" — is a JOIN between two live
# MAGIC streams. Stream-stream joins are **stateful**: each side must wait for
# MAGIC the other, state holds candidates in memory, and **watermarks on BOTH
# MAGIC sides** decide how long the wait can last.
# MAGIC
# MAGIC > Prereqs: S3 complete (`bronze.bus_raw` with batches 1–5).

# COMMAND ----------

from pyspark.sql.functions import col, expr, from_json

ORDER_PAYLOAD = ("order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")
PAYMENT_PAYLOAD = ("payment_id STRING, order_id STRING, payment_ts TIMESTAMP, "
                   "amount DOUBLE, method STRING, status_code INT")

def topic_stream(topic, payload_schema):
    """One parsed, deduplicated stream per topic from multiplex bronze."""
    return (spark.readStream.table("lakeshore.bronze.bus_raw")
            .filter(col("topic") == topic)
            .withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["key"])
            .withColumn("payload",
                        from_json(col("value").cast("string"), payload_schema))
            .select("payload.*", col("event_ts")))

orders = topic_stream("orders", ORDER_PAYLOAD).filter("order_id IS NOT NULL")
payments = (topic_stream("payments", PAYMENT_PAYLOAD)
            .withColumnRenamed("event_ts", "pay_event_ts")
            .withColumnRenamed("order_id", "pay_order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Both sides reuse the S3 recipe — topic filter, watermark, dedup, parse.
# MAGIC Both carry a watermarked event-time column. Without them an inner join
# MAGIC still runs, but its state grows forever, so treat them as mandatory; the
# MAGIC docs recommend watermarks on both sides of all stream-stream joins. (The one unparseable order from batch 4 is filtered out —
# MAGIC no join key, no join.)
# MAGIC
# MAGIC ## 1. The join — with a time constraint
# MAGIC
# MAGIC The equality condition alone (`order_id`) would force the engine to
# MAGIC keep EVERY order in state forever — a payment might always arrive. The
# MAGIC **time-interval condition** is what lets state expire: a payment
# MAGIC matches only within 2 hours of its order.

# COMMAND ----------

paired = orders.join(
    payments,
    expr("""order_id = pay_order_id
            AND pay_event_ts BETWEEN event_ts
                                 AND event_ts + INTERVAL 2 HOURS"""),
    "inner")

# COMMAND ----------

CHECKPOINT = "/Volumes/lakeshore/landing/files/_checkpoints/order_payments"

(paired.select("order_id", "customer_id", "order_ts", "payment_id",
               "payment_ts", "amount", "method", "status_code",
               "event_ts", "pay_event_ts")
 .writeStream
 .option("checkpointLocation", CHECKPOINT)
 .trigger(availableNow=True)
 .toTable("lakeshore.silver.bus_order_payments")
 .awaitTermination())

n = spark.table("lakeshore.silver.bus_order_payments").count()
print(f"silver.bus_order_payments: {n} matched pairs")
assert n >= 95, f"expected ~99 matched pairs, got {n}"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, amount, method,
# MAGIC        timestampdiff(MINUTE, order_ts, payment_ts) AS minutes_to_pay
# MAGIC FROM lakeshore.silver.bus_order_payments
# MAGIC ORDER BY minutes_to_pay DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ~99 pairs: every parseable order met its payment (the corrupted order
# MAGIC that lost its `order_id` can never join — its payment waits in state
# MAGIC until the watermark expires it, then is dropped. With an INNER join
# MAGIC that's silent; keep that in mind).
# MAGIC
# MAGIC ## 2. What the exam asks about state here
# MAGIC
# MAGIC - **Watermarks on both sides** — the recommendation is explicit in the
# MAGIC   docs; without them, state grows unbounded. Outer joins can't emit their
# MAGIC   unmatched rows at all without one.
# MAGIC - **The time constraint** (interval condition or event-time window
# MAGIC   equality) is what bounds HOW LONG a row is a join candidate.
# MAGIC - **Outer joins** emit the unmatched row only AFTER the watermark
# MAGIC   passes — that's why left-outer results appear "late": correctness,
# MAGIC   not a bug. Supported: inner, left/right/full outer, left semi.
# MAGIC - **Output mode**: stream-stream joins support append only.
# MAGIC
# MAGIC Try reading the join condition aloud as an SLA: "a payment counts if it
# MAGIC arrives within two hours of its order." When the exam gives you an SLA
# MAGIC sentence like that, it IS the interval condition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Stream⋈stream: watermark both sides + time-interval condition = bounded
# MAGIC state and expiring candidates. Inner joins drop expired singles
# MAGIC silently; outer joins emit them after the watermark. Next: the cheaper
# MAGIC sibling — joining a stream to a static Delta table, and a gold
# MAGIC aggregate that handles late data.

# COMMAND ----------

dbutils.notebook.exit("stream-stream-complete")
