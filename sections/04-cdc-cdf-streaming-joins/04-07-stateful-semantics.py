# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 4.7 — Output modes, checkpoints and exactly-once recovery
# MAGIC
# MAGIC The guide asks for this in one sentence: "Apply Structured Streaming
# MAGIC stateful-processing semantics, including watermarks, output modes,
# MAGIC foreachBatch, and checkpoints for fault-tolerant exactly-once state
# MAGIC recovery." You have used each piece. Today we line them up on one aggregate,
# MAGIC stop the query halfway through a window, restart it, and check the arithmetic.
# MAGIC
# MAGIC Everything here runs on scratch tables named `zz_state_lab_*`, dropped at the
# MAGIC end, so your silver and gold tables are not touched.
# MAGIC
# MAGIC > Prereqs: `silver.bus_orders` with batches 1–4 (lesson 3.5 onward).

# COMMAND ----------

from pyspark.sql.functions import col, count, expr, sum as fsum, window
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

CHECKPOINTS = "/Volumes/lakeshore/landing/files/_checkpoints/zz_state_lab"
SOURCE = "lakeshore.silver.zz_state_lab_orders"
APPEND_T = "lakeshore.gold.zz_state_lab_append"
LIVE_T = "lakeshore.gold.zz_state_lab_update"
COMPLETE_T = "lakeshore.gold.zz_state_lab_complete"

def clean_lab():
    """Drop every scratch object, so this notebook can always start over."""
    for table in (SOURCE, APPEND_T, LIVE_T, COMPLETE_T):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    dbutils.fs.rm(CHECKPOINTS, True)

clean_lab()

# A fixed slice of real orders, numbered by event time, so results match the video
orders = (spark.table("lakeshore.silver.bus_orders")
          .select("order_id", "order_ts",
                  expr("aggregate(items, 0.0D, (acc, i) -> "
                       "acc + i.quantity * i.unit_price)").alias("cart_total"))
          .withColumn("seq", row_number().over(Window.orderBy("order_ts", "order_id")))
          .filter("seq <= 80"))

spark.sql(f"""CREATE TABLE {SOURCE}
              (order_id STRING, order_ts TIMESTAMP, cart_total DOUBLE, seq INT)""")

def land(first, last):
    """Append a slice of orders to the lab source: this is the arriving data."""
    (orders.filter(col("seq").between(first, last))
     .select("order_id", "order_ts", "cart_total", "seq")
     .write.mode("append").saveAsTable(SOURCE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. One aggregate, three output modes
# MAGIC
# MAGIC 30-minute revenue windows with a 5-minute watermark. Each mode gets its own
# MAGIC query and its own checkpoint: "Multiple queries should never share the same
# MAGIC location."
# MAGIC
# MAGIC | Mode | Writes | State |
# MAGIC |---|---|---|
# MAGIC | **append** | a window once, after the watermark passes it | dropped |
# MAGIC | **update** | every row that changed in this trigger | dropped |
# MAGIC | **complete** | every row, rewriting the target | **never dropped** |
# MAGIC
# MAGIC The catch on a Delta target: it "supports append and complete modes but not
# MAGIC update mode". Update on Delta means `foreachBatch` with a MERGE. And that
# MAGIC MERGE has to be idempotent, because `foreachBatch` is at-least-once. Ours is:
# MAGIC it overwrites each window with the value the state store computed.

# COMMAND ----------

def revenue_windows():
    return (spark.readStream.table(SOURCE)
            .withWatermark("order_ts", "5 minutes")
            .groupBy(window(col("order_ts"), "30 minutes").alias("w"))
            .agg(fsum("cart_total").alias("revenue"), count("*").alias("orders"))
            .select(col("w.start").alias("window_start"), "revenue", "orders"))

spark.sql(f"""CREATE TABLE {LIVE_T}
              (window_start TIMESTAMP, revenue DOUBLE, orders BIGINT)""")

def merge_live(batch_df, batch_id):
    batch_df.createOrReplaceTempView("live_batch")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {LIVE_T} AS t
        USING live_batch AS s ON t.window_start = s.window_start
        WHEN MATCHED THEN UPDATE SET revenue = s.revenue, orders = s.orders
        WHEN NOT MATCHED THEN INSERT *""")

def run_append():
    return (revenue_windows().writeStream
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINTS}/append")
            .trigger(availableNow=True)
            .toTable(APPEND_T))

def run_update():
    return (revenue_windows().writeStream
            .outputMode("update")
            .foreachBatch(merge_live)
            .option("checkpointLocation", f"{CHECKPOINTS}/update")
            .trigger(availableNow=True)
            .start())

def run_complete():
    return (revenue_windows().writeStream
            .outputMode("complete")
            .option("checkpointLocation", f"{CHECKPOINTS}/complete")
            .trigger(availableNow=True)
            .toTable(COMPLETE_T))

# Update straight into a Delta table: refused before the query even starts
try:
    (revenue_windows().writeStream.outputMode("update")
     .option("checkpointLocation", f"{CHECKPOINTS}/refused")
     .trigger(availableNow=True).toTable(LIVE_T))
    raise AssertionError("a Delta target should refuse update mode")
except AssertionError:
    raise
except Exception as error:
    print("update mode on a Delta target:", str(error).splitlines()[0][:110])

def run_all():
    """Start all three queries from their checkpoints; this IS a restart."""
    queries = {"append": run_append(), "update": run_update(), "complete": run_complete()}
    for query in queries.values():
        query.awaitTermination()
    return queries

def windows(table):
    return {r.window_start: (round(r.revenue, 2), r.orders)
            for r in spark.table(table).collect()}

def show_all(title):
    print(title)
    for name, table in (("append", APPEND_T), ("update", LIVE_T), ("complete", COMPLETE_T)):
        print(f"  {name:<9}", {str(k)[11:16]: v for k, v in sorted(windows(table).items())})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run one: stop in the middle of a window
# MAGIC
# MAGIC Orders 1–20 fall in the 09:00 window, orders 21–40 in the 09:30 window. We land
# MAGIC orders 1–36 and run: the 09:30 window is still filling when the queries stop.

# COMMAND ----------

land(1, 36)
run_all()
show_all("after run one (orders 1-36):")

first_append = windows(APPEND_T)
newest = max(windows(COMPLETE_T))
assert newest not in first_append, "append must not have written the open window"
assert newest in windows(LIVE_T), "update shows the open window immediately"

# COMMAND ----------

# MAGIC %md
# MAGIC Update and complete already show 09:30, with only 16 of its 20 orders. Append
# MAGIC doesn't: that window isn't final. Same data, three contracts.
# MAGIC
# MAGIC ## 3. The restart
# MAGIC
# MAGIC Our cluster dies here, as far as the query knows. A restart is nothing more
# MAGIC than starting the query again on the same checkpoint. With no new data, it
# MAGIC must produce nothing new. The checkpoint stores the source offsets, the commit
# MAGIC log and the state store, which is everything it needs to know that.

# COMMAND ----------

before = (windows(APPEND_T), windows(LIVE_T), windows(COMPLETE_T))
run_all()
after = (windows(APPEND_T), windows(LIVE_T), windows(COMPLETE_T))
assert before == after, "a restart with no new data must change nothing"
print("restart with no new data: all three targets unchanged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The proof: finish the window after the restart
# MAGIC
# MAGIC Orders 37–40 finish the 09:30 window, and later orders push the watermark past
# MAGIC it. The restarted query reads only these new rows. So if 09:30 comes out
# MAGIC right, its first 16 orders can only have come from state restored out of the
# MAGIC checkpoint. We land two tranches, because a watermark moves at batch boundaries:
# MAGIC the second run is the one that can act on what the first one saw.

# COMMAND ----------

land(37, 60)
run_all()
land(61, 75)
run_all()
show_all("after the restart and orders 37-75:")

truth = {r.window_start: (round(r.revenue, 2), r.orders)
         for r in spark.table(SOURCE)
         .groupBy(window(col("order_ts"), "30 minutes").alias("w"))
         .agg(fsum("cart_total").alias("revenue"), count("*").alias("orders"))
         .select(col("w.start").alias("window_start"), "revenue", "orders").collect()}

append_now = windows(APPEND_T)
assert newest in append_now, "the 09:30 window must now be final"
assert append_now[newest] == truth[newest], (append_now[newest], truth[newest])
assert append_now[newest][1] == 20, "all 20 orders, the 16 from state plus the 4 new"
assert all(append_now[w] == truth[w] for w in append_now), "append must match the batch truth"
assert windows(LIVE_T) == truth, "update, via MERGE, must match the batch truth"
assert windows(COMPLETE_T) == truth, "complete must match the batch truth"
duplicates = spark.table(APPEND_T).groupBy("window_start").count().filter("count > 1").count()
assert duplicates == 0, "append wrote a window twice"
print(f"09:30 window: {append_now[newest]} = batch truth {truth[newest]} · duplicate windows: 0")

# COMMAND ----------

# MAGIC %md
# MAGIC That's the official sample question's answer, measured: the **watermark**
# MAGIC bounds the state, and the **checkpoint** restores offsets and state on restart,
# MAGIC so nothing is double-counted.
# MAGIC
# MAGIC ## 5. What the watermark costs: a very late order
# MAGIC
# MAGIC Now an order from the 09:00 window arrives, long after the watermark passed it.

# COMMAND ----------

first_window = min(truth)
append_before = windows(APPEND_T)[first_window]
spark.sql(f"""INSERT INTO {SOURCE} VALUES
              ('O-LATE', TIMESTAMP'{first_window}' + INTERVAL 5 MINUTES, 100.0, 9999)""")
progress = run_all()

append_after = windows(APPEND_T)[first_window]
complete_after = windows(COMPLETE_T)[first_window]
print(f"09:00 window · append: {append_before} → {append_after} · complete: {complete_after}")
assert append_after == append_before, "append state is gone, so the late order can't change it"
assert complete_after[1] == append_before[1] + 1, "complete kept its state and counted it"

def state_rows(query):
    return query.lastProgress["stateOperators"][0]["numRowsTotal"]

kept = {name: state_rows(q) for name, q in progress.items()}
print("rows held in state:", kept)
assert kept["complete"] > kept["append"], "complete never drops state"

# COMMAND ----------

# MAGIC %md
# MAGIC Append ignored the late order; complete counted it. That is the trade in one
# MAGIC cell. Complete is always right and its state grows forever, and the
# MAGIC `rows held in state` line shows it: `lastProgress` is how you see this on a
# MAGIC real query. For a complete-mode need, the docs point you to a materialized view.
# MAGIC
# MAGIC ## 6. And what the checkpoint was buying
# MAGIC
# MAGIC Delete the append query's checkpoint and run it again. "The next run of the
# MAGIC query begins fresh": no offsets, no state, no memory of what it wrote.

# COMMAND ----------

dbutils.fs.rm(f"{CHECKPOINTS}/append", True)
run_append().awaitTermination()
land(76, 80)                       # a little more data, to move the fresh watermark
run_append().awaitTermination()

doubled = (spark.table(APPEND_T).groupBy("window_start").count()
           .filter("count > 1").count())
print(f"windows written twice after deleting the checkpoint: {doubled}")
assert doubled > 0, "with no checkpoint, the query reprocesses and appends again"

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing failed, and the revenue is now double-counted. That's why the first
# MAGIC question about any "rebuild" is whether the target goes too. It's the ritual
# MAGIC from Section 3: drop the table **and** the checkpoint, replay from bronze.
# MAGIC
# MAGIC ## 7. Clean up

# COMMAND ----------

clean_lab()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap — and the section closes
# MAGIC
# MAGIC Watermark bounds state. Output mode decides when rows are written: append once
# MAGIC and final, update as they change (on Delta, through `foreachBatch` and MERGE),
# MAGIC complete by rewriting everything and keeping all state. The checkpoint restores
# MAGIC offsets and state, and with an idempotent sink that makes exactly-once. The full
# MAGIC chain now runs: bus → bronze → gated deduped silver → SCD2 + CDF mirror → joined
# MAGIC and windowed gold. Next section sharpens the language itself: Python and SQL at
# MAGIC Professional depth.

# COMMAND ----------

dbutils.notebook.exit("stateful-semantics-complete")
