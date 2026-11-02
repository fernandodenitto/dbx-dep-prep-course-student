# Databricks notebook source
# MAGIC %md
# MAGIC # 6.4 — Append-only pipelines: batch and streaming, one codebase
# MAGIC
# MAGIC The exam doesn't ask for this pattern by name any more. It stays because it teaches
# MAGIC the checkpoint and trigger semantics the guide does name, and because Section 10 needs
# MAGIC the table it builds. It's ONE pipeline: on classic compute the cadence is a trigger
# MAGIC option, not an architecture. **On serverless, including Free Edition, only one half of
# MAGIC that sentence runs**, and this notebook shows you the error.
# MAGIC
# MAGIC Our subject is real: the **delete-requests feed** (landed in S1) —
# MAGIC the compliance queue Section 10 will act on. Today it gets its
# MAGIC canonical bronze table.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

DELETES = "/Volumes/lakeshore/landing/files/deletes"
CHECKPOINT = "/Volumes/lakeshore/landing/files/_autoloader/delete_requests"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The pipeline — written once
# MAGIC
# MAGIC Declared schema (the envelope is a contract), Auto Loader discovery,
# MAGIC Delta sink. Nothing here says "batch" or "streaming": that decision
# MAGIC lives in ONE line, the trigger.

# COMMAND ----------

from pyspark.sql.functions import col

DELETE_SCHEMA = ("request_id STRING, customer_id STRING, "
                 "requested_at TIMESTAMP")

def ingest_deletes(trigger_kwargs):
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(DELETE_SCHEMA)
            .load(DELETES)
            .withColumn("source_file", col("_metadata.file_path"))
            .writeStream
            .option("checkpointLocation", CHECKPOINT)
            .trigger(**trigger_kwargs)
            .toTable("lakeshore.bronze.delete_requests"))

# batch cadence: process the backlog, then stop
ingest_deletes({"availableNow": True}).awaitTermination()
n1 = spark.table("lakeshore.bronze.delete_requests").count()
print(f"bronze.delete_requests: {n1} requests after first pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. New data, same pipeline, batch cadence

# COMMAND ----------

feed.emit_batch("deletes", DELETES, 2, batch_size=5)
ingest_deletes({"availableNow": True}).awaitTermination()
n2 = spark.table("lakeshore.bronze.delete_requests").count()
print(f"{n1} → {n2}")
assert n2 >= 10, f"expected ≥10 (batches 1-2), got {n2}"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, customer_id, requested_at
# MAGIC FROM lakeshore.bronze.delete_requests ORDER BY requested_at;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The trigger table, and which rows run where
# MAGIC
# MAGIC | Trigger | Behavior | Serverless / Free Edition | Classic compute |
# MAGIC |---|---|---|---|
# MAGIC | `availableNow=True` | process ALL pending, then stop | ✅ the recommended one | ✅ |
# MAGIC | `processingTime="5 minutes"` | micro-batch every interval, forever | ❌ error | ✅ |
# MAGIC | *(no trigger set)* | same as `processingTime="0 seconds"` | ❌ error | ✅ |
# MAGIC | `once=True` *(deprecated)* | one micro-batch, then stop | runs, not recommended | runs, not recommended |
# MAGIC
# MAGIC `availableNow` beats the deprecated `once` because it respects rate limits
# MAGIC (`maxFilesPerTrigger` and friends) by splitting the backlog into MULTIPLE micro-batches.
# MAGIC `once` crams everything into one.
# MAGIC
# MAGIC Now the row that surprises people. The next cell asks this compute for an interval
# MAGIC trigger. It writes to a `noop` sink with its own checkpoint, so the real table can't be
# MAGIC touched either way.

# COMMAND ----------

TRIGGER_CKPT = "/Volumes/lakeshore/landing/files/zoo/_checkpoints/trigger_flip"

def try_interval_trigger():
    """Start the same read on a processingTime trigger, and report what the compute says."""
    query = None
    try:
        query = (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .schema(DELETE_SCHEMA)
                 .load(DELETES)
                 .writeStream.format("noop")
                 .option("checkpointLocation", TRIGGER_CKPT)
                 .trigger(processingTime="5 minutes")
                 .start())
        query.awaitTermination(15)
        return "started: this compute accepts interval triggers (classic)"
    except Exception as error:
        return str(error).split("\n")[0][:160]
    finally:
        if query is not None and query.isActive:
            query.stop()

print(try_interval_trigger())

# COMMAND ----------

# MAGIC %md
# MAGIC On serverless that prints `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. Leaving the
# MAGIC trigger off fails the same way, because no trigger means `processingTime="0 seconds"`.
# MAGIC
# MAGIC So "flip one line and it's streaming" is true on classic compute and false here. On
# MAGIC serverless the docs give you two low-latency routes: a **Lakeflow pipeline in continuous
# MAGIC mode**, or `availableNow` inside a **continuous job**. Those restrictions don't apply to
# MAGIC pipelines at all, which is one more reason Section 8 exists.
# MAGIC
# MAGIC ## 4. Why this is exactly-once
# MAGIC
# MAGIC Two guarantees compose: Auto Loader's checkpoint records which FILES
# MAGIC were ingested (a re-listed file is never re-read), and "the Delta Lake
# MAGIC transaction log guarantees exactly-once processing, even when there
# MAGIC are other streams or batch queries running concurrently against the
# MAGIC table." You just saw it: batch 2 ran through the same call, and only
# MAGIC the new file came in.

# COMMAND ----------

# The append-only property, verified: this table only ever grows
history = spark.sql(
    "DESCRIBE HISTORY lakeshore.bronze.delete_requests").select("operation")
ops = {r["operation"] for r in history.collect()}
forbidden = ops & {"DELETE", "UPDATE", "MERGE"}
assert not forbidden, f"append-only violated: {forbidden}"
print(f"✓ operations so far: {sorted(ops)} — append-only holds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC One declared-schema Auto Loader pipeline; cadence = a trigger option on classic compute,
# MAGIC and `availableNow` only on serverless; exactly-once = checkpoint × Delta log; append-only
# MAGIC verified from the table history. `bronze.delete_requests` is now live, and Section 10
# MAGIC turns it into deletions. Next: the acquisition you DON'T build, Lakeflow Connect.

# COMMAND ----------

dbutils.notebook.exit("append-only-unified-complete")
