# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.2 — Multiplex bronze
# MAGIC
# MAGIC One Auto Loader stream. Every topic. One liquid-clustered bronze table:
# MAGIC `lakeshore.bronze.bus_raw`. This is the table three silver pipelines
# MAGIC will fan out from.
# MAGIC
# MAGIC > Prereqs: fast-forward run; this notebook run from the course Git
# MAGIC > folder, so it can find `datagen/`; S1's bus batches 1–2 already in `topics/`.
# MAGIC >
# MAGIC > "Multiplex" is this course's name for the pattern, not an exam term: the
# MAGIC > guide says "message buses (Kafka, Kinesis, Pub/Sub)". The skill the exam
# MAGIC > tests is ingesting from a bus; one table per bus is how we organise it.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

TOPICS_PATH = "/Volumes/lakeshore/landing/files/topics"
CHECKPOINT = "/Volumes/lakeshore/landing/files/_autoloader/bus"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The bronze contract: declared envelope, clustered layout
# MAGIC
# MAGIC The bus envelope is stable — that's the transport's contract, so we
# MAGIC DECLARE it (payloads inside `value` stay unparsed strings: bronze keeps
# MAGIC raw). Layout: **liquid clustering** on `(topic, event_date)` — every
# MAGIC silver consumer filters on topic, every retention job on date.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS lakeshore.bronze.bus_raw (
# MAGIC   key         STRING,
# MAGIC   value       STRING,
# MAGIC   topic       STRING,
# MAGIC   partition   BIGINT,
# MAGIC   offset      BIGINT,
# MAGIC   timestamp   BIGINT,
# MAGIC   event_ts    TIMESTAMP,
# MAGIC   event_date  DATE,
# MAGIC   source_file STRING
# MAGIC )
# MAGIC CLUSTER BY (topic, event_date)
# MAGIC COMMENT 'Multiplex bronze — full bus envelope, payloads unparsed';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The ingestion stream — Auto Loader (`cloudFiles`)
# MAGIC
# MAGIC Auto Loader tracks files in a checkpoint (RocksDB) so each file is
# MAGIC ingested **exactly once** into Delta — re-runs cost nothing. The
# MAGIC envelope schema is passed explicitly; `timestamp_millis` turns the bus
# MAGIC epoch into a real timestamp for clustering and watermarks later.

# COMMAND ----------

from pyspark.sql.functions import col, timestamp_millis, to_date

BUS_SCHEMA = ("key STRING, value STRING, topic STRING, "
              "partition BIGINT, offset BIGINT, timestamp BIGINT")

def ingest_bus():
    """One availableNow pass: land everything new, then stop."""
    stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .schema(BUS_SCHEMA)
              .load(TOPICS_PATH)
              .withColumn("event_ts", timestamp_millis(col("timestamp")))
              .withColumn("event_date", to_date(timestamp_millis(col("timestamp"))))
              .withColumn("source_file", col("_metadata.file_path")))
    return (stream.writeStream
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .toTable("lakeshore.bronze.bus_raw"))

ingest_bus().awaitTermination()
print("first pass complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT topic, count(*) AS records,
# MAGIC        min(event_date) AS first_day, max(event_date) AS last_day
# MAGIC FROM lakeshore.bronze.bus_raw
# MAGIC GROUP BY topic ORDER BY topic;

# COMMAND ----------

# MAGIC %md
# MAGIC Three topics, one table — the multiplex shape from Section 2, now real.
# MAGIC
# MAGIC ## 3. New data arrives — the stream picks up ONLY the new file

# COMMAND ----------

before = spark.table("lakeshore.bronze.bus_raw").count()
feed.emit_batch("bus", TOPICS_PATH, 3)          # deterministic batch 3
ingest_bus().awaitTermination()
after = spark.table("lakeshore.bronze.bus_raw").count()
print(f"{before} → {after} records (+{after - before} from bus_batch_0003)")
assert after - before in (0, 43), "expected exactly batch 3 (43 records) or a no-op re-run"

# COMMAND ----------

# MAGIC %md
# MAGIC `+43`: twenty orders, their twenty payments, three customer updates —
# MAGIC one file, discovered and ingested once. (Run this cell a second time and
# MAGIC you get `+0`: the checkpoint remembers, so Auto Loader ignores the
# MAGIC re-emitted file. To see `+43` again on a re-take, run `reset-streaming-labs`
# MAGIC in this folder first.)
# MAGIC
# MAGIC ## 4. Look at the layout

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED lakeshore.bronze.bus_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC Find `Clustering Information` in the output: `topic, event_date`. Every
# MAGIC `WHERE topic = 'orders'` a silver consumer runs from now on skips files
# MAGIC that hold only payments and customers — layout serving the read pattern
# MAGIC we designed in S2.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC One stream, one checkpoint, one clustered table, three (and someday
# MAGIC thirty) topics. Silver consumers now fan out FROM the table, not from
# MAGIC the bus. Next: the first consumer — orders, parsed with a declared
# MAGIC schema.

# COMMAND ----------

dbutils.notebook.exit("multiplex-bronze-complete")
