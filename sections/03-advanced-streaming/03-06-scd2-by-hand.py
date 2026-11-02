# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.6 — SCD2 by hand: MERGE + foreachBatch
# MAGIC
# MAGIC The customers topic becomes a real **SCD Type 2 dimension**:
# MAGIC `silver.dim_customer_scd2` (the shell we designed in Section 2). Today
# MAGIC we write the close-and-insert choreography ourselves — every line of it
# MAGIC — so that when AUTO CDC replaces it with one clause in Section 8, you
# MAGIC know exactly what that clause is doing.
# MAGIC
# MAGIC > Prereqs: lesson 2.5 (the shell table) and this section so far.
# MAGIC > Run this notebook from the course Git folder.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

CDC_PATH = "/Volumes/lakeshore/landing/files/cdc/customers"
CHECKPOINT = "/Volumes/lakeshore/landing/files/_checkpoints/cdc_customers"

# Idempotent guard: the S2 shell, exactly as designed there
spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.silver.dim_customer_scd2 (
               customer_id STRING NOT NULL,
               first_name  STRING, last_name STRING, email STRING,
               valid_from  TIMESTAMP NOT NULL,
               valid_to    TIMESTAMP,
               is_current  BOOLEAN NOT NULL)
             CLUSTER BY (customer_id)
             COMMENT 'Customer dimension, SCD Type 2 — maintained by the S3 CDC stream'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The feed, again
# MAGIC
# MAGIC CDC events: `operation`, `sequence_num` (commit order), `event_ts`,
# MAGIC full row image. Batch 1 has been sitting in `cdc/customers/` since
# MAGIC Section 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT operation, sequence_num, customer_id, email
# MAGIC FROM json.`/Volumes/lakeshore/landing/files/cdc/customers/`
# MAGIC ORDER BY sequence_num LIMIT 8;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The choreography, in words first
# MAGIC
# MAGIC Per micro-batch:
# MAGIC 1. **Collapse** to the latest event per customer (by `sequence_num`) —
# MAGIC    batch-grain versioning: one version per key per batch.
# MAGIC 2. **Drop no-ops**: an event that wouldn't change the current version
# MAGIC    must stage NOTHING (or it would open a duplicate current row).
# MAGIC 3. **Stage** two kinds of rows: *close* rows (match the current version
# MAGIC    → stamp `valid_to`, drop the flag) and *insert* rows (the new
# MAGIC    version — merge key NULL so they never match).
# MAGIC 4. **One MERGE** does both: matched → close; not-matched inserts →
# MAGIC    new current version. DELETEs close without inserting.

# COMMAND ----------

from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

CDC_SCHEMA = ("operation STRING, sequence_num BIGINT, event_ts TIMESTAMP, "
              "customer_id STRING, first_name STRING, last_name STRING, "
              "email STRING")

def scd2_apply(batch_df, batch_id):
    """Apply one micro-batch of CDC events to the SCD2 dimension."""
    latest = (batch_df
              .withColumn("rn", row_number().over(
                  Window.partitionBy("customer_id")
                        .orderBy(col("sequence_num").desc())))
              .filter("rn = 1").drop("rn"))

    # Drop NO-OP events: an UPDATE that changes nothing must stage nothing,
    # or its insert row would open a second "current" version.
    current = (batch_df.sparkSession
               .table("lakeshore.silver.dim_customer_scd2")
               .filter("is_current")
               .select(col("customer_id"),
                       col("first_name").alias("cur_first"),
                       col("last_name").alias("cur_last"),
                       col("email").alias("cur_email")))
    changed = (latest.join(current, "customer_id", "left")
               .filter("operation = 'DELETE' OR cur_email IS NULL "
                       "OR email <> cur_email OR last_name <> cur_last "
                       "OR first_name <> cur_first")
               .drop("cur_first", "cur_last", "cur_email"))

    closes = (changed
              .withColumn("merge_key", col("customer_id"))
              .withColumn("stage_role", lit("close")))
    inserts = (changed.filter("operation != 'DELETE'")
               .withColumn("merge_key", lit(None).cast("string"))
               .withColumn("stage_role", lit("insert")))
    staged = closes.unionByName(inserts)
    staged.createOrReplaceTempView("staged_changes")

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

# COMMAND ----------

# MAGIC %md
# MAGIC Read the MERGE slowly on camera — every clause earns its keep:
# MAGIC - `ON … merge_key AND is_current` — only the OPEN version can match.
# MAGIC - Matched + changed (or DELETE) → the open version is **closed**.
# MAGIC - Not matched + role 'insert' → the **new version opens**. Insert rows
# MAGIC   carry a NULL merge key precisely so they land here.
# MAGIC - An unchanged customer matches nothing meaningful and nothing happens
# MAGIC   — idempotent by comparison.
# MAGIC
# MAGIC One subtlety we accept openly: a new version's row is inserted in the
# MAGIC same MERGE that closes the old one — but the close fires only when
# MAGIC something actually changed, so replaying identical events does nothing.
# MAGIC
# MAGIC That is the second cure for `foreachBatch` being at-least-once: lesson 3.4
# MAGIC used `txnAppId`/`txnVersion` for blind appends; here a retried micro-batch
# MAGIC re-runs a MERGE that finds nothing left to change.

# COMMAND ----------

def run_scd2():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(CDC_SCHEMA)
            .load(CDC_PATH)
            .writeStream
            .foreachBatch(scd2_apply)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start())

run_scd2().awaitTermination()
print("batch 1 applied")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_current, count(*) AS versions
# MAGIC FROM lakeshore.silver.dim_customer_scd2 GROUP BY is_current;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. More changes arrive — history accumulates

# COMMAND ----------

feed.emit_batch("cdc", CDC_PATH, 2, entity="customers")
run_scd2().awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A customer with real history: every version, in order
# MAGIC SELECT customer_id, email, valid_from, valid_to, is_current
# MAGIC FROM lakeshore.silver.dim_customer_scd2
# MAGIC WHERE customer_id IN (
# MAGIC   SELECT customer_id FROM lakeshore.silver.dim_customer_scd2
# MAGIC   GROUP BY customer_id HAVING count(*) >= 2)
# MAGIC ORDER BY customer_id, valid_from LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Read one customer's rows aloud: version 1 closed at exactly the
# MAGIC timestamp version 2 opened — a seamless validity chain. That chain is
# MAGIC what makes point-in-time joins exact:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- "Which version of this customer was true at 2026-08-01 10:00?"
# MAGIC SELECT customer_id, email, valid_from, valid_to
# MAGIC FROM lakeshore.silver.dim_customer_scd2
# MAGIC WHERE TIMESTAMP'2026-08-01 10:00:00' >= valid_from
# MAGIC   AND (valid_to IS NULL OR TIMESTAMP'2026-08-01 10:00:00' < valid_to)
# MAGIC LIMIT 5;

# COMMAND ----------

# The SCD2 invariants — the assertions that make this a dimension, not a log
current_dupes = spark.sql("""
    SELECT count(*) FROM (
      SELECT customer_id FROM lakeshore.silver.dim_customer_scd2
      WHERE is_current GROUP BY customer_id HAVING count(*) > 1)""").first()[0]
open_closed = spark.sql("""
    SELECT count(*) FROM lakeshore.silver.dim_customer_scd2
    WHERE NOT is_current AND valid_to IS NULL""").first()[0]
deleted_still_current = spark.sql("""
    WITH last_events AS (
      SELECT customer_id, operation
      FROM json.`/Volumes/lakeshore/landing/files/cdc/customers/`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY sequence_num DESC) = 1)
    SELECT count(*)
    FROM lakeshore.silver.dim_customer_scd2 t
    JOIN last_events e USING (customer_id)
    WHERE t.is_current AND e.operation = 'DELETE'""").first()[0]
total = spark.table("lakeshore.silver.dim_customer_scd2").count()
print(f"rows: {total} · dup currents: {current_dupes} · "
      f"closed-without-valid_to: {open_closed} · deleted-still-current: {deleted_still_current}")
assert current_dupes == 0 and open_closed == 0 and deleted_still_current == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC You just wrote what most engineers copy-paste: collapse to latest per
# MAGIC key, stage close+insert rows, one MERGE against the open versions,
# MAGIC DELETEs close without reopening. It works, it's replayable, and it's
# MAGIC ~40 lines you now own. Remember the feeling — in Section 8 AUTO CDC
# MAGIC (formerly APPLY CHANGES) will do all of it with
# MAGIC `STORED AS SCD TYPE 2`, and you'll know precisely what you're
# MAGIC delegating. Section 4 first: CDC feeds that arrive out of order, and
# MAGIC the ranking discipline that survives them.

# COMMAND ----------

dbutils.notebook.exit("scd2-by-hand-complete")
