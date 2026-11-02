# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 4.3 — Change data feed
# MAGIC
# MAGIC Our SCD2 dimension is maintained by MERGE. Now a downstream team wants a
# MAGIC simple "current customers" mirror, fed *incrementally*. Watch the
# MAGIC problem: **Structured Streaming refuses tables with updates** — and then
# MAGIC the sanctioned answer: **change data feed (CDF)**, which the guide asks for
# MAGIC as "to expose row-level changes (updates/deletes) for efficient incremental
# MAGIC downstream processing."
# MAGIC
# MAGIC > Prereqs: lessons through 4.2 (dimension live with CDC batches 1–4).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The wall, first
# MAGIC
# MAGIC A Delta table is a valid streaming source **only while it's
# MAGIC append-only**: "Structured Streaming only accepts append inputs and
# MAGIC throws an exception if any modifications occur on the source". Our
# MAGIC dimension gets UPDATEs on every close — streaming it directly will
# MAGIC fail as soon as it hits a changed commit. (`skipChangeCommits` exists,
# MAGIC but it *skips* exactly the changes we want to propagate.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The feed: nothing to turn on
# MAGIC
# MAGIC The current form is **automatic change data feed**, generally available since
# MAGIC 1 September 2026. It computes row-level changes at query time, from row tracking,
# MAGIC and needs no per-table setting if the table qualifies: Unity Catalog, Databricks
# MAGIC Runtime 19 or above, row tracking enabled. Check that ours does.

# COMMAND ----------

row_tracking = {r.key: r.value for r in
                spark.sql("SHOW TBLPROPERTIES lakeshore.silver.dim_customer_scd2").collect()
                }.get("delta.enableRowTracking")
print("delta.enableRowTracking:", row_tracking)

# A table created before row tracking was the default needs it switched on, once.
# Changes can then be read only from that version forward.
if row_tracking != "true":
    spark.sql("""ALTER TABLE lakeshore.silver.dim_customer_scd2
                 SET TBLPROPERTIES (delta.enableRowTracking = true)""")

enabled_at = (spark.sql("DESCRIBE HISTORY lakeshore.silver.dim_customer_scd2")
              .filter("operation = 'SET TBLPROPERTIES' AND "
                      "operationParameters.properties LIKE '%enableRowTracking%'")
              .agg({"version": "max"}).first()[0])
cdf_start = enabled_at if enabled_at is not None else 0
print("reading changes from version", cdf_start)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Make some changes worth watching

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import feed

CDC_PATH = "/Volumes/lakeshore/landing/files/cdc/customers"

feed.emit_batch("cdc", CDC_PATH, 5, entity="customers")

# COMMAND ----------

# The hardened v2 pipeline from lesson 4.2, inlined once more. (Yes, we're
# copy-pasting pipeline code between notebooks for the third time — feel the
# friction: Section 12 turns this into a proper Python module, imported.)
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

CDC_SCHEMA = ("operation STRING, sequence_num BIGINT, event_ts TIMESTAMP, "
              "customer_id STRING, first_name STRING, last_name STRING, "
              "email STRING")

def scd2_apply_v2(batch_df, batch_id):
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
               .filter("cur_valid_from IS NULL OR event_ts > cur_valid_from")
               .filter("operation = 'DELETE' OR cur_email IS NULL "
                       "OR email <> cur_email OR last_name <> cur_last "
                       "OR first_name <> cur_first")
               .drop("cur_first", "cur_last", "cur_email", "cur_valid_from"))
    closes = (changed.withColumn("merge_key", col("customer_id"))
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
            .option("checkpointLocation",
                    "/Volumes/lakeshore/landing/files/_checkpoints/cdc_customers")
            .trigger(availableNow=True)
            .start())

run_scd2_v2().awaitTermination()
print("batch 5 applied — the dimension changed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read the changes

# COMMAND ----------

changes = spark.sql(f"""
    SELECT _change_type, _commit_version, customer_id, email, is_current
    FROM table_changes('lakeshore.silver.dim_customer_scd2', {cdf_start})
    ORDER BY _commit_version DESC, customer_id
""")
display(changes.limit(12))
display(changes.groupBy("_change_type").count().orderBy("_change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC Four `_change_type` values — know them cold for the exam:
# MAGIC - `insert` — new rows (our new versions and brand-new customers)
# MAGIC - `update_preimage` — the row BEFORE an update (our closes: was current)
# MAGIC - `update_postimage` — the row AFTER (now closed, valid_to stamped)
# MAGIC - `delete` — physically deleted rows (none on this table: SCD2 closes, it doesn't delete)
# MAGIC
# MAGIC Plus `_commit_version` and `_commit_timestamp` for ordering. Pre/post
# MAGIC pairs are what make MERGE-driven tables streamable: every UPDATE
# MAGIC becomes an explicit before/after record.
# MAGIC
# MAGIC ## 5. Deletes too — and the legacy form you'll meet on the exam
# MAGIC
# MAGIC Our dimension never shows a `delete` row, so here are all four types on two scratch
# MAGIC tables. The second uses the **legacy** form: the table property
# MAGIC `delta.enableChangeDataFeed`, which records changes at write time, in extra files. It's
# MAGIC what older material and many exam stems describe. Same statements, same read API.

# COMMAND ----------

def all_four_types(table, properties=""):
    """Insert, update and delete on a scratch table, then read the change feed."""
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} (id INT, status STRING) {properties}")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'new'), (2, 'new'), (3, 'new')")
    spark.sql(f"UPDATE {table} SET status = 'shipped' WHERE id = 2")
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    rows = spark.sql(f"SELECT id, status, _change_type FROM table_changes('{table}', 0)").collect()
    spark.sql(f"DROP TABLE {table}")
    return sorted((r.id, r.status, r._change_type) for r in rows)

automatic = all_four_types("lakeshore.silver.zz_cdf_auto_lab")
legacy = all_four_types("lakeshore.silver.zz_cdf_legacy_lab",
                        "TBLPROPERTIES (delta.enableChangeDataFeed = true)")
for change in automatic:
    print(change)
assert automatic == legacy, (automatic, legacy)
assert {c[2] for c in automatic} == {"insert", "update_preimage", "update_postimage", "delete"}

# COMMAND ----------

# MAGIC %md
# MAGIC Identical answers, different mechanics:
# MAGIC
# MAGIC | | Automatic | Legacy |
# MAGIC |---|---|---|
# MAGIC | Setup | None: row tracking, Runtime 19+ | `delta.enableChangeDataFeed = true` |
# MAGIC | Changes computed | At query time | At write time, in extra files |
# MAGIC | History | Doesn't depend on a switch-on date | **Only from the version you enabled it** |
# MAGIC | Row filters, column masks | **Not supported** | See the docs |
# MAGIC | Reads | `table_changes`, `readChangeFeed` | The same |
# MAGIC
# MAGIC Databricks recommends migrating to automatic; you can't use both on one table.
# MAGIC
# MAGIC ## 6. The incremental consumer: a current-customers mirror

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.gold.customer_current (
               customer_id STRING NOT NULL,
               first_name STRING, last_name STRING, email STRING,
               refreshed_from_version BIGINT)
             COMMENT 'SCD1 mirror of the SCD2 dimension, fed by CDF'""")

from pyspark.sql.functions import col

def mirror_current(batch_df, batch_id):
    """CDF micro-batch → upsert the mirror, and remove what stopped being current."""
    # final images of rows, and physical deletes of rows that were current
    relevant = batch_df.filter(
        col("_change_type").isin("insert", "update_postimage")
        | ((col("_change_type") == "delete") & col("is_current")))
    relevant.createOrReplaceTempView("cdf_batch")
    batch_df.sparkSession.sql("""
        MERGE INTO lakeshore.gold.customer_current AS t
        USING (SELECT customer_id, first_name, last_name, email, keep,
                      _commit_version AS v
               FROM (SELECT *, is_current AND _change_type <> 'delete' AS keep
                     FROM cdf_batch)
               QUALIFY ROW_NUMBER() OVER (
                 PARTITION BY customer_id
                 ORDER BY _commit_version DESC, keep DESC) = 1) AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND NOT s.keep THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          first_name = s.first_name, last_name = s.last_name,
          email = s.email, refreshed_from_version = s.v
        WHEN NOT MATCHED AND s.keep THEN INSERT
          (customer_id, first_name, last_name, email, refreshed_from_version)
          VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.v)
    """)

(spark.readStream
 .option("readChangeFeed", "true")
 .option("startingVersion", cdf_start)
 .table("lakeshore.silver.dim_customer_scd2")
 .writeStream
 .foreachBatch(mirror_current)
 .option("checkpointLocation",
         "/Volumes/lakeshore/landing/files/_checkpoints/customer_current")
 .trigger(availableNow=True)
 .start()
 .awaitTermination())

n = spark.table("lakeshore.gold.customer_current").count()
current = spark.table("lakeshore.silver.dim_customer_scd2").filter("is_current").count()
print(f"gold.customer_current: {n} rows · current versions in the dimension: {current}")
assert n == current, "the mirror must match the dimension's current rows, closes included"

# COMMAND ----------

# MAGIC %md
# MAGIC The mirror saw ONLY change rows — no full scan of the dimension, ever. And a
# MAGIC customer whose latest change closed the row with no successor, a CDC `DELETE`,
# MAGIC leaves the mirror. Row-level updates *and* deletes, propagated incrementally, which
# MAGIC is the guide's sentence. Run this cell again and the MERGE finds nothing to do.
# MAGIC
# MAGIC ## 7. One retention warning, on the record

# COMMAND ----------

# MAGIC %md
# MAGIC Change records are transient, available only within a retention window. On legacy
# MAGIC the mechanism is explicit: **VACUUM deletes change data files**. A consumer that
# MAGIC falls behind further than that window loses its history and must re-bootstrap from
# MAGIC the table itself. CDF is a conveyor belt, not an archive; for permanent history, the
# MAGIC docs say to write the changes to a table of your own.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Automatic CDF needs row tracking and no property; legacy needs the property and
# MAGIC isn't retroactive. Read either with `table_changes` (SQL) or `readChangeFeed`
# MAGIC (streaming); four change types with pre/post images; consumers become incremental
# MAGIC MERGEs that handle deletes too; retention bounds the belt. Next: joining two live
# MAGIC streams — orders meet payments.

# COMMAND ----------

dbutils.notebook.exit("cdf-complete")
