# Databricks notebook source
# MAGIC %md
# MAGIC # 7.2 — Partitioning vs Z-ORDER vs liquid clustering
# MAGIC
# MAGIC The layout fork, settled with files on disk instead of opinions. One synthetic workload
# MAGIC (1M events, a high-cardinality `device_id`, a 30-day `event_date`) laid out three ways,
# MAGIC then migrated, re-keyed and compacted.
# MAGIC
# MAGIC > Demo tables (`bronze.layout_*`), dropped at the end.

# COMMAND ----------

from pyspark.sql.functions import col, expr

BRONZE = "lakeshore.bronze"
DEMOS = ("layout_part_device", "layout_part_date", "layout_zorder", "layout_liquid",
         "layout_rekey", "layout_auto", "layout_small")

def detail(table):
    """The three numbers this lesson keeps asking for."""
    d = spark.sql(f"DESCRIBE DETAIL {BRONZE}.{table}").first()
    return d.numFiles, d.partitionColumns, d.clusteringColumns

def refused(statement):
    """Run a statement we expect to fail, and print the error class it fails with."""
    try:
        spark.sql(statement)
        return "accepted"
    except Exception as error:
        return str(error).split("]")[0].strip("[")

events = (spark.range(1_000_000)
          .withColumn("device_id", (col("id") % 10_000).cast("int"))
          .withColumn("event_date",
                      expr("date_add(DATE'2026-07-01', CAST(id % 30 AS INT))"))
          .withColumn("reading", (col("id") % 997) / 10.0))
print(f"{events.count():,} events · 10,000 devices · 30 days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The partitioning anti-pattern, measured
# MAGIC
# MAGIC Partition by (a bucket of) the high-cardinality column, the reflex of a decade of Hive
# MAGIC habits. Watch the file count.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_part_device")
(events.withColumn("device_bucket", col("device_id") % 200)
 .write.partitionBy("device_bucket")
 .saveAsTable(f"{BRONZE}.layout_part_device"))

d1 = spark.sql(f"DESCRIBE DETAIL {BRONZE}.layout_part_device").first()
print(f"partitioned by device bucket: {d1.numFiles} files, "
      f"{d1.sizeInBytes/1e6:.1f} MB → avg {d1.sizeInBytes/d1.numFiles/1e3:.0f} KB/file")

# COMMAND ----------

# MAGIC %md
# MAGIC Hundreds of files for a table this small: each partition forces at least one file, and
# MAGIC every one is tiny. The docs put numbers on the rule, so use them:
# MAGIC
# MAGIC | Table size | The docs say |
# MAGIC |---|---|
# MAGIC | under 1 TB | don't partition |
# MAGIC | 1 TB to 100 TB | use liquid clustering; partitioning hurts more often than it helps |
# MAGIC | 100 TB or more | partitioning might help, but try liquid clustering first and verify |
# MAGIC | any partition | should hold at least 1 GB |
# MAGIC
# MAGIC **Map the partition to a coarse grain.** Date on a huge fact: fine. Customer, device or
# MAGIC order: never.
# MAGIC
# MAGIC ## 2. The previous era: partition plus Z-ORDER

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_part_date")
(events.write.partitionBy("event_date")
 .saveAsTable(f"{BRONZE}.layout_part_date"))
spark.sql(f"OPTIMIZE {BRONZE}.layout_part_date ZORDER BY (device_id)")
d2 = spark.sql(f"DESCRIBE DETAIL {BRONZE}.layout_part_date").first()
print(f"date-partitioned + ZORDER: {d2.numFiles} files, "
      f"avg {d2.sizeInBytes/d2.numFiles/1e3:.0f} KB/file")

# COMMAND ----------

# MAGIC %md
# MAGIC Better, and notice what it costs. You must re-run it as data arrives. The docs call
# MAGIC Z-ordering "not idempotent" but incremental in aim, with no promise that later runs get
# MAGIC faster. The keys are fixed at optimize time, it can't cross a partition boundary, and
# MAGIC the partition scheme itself is frozen. Predictive optimization won't help either: it
# MAGIC doesn't run ZORDER and it ignores Z-ordered files.
# MAGIC
# MAGIC ## 3. Liquid clustering: one layout, both filter columns

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_liquid")
spark.sql(f"""CREATE TABLE {BRONZE}.layout_liquid
              (id BIGINT, device_id INT, event_date DATE, reading DOUBLE)
              CLUSTER BY (event_date, device_id)""")
events.select("id", "device_id", "event_date", "reading") \
      .write.mode("append").saveAsTable(f"{BRONZE}.layout_liquid")
spark.sql(f"OPTIMIZE {BRONZE}.layout_liquid")   # clusters incrementally

d3 = spark.sql(f"DESCRIBE DETAIL {BRONZE}.layout_liquid").first()
print(f"liquid clustered: {d3.numFiles} files, "
      f"avg {d3.sizeInBytes/d3.numFiles/1e6:.1f} MB/file")
assert d3.numFiles < d1.numFiles / 5, "liquid should use far fewer files"

# COMMAND ----------

# MAGIC %md
# MAGIC A handful of well-sized files, clustered on BOTH filter columns. High cardinality is
# MAGIC welcome and there are no directories.
# MAGIC
# MAGIC ## 4. Re-keying, and forcing the old data to follow
# MAGIC
# MAGIC `ALTER ... CLUSTER BY` is a metadata change: no data moves. From then on, new writes and
# MAGIC `OPTIMIZE` use the new keys, and **existing data stays where it is until you force it**
# MAGIC with `OPTIMIZE ... FULL`. This table is bigger, because a one-file table has nothing to
# MAGIC rewrite.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_rekey")
spark.sql(f"""CREATE TABLE {BRONZE}.layout_rekey
              (id BIGINT, device_id INT, event_date DATE, payload STRING)
              CLUSTER BY (event_date)""")
(spark.range(6_000_000)
 .withColumn("device_id", (col("id") % 10_000).cast("int"))
 .withColumn("event_date", expr("date_add(DATE'2026-07-01', CAST(id % 30 AS INT))"))
 .withColumn("payload", expr("sha2(CAST(id AS STRING), 256)"))
 .write.mode("append").saveAsTable(f"{BRONZE}.layout_rekey"))

before = spark.sql(f"DESCRIBE HISTORY {BRONZE}.layout_rekey").first().version
spark.sql(f"ALTER TABLE {BRONZE}.layout_rekey CLUSTER BY (device_id)")   # metadata only
print("keys now:", detail("layout_rekey")[2])

spark.sql(f"OPTIMIZE {BRONZE}.layout_rekey FULL")                        # moves the old data

(spark.sql(f"DESCRIBE HISTORY {BRONZE}.layout_rekey")
 .filter(f"version > {before}")
 .select("version", "operation",
         expr("operationParameters['isFull']").alias("isFull"),
         expr("operationParameters['clusterBy']").alias("clusterBy"),
         expr("operationMetrics['numRemovedFiles']").alias("removed"),
         expr("operationMetrics['numAddedFiles']").alias("added"))
 .orderBy("version").show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC The `CLUSTER BY` commit touched no files, and the `FULL` run did the rewriting. On a
# MAGIC large table never clustered on these keys that can take hours, so it's a decision and
# MAGIC not a reflex. Run it again and it finds nothing to do: files already clustered on the
# MAGIC current keys aren't rewritten. Compare with changing a partition column: a full table
# MAGIC rewrite.
# MAGIC
# MAGIC ## 5. Migrating what you inherit
# MAGIC
# MAGIC Three statements, three different answers. Run them and read the error classes.

# COMMAND ----------

# A: unpartitioned and Z-ordered. Just ALTER it, reusing the ZORDER columns as keys.
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_zorder")
events.write.saveAsTable(f"{BRONZE}.layout_zorder")
spark.sql(f"OPTIMIZE {BRONZE}.layout_zorder ZORDER BY (device_id)")
print("ALTER on a Z-ordered table:    ",
      refused(f"ALTER TABLE {BRONZE}.layout_zorder CLUSTER BY (device_id)"))

# B: now it's clustered. ZORDER is what's refused, not the migration.
print("ZORDER on a clustered table:   ",
      refused(f"OPTIMIZE {BRONZE}.layout_zorder ZORDER BY (device_id)"))

# C: partitioned. Plain ALTER is refused, and the error names the right statement.
print("ALTER on a partitioned table:  ",
      refused(f"ALTER TABLE {BRONZE}.layout_part_date CLUSTER BY (device_id)"))

# COMMAND ----------

spark.sql(f"""ALTER TABLE {BRONZE}.layout_part_date
              REPLACE PARTITIONED BY WITH CLUSTER BY (event_date, device_id)""")
files, partitions, keys = detail("layout_part_date")
print(f"converted in place: {files} files, partitions {partitions}, clustering {keys}")
assert partitions == [] and keys == ["event_date", "device_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC The rule the docs give: partition columns and ZORDER columns become the clustering keys,
# MAGIC then run `OPTIMIZE`. Layout, as always, changes when `OPTIMIZE` runs, and the statement
# MAGIC itself only changed the definition. That conversion needs Runtime 18.1 or above, and
# MAGIC doesn't apply to pipeline tables, where you change `PARTITIONED BY` in the definition.
# MAGIC
# MAGIC ## 6. `CLUSTER BY AUTO`: letting the platform pick

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_auto")
spark.sql(f"""CREATE TABLE {BRONZE}.layout_auto
              (id BIGINT, device_id INT, event_date DATE) CLUSTER BY AUTO""")
(spark.sql(f"SHOW TBLPROPERTIES {BRONZE}.layout_auto")
 .filter("key IN ('clusterByAuto', 'clusteringColumns')").show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC `clusterByAuto` is true and the key list is empty, and an empty list is a valid outcome.
# MAGIC AUTO needs a **Unity Catalog managed table** and **predictive optimization**. It reads
# MAGIC your query history and changes keys only when the predicted savings beat the cost of
# MAGIC clustering, so a small or rarely queried table may never get a key.
# MAGIC
# MAGIC ## 7. Compaction: the size problem, separate from the layout problem

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.layout_small")
for batch in range(40):                                       # forty tiny appends
    (spark.range(batch * 1000, (batch + 1) * 1000)
     .withColumn("v", col("id") % 7)
     .write.mode("append").saveAsTable(f"{BRONZE}.layout_small"))

small_before = detail("layout_small")[0]
auto_runs = (spark.sql(f"DESCRIBE HISTORY {BRONZE}.layout_small")
             .filter("operation = 'OPTIMIZE' AND operationParameters['auto'] = 'true'").count())
spark.sql(f"OPTIMIZE {BRONZE}.layout_small")
small_after = detail("layout_small")[0]

print(f"40 appends → {small_before} files · auto compaction ran {auto_runs} times along the way")
print(f"after OPTIMIZE → {small_after} file(s)")
assert small_after < small_before

# COMMAND ----------

# MAGIC %md
# MAGIC Auto compaction was already working, and it shows in the history with `auto = true`.
# MAGIC `OPTIMIZE` finished the job. Target sizes are autotuned: 256 MB under 2.56 TB, growing
# MAGIC to 1 GB at 10 TB.
# MAGIC
# MAGIC ## 8. The fork, settled
# MAGIC
# MAGIC | | Partitioning | Z-ORDER | Liquid, manual keys | `CLUSTER BY AUTO` |
# MAGIC |---|---|---|---|---|
# MAGIC | High-cardinality keys | ❌ tiny partitions | ✅ | ✅ | ✅ |
# MAGIC | Change keys later | full rewrite | re-run OPTIMIZE | `ALTER`, then `OPTIMIZE FULL` for old data | the platform decides |
# MAGIC | Maintenance | manual | manual, predictive optimization ignores it | incremental | predictive optimization required |
# MAGIC | Combine with another layout | with Z-ORDER | with partitions | never | never |
# MAGIC | When it's right | 100 TB+ with a coarse low-card column, verified | legacy tables you inherit | known stable filters, or you need the keys fixed | managed table, changing or unknown patterns |
# MAGIC
# MAGIC Deletion vectors aren't a column in this table because they answer a different question:
# MAGIC what a `MERGE` or `DELETE` rewrites. That's the next lesson. And **managed Iceberg** tables
# MAGIC have only one column here: `PARTITIONED BY` becomes clustering keys, and `CLUSTER BY AUTO`
# MAGIC needs Iceberg v3 (lesson 6.6).

# COMMAND ----------

for t in DEMOS:
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{t}")
print("demo tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Partitioning: big, coarse, frozen, and only past 100 TB. Z-ORDER: the previous era's fix,
# MAGIC re-run forever, incompatible with clustering on the same table. Liquid: high-cardinality
# MAGIC safe, re-keyable in metadata, `FULL` when the old data must follow. AUTO when you'd
# MAGIC rather not guess. OUR tables (`bus_raw`, `fact_order_items`, `dim_customer_scd2`) already
# MAGIC use liquid. Next: what happens to files when you DELETE.

# COMMAND ----------

dbutils.notebook.exit("layout-fork-complete")
