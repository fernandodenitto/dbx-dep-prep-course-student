# Databricks notebook source
# MAGIC %md
# MAGIC # 7.4 — Data skipping, and where its numbers live
# MAGIC
# MAGIC The guide asks you to spot "data-skipping inefficiencies". You can't spot what you've
# MAGIC never seen, so we look at the statistics themselves, then do the skipping arithmetic by
# MAGIC hand. UC managed tables (rightly) block raw path access, so we build a small
# MAGIC **path-based Delta table in our volume**, where we're allowed to look.
# MAGIC
# MAGIC > Demo path `zoo/log_demo`, wiped and rebuilt each run. Beyond the exam: how the log is
# MAGIC > stored on disk. The current guide doesn't test transaction log anatomy, only what
# MAGIC > the statistics do.

# COMMAND ----------

import json
import re

from pyspark.sql.functions import col

PATH = "/Volumes/lakeshore/landing/files/zoo/log_demo"
LOG = f"{PATH}/_delta_log"
dbutils.fs.rm(PATH, True)

# Eleven commits with non-overlapping id ranges, so the arithmetic is easy to follow
(spark.range(0, 10_000).coalesce(1)
 .withColumn("device_id", (col("id") % 50).cast("int"))
 .withColumn("reading", (col("id") % 997) / 10.0)
 .write.format("delta").save(PATH))
for i in range(10):
    start = 10_000 + i * 100
    (spark.range(start, start + 100).coalesce(1)
     .withColumn("device_id", (col("id") % 50).cast("int"))
     .withColumn("reading", col("id") / 10.0)
     .write.mode("append").format("delta").save(PATH))
print("eleven commits written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Every file arrives with its statistics
# MAGIC
# MAGIC A commit is a JSON file. Its `add` actions list the data files, each carrying `stats`.

# COMMAND ----------

def commits():
    """The numbered JSON commits, in order. Other files in the folder are not commits."""
    names = sorted(f.name for f in dbutils.fs.ls(LOG))
    return [n for n in names if re.fullmatch(r"\d{20}\.json", n)]

def add_actions():
    """Every file added by every commit, with its parsed statistics."""
    adds = []
    for name in commits():
        with open(f"{LOG}/{name}") as f:
            for line in f:
                action = json.loads(line)
                if "add" in action:
                    adds.append(action["add"])
    return adds

adds = add_actions()
print(f"{len(commits())} commits, {len(adds)} data files")
print(json.dumps(json.loads(adds[0]["stats"]), indent=2)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC **That's the entire data-skipping machinery**: per file, `numRecords`, `minValues`,
# MAGIC `maxValues`, `nullCount`. There's no index. The engine compares your filter with these
# MAGIC numbers before it opens a file.
# MAGIC
# MAGIC Which columns get statistics depends on the table:
# MAGIC - **External tables**: the first 32 columns, unless you set
# MAGIC   `delta.dataSkippingStatsColumns` (names them, and wins) or
# MAGIC   `delta.dataSkippingNumIndexedCols` (changes the count, depends on column order).
# MAGIC - **Managed tables with predictive optimization**: those, plus other columns your
# MAGIC   queries often filter on, chosen by the service.
# MAGIC
# MAGIC Long strings are truncated in statistics, so they make weak bounds.
# MAGIC
# MAGIC ## 2. Skipping is arithmetic. Do it yourself.

# COMMAND ----------

def files_a_range_filter_must_read(column, low, high):
    """Keep the files whose [min, max] overlaps the filter. The rest can be skipped unread."""
    keep = []
    for add in add_actions():
        stats = json.loads(add["stats"])
        if stats["maxValues"][column] >= low and stats["minValues"][column] <= high:
            keep.append(add["path"])
    return keep

total = len(add_actions())
visible = files_a_range_filter_must_read("id", 10_100, 10_199)
print(f"id BETWEEN 10100 AND 10199     → the stats rule out {total - len(visible)} of {total} files")
print(f"abs(hash(id)) % 100 = 3        → the stats rule out 0 of {total}: they can't evaluate it")
assert len(visible) == 1 and total == 11

# COMMAND ----------

# Both queries still return the right answer. Only the bill is different.
fast = spark.read.format("delta").load(PATH).filter("id BETWEEN 10100 AND 10199")
slow = spark.read.format("delta").load(PATH).filter("abs(hash(id)) % 100 = 3")
print("range filter:     ", fast.count(), "rows")
print("expression filter:", slow.count(), "rows")
assert spark.read.format("delta").load(PATH).count() == 11_000

# COMMAND ----------

# MAGIC %md
# MAGIC Same table, same size, different physics. Click **See performance** under that cell for
# MAGIC the query profile of each. It's the next lesson's tool. **Write predicates the stats can
# MAGIC see**, and lay tables out (lesson 7.2) so the ranges are tight. Files with overlapping
# MAGIC ranges can't be skipped no matter how clean your filter is.
# MAGIC
# MAGIC *Beyond the exam: the log also holds periodic checkpoints, so a reader loads a recent
# MAGIC checkpoint plus the newest commits rather than replaying all of history. Modern writers
# MAGIC may use v2 checkpoints or log compaction for this, on their own cadence, which is why
# MAGIC we no longer try to catch one on camera.*

# COMMAND ----------

dbutils.fs.rm(PATH, True)
print("demo cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Every data file carries min, max and null counts. Skipping is arithmetic on them. Which
# MAGIC columns get statistics depends on table type and predictive optimization, and clustering
# MAGIC is what makes the ranges tight. Next: watching all of this in the Query Profile.

# COMMAND ----------

dbutils.notebook.exit("transaction-log-complete")
