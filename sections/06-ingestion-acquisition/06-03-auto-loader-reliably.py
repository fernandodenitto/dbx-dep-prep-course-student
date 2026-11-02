# Databricks notebook source
# MAGIC %md
# MAGIC # 6.3 — Auto Loader, reliably
# MAGIC
# MAGIC You've used Auto Loader since Section 3. Today we stress it the way
# MAGIC production does: **the schema changes mid-stream.** Every evolution
# MAGIC mode, live, including the controlled crash.
# MAGIC
# MAGIC > **Run this notebook cell by cell, not with Run All.** Section 2 has a cell
# MAGIC > that fails on purpose, and Run All stops there. When it fails, run the next
# MAGIC > cell. To replay without the failure, set the `crash_demo` widget to `false`.
# MAGIC >
# MAGIC > Demo objects only: `bronze.zoo_events` + scratch checkpoints, reset
# MAGIC > at the start so the lesson replays identically every time.

# COMMAND ----------

import json
import os

EVOLVING = "/Volumes/lakeshore/landing/files/zoo/evolving"
CKPT_A = "/Volumes/lakeshore/landing/files/_checkpoints/zoo_evolve_add"
CKPT_B = "/Volumes/lakeshore/landing/files/_checkpoints/zoo_evolve_rescue"

# fresh stage every run — this is a demo, not a canonical pipeline
spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_events")
spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_events_rescue")
for p in (CKPT_A, CKPT_B, EVOLVING):
    dbutils.fs.rm(p, True)
os.makedirs(EVOLVING, exist_ok=True)

def land(name, rows):
    with open(f"{EVOLVING}/{name}", "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"landed {name}")

land("events_001.json", [
    {"event_id": "E001", "kind": "page_view", "event_ts": "2026-08-01T09:00:00"},
    {"event_id": "E002", "kind": "add_to_cart", "event_ts": "2026-08-01T09:00:05"},
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inference + schemaLocation
# MAGIC
# MAGIC No schema declared: Auto Loader infers one (strings for JSON — types
# MAGIC are a text format's missing half) and TRACKS it at
# MAGIC `cloudFiles.schemaLocation`. A `schemaHints` option upgrades what we
# MAGIC know: `event_ts` really is a timestamp.

# COMMAND ----------

def ingest_add():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", CKPT_A)
            .option("cloudFiles.schemaHints", "event_ts TIMESTAMP")
            .load(EVOLVING)
            .writeStream
            .option("checkpointLocation", CKPT_A)
            .trigger(availableNow=True)
            .toTable("lakeshore.bronze.zoo_events"))

ingest_add().awaitTermination()
spark.table("lakeshore.bronze.zoo_events").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note `_rescued_data` appeared automatically — the safety net column
# MAGIC every inferred Auto Loader stream carries. Now, the schema changes:
# MAGIC
# MAGIC ## 2. A new column arrives — the controlled crash
# MAGIC
# MAGIC Default mode is **`addNewColumns`**: the stream "fails with an
# MAGIC `UnknownFieldException` AFTER Auto Loader adds the new columns to the
# MAGIC schema". Failure IS the protocol — the restart resumes with the
# MAGIC evolved schema.

# COMMAND ----------

land("events_002.json", [
    {"event_id": "E003", "kind": "purchase",
     "event_ts": "2026-08-01T09:01:00", "channel": "mobile_app"},
])

# crash_demo=true (default, interactive): the next cell FAILS on purpose.
# A stream that dies with this exception fails its notebook cell, in a lesson
# and in a job task alike. That hard stop IS the production behavior this mode
# is designed around: the task fails, and its retry is the restart.
dbutils.widgets.text("crash_demo", "true")
crash_demo = dbutils.widgets.get("crash_demo") == "true"

# COMMAND ----------

# ▶️ INTERACTIVE: this cell FAILS with UnknownFieldException — by design.
# The real exception, from this very pipeline:
#   [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH]
#   Encountered unknown fields during parsing: {"channel":"mobile_app"},
#   which can be fixed by an automatic retry: true
# Read that last clause again: "fixed by an automatic retry: true".
if crash_demo:
    ingest_add().awaitTermination()   # 💥 dies here — then run the NEXT cell

# COMMAND ----------

# The restart (or, in job/validation mode, an equivalent fresh build whose
# initial inference already sees both files — no crash, same end state):
if crash_demo:
    ingest_add().awaitTermination()          # schema already evolved → resumes
else:
    spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_events")
    dbutils.fs.rm(CKPT_A, True)
    ingest_add().awaitTermination()          # infers over BOTH files at start

df = spark.table("lakeshore.bronze.zoo_events")
assert "channel" in df.columns and df.count() == 3, (df.columns, df.count())
df.select("event_id", "kind", "channel").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Three rows, `channel` present (NULL for the old rows). One restart —
# MAGIC in production, that's your job's retry doing the work. **Exam
# MAGIC sentence: the failure is expected behavior, the fix is a restart, and
# MAGIC retries make it automatic.**
# MAGIC
# MAGIC ## 3. `rescue` mode — never fail, never evolve

# COMMAND ----------

# The contract schema is DECLARED (frozen — channel is deliberately absent);
# everything that doesn't fit rides in the rescue column. (With INFERRED
# schemas the same behavior is schemaEvolutionMode='rescue'; starting from a
# declared contract, the rescuedDataColumn option is the direct route.)
FROZEN = "event_id STRING, kind STRING, event_ts TIMESTAMP"

def ingest_rescue():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .schema(FROZEN)
            .load(EVOLVING)
            .writeStream
            .option("checkpointLocation", CKPT_B)
            .trigger(availableNow=True)
            .toTable("lakeshore.bronze.zoo_events_rescue"))

ingest_rescue().awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, kind, _rescued_data
# MAGIC FROM lakeshore.bronze.zoo_events_rescue ORDER BY event_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Same files, no crash — and `channel` is NOT a column: it rode along
# MAGIC inside `_rescued_data` as JSON. The schema is frozen; nothing is
# MAGIC lost; downstream decides what to do with the rescued blob.
# MAGIC
# MAGIC The full mode table:
# MAGIC
# MAGIC | `schemaEvolutionMode` | New column → | Use when |
# MAGIC |---|---|---|
# MAGIC | `addNewColumns` (default) | fail, evolve, restart resumes | evolving sources + retries |
# MAGIC | `addNewColumnsWithTypeWidening` | same + widens types (int→long) | numeric drift too |
# MAGIC | `rescue` | into `_rescued_data`, never fails | frozen contract, zero downtime |
# MAGIC | `failOnNewColumns` | fail, do NOT evolve | manual schema control |
# MAGIC | `none` | ignored, not rescued | you truly don't care (rare) |
# MAGIC
# MAGIC ## 4. File discovery — the other reliability axis
# MAGIC
# MAGIC - **Directory listing** (default): Auto Loader lists the path each
# MAGIC   run. Fine for our volumes; cost grows with directory size.
# MAGIC - **File notification / file events**: cloud storage PUSHES arrivals;
# MAGIC   the docs recommend file events for most workloads at scale, but they
# MAGIC   need cloud infrastructure (queues and events) we can't build on Free
# MAGIC   Edition. 📖 Know it as the scalable option.
# MAGIC - `cloudFiles.backfillInterval` schedules periodic full re-listings to
# MAGIC   guarantee eventual completeness when notifications can miss files.

# COMMAND ----------

# cleanup: demo objects away (checkpoints stay reset-able at top)
spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_events")
spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_events_rescue")
print("demo tables dropped — lesson replayable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC schemaLocation tracks, hints upgrade, `addNewColumns` fails-forward,
# MAGIC `rescue` freezes and preserves, discovery scales via notifications.
# MAGIC Auto Loader's reliability is CONFIGURED, not assumed. Next: one
# MAGIC pipeline that serves both batch and streaming cadences.

# COMMAND ----------

dbutils.notebook.exit("auto-loader-reliably-complete")
