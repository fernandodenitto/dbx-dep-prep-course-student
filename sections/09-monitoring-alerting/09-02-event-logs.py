# Databricks notebook source
# MAGIC %md
# MAGIC # 9.2 — The pipeline event log
# MAGIC
# MAGIC System tables watch the workspace. The **event log** watches one pipeline: update by update, flow by
# MAGIC flow, expectation by expectation. It's a Delta table you query with `event_log()`, and it's the tool
# MAGIC the guide names: "Use Pipeline event logs to monitor pipeline health and data quality."
# MAGIC
# MAGIC > Prereq: lesson 8.4 (`lakeshore-platform-sdp` has run at least once). Nothing here is hard-coded to
# MAGIC > one workspace: we look the pipeline up by name, and you can paste an id in the widget instead.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("pipeline_name", "lakeshore-platform-sdp")
dbutils.widgets.text("pipeline_id", "")

name = dbutils.widgets.get("pipeline_name")
pipeline_id = dbutils.widgets.get("pipeline_id").strip()

if not pipeline_id:
    found = [p for p in WorkspaceClient().pipelines.list_pipelines(filter=f"name LIKE '{name}'")
             if p.name == name]
    assert found, f"No pipeline called {name} here. Build it in lesson 8.2, or paste its id in the widget."
    pipeline_id = found[0].pipeline_id
print(f"{name}: {pipeline_id}")

spark.sql(f"""CREATE OR REPLACE TEMP VIEW ev AS
              SELECT * FROM event_log("{pipeline_id}")""")

# COMMAND ----------

# MAGIC %md
# MAGIC If this fails with `PERMISSION_DENIED`, you aren't the pipeline's run-as user. By default only that
# MAGIC user can query `event_log()`. To open it to others, publish the log as a table in the pipeline's
# MAGIC settings and grant `SELECT` on it.
# MAGIC
# MAGIC ## 1. What's in the diary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, count(*) AS events
# MAGIC FROM ev GROUP BY event_type ORDER BY events DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC The three to know: `create_update` (an update begins), `update_progress` (state transitions:
# MAGIC WAITING_FOR_RESOURCES, INITIALIZING, SETTING_UP_TABLES, RUNNING, then COMPLETED, FAILED or CANCELED) and
# MAGIC `flow_progress` (per-table work: rows written, data quality).
# MAGIC
# MAGIC ## 2. Update outcomes: the pipeline's run history
# MAGIC
# MAGIC The last state means the **latest in time**, so we use `max_by(state, timestamp)`. A plain `max(state)`
# MAGIC sorts the strings alphabetically and hands you WAITING_FOR_RESOURCES for a run that finished hours ago.
# MAGIC Both columns are here so you can see it happen.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origin.update_id,
# MAGIC        min(timestamp) AS started,
# MAGIC        max_by(details:update_progress.state, timestamp) AS last_state,
# MAGIC        max(details:update_progress.state)               AS alphabetical_max_wrong
# MAGIC FROM ev
# MAGIC WHERE event_type = 'update_progress'
# MAGIC GROUP BY origin.update_id ORDER BY started DESC LIMIT 10;

# COMMAND ----------

outcomes = spark.sql("""
    SELECT max_by(details:update_progress.state, timestamp) AS last_state
    FROM ev WHERE event_type = 'update_progress'
    GROUP BY origin.update_id""").collect()
states = {row.last_state for row in outcomes}
print(f"final states seen: {states}")
assert "COMPLETED" in states, "expected at least one completed update"

# COMMAND ----------

# MAGIC %md
# MAGIC Your Section 8 history as rows. What the UI shows as coloured dots, the event log serves as data you
# MAGIC can trend and alert on. Any attempt that failed, for instance on a serverless capacity limit, shows up
# MAGIC here too.
# MAGIC
# MAGIC ## 3. Expectation metrics: quality as data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origin.update_id,
# MAGIC        expectation.name,
# MAGIC        sum(expectation.passed_records) AS passed,
# MAGIC        sum(expectation.failed_records) AS failed
# MAGIC FROM (
# MAGIC   SELECT origin, explode(
# MAGIC     from_json(details:flow_progress.data_quality.expectations,
# MAGIC       "array<struct<name: string, dataset: string,
# MAGIC              passed_records: int, failed_records: int>>")) AS expectation
# MAGIC   FROM ev WHERE event_type = 'flow_progress')
# MAGIC GROUP BY origin.update_id, expectation.name
# MAGIC ORDER BY origin.update_id DESC, expectation.name;

# COMMAND ----------

# MAGIC %md
# MAGIC Our three gate rules, `parsed_payload`, `has_customer` and `positive_quantities`, with pass and fail
# MAGIC counts per update. That's the Section 8 UI panel as queryable history: trend `failed` over weeks and
# MAGIC you have a data-quality dashboard. Alert on it, next lesson, and you have a pager.

# COMMAND ----------

rows = spark.sql("""
    SELECT count(*) FROM (
      SELECT explode(from_json(details:flow_progress.data_quality.expectations,
        "array<struct<name: string, dataset: string,
               passed_records: int, failed_records: int>>")) AS e
      FROM ev WHERE event_type = 'flow_progress')
    WHERE e.name IS NOT NULL""").first()[0]
print(f"expectation metric rows: {rows}")
assert rows > 0, "expected expectation metrics in the event log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Operational notes
# MAGIC
# MAGIC - `event_log("<pipeline-id>")` is the table-valued function. The log can also be **published** to Unity
# MAGIC   Catalog as a real table, through the pipeline's `event_log` setting, for grants and dashboards.
# MAGIC - Backlog and resource metrics also land here. The UI is a rendering of this table.
# MAGIC - Beyond one pipeline: `system.lakeflow_pipeline_events_preview.pipeline_events` is a Beta system table
# MAGIC   with event log entries across pipelines in a region. It's queryable on our Free Edition workspace.
# MAGIC   The exam's named tool for one pipeline's quality stays the event log.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `event_log()` → `event_type` → `update_progress` for outcomes, with `max_by` on the timestamp;
# MAGIC `flow_progress.data_quality` for expectations. Monitoring's raw material secured. Next we make it ring.

# COMMAND ----------

dbutils.notebook.exit("event-log-complete")
