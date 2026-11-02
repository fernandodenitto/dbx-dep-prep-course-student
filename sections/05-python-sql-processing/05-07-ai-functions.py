# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.7 — AI functions and `ai_query` in a pipeline
# MAGIC
# MAGIC The guide: "Apply AI functions, including ai_query, to perform model inference within
# MAGIC data pipelines for enrichment and classification tasks." For a data engineer, this is
# MAGIC a SQL function that happens to cost money per row. So we learn the two families, keep
# MAGIC a hard cap on rows, and put it in a real pipeline.
# MAGIC
# MAGIC **Models answer differently run to run.** The asserts in this notebook check the
# MAGIC shape of the answers, never their exact words. Your categories may differ from mine.
# MAGIC
# MAGIC **Free Edition honesty.** The limitations page doesn't mention AI functions, and it
# MAGIC does say "Certain models not available" and that compute shuts down for the day when
# MAGIC you exceed your quota. This ran on 2026-11-01 with a dozen rows. Keep it to a dozen.
# MAGIC
# MAGIC > Prereqs: lesson 5.6 (reading a `VARIANT`). Run from the course Git folder: the
# MAGIC > pipeline's source sits beside this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Something a parser can't read: free text
# MAGIC
# MAGIC Twelve support messages, written by hand, about real orders from the bus. That's the
# MAGIC whole dataset, on purpose: every row below is a billable model call.

# COMMAND ----------

MESSAGES = [
    (1, "O002001", "The tent arrived with a snapped pole. I need a replacement before Friday."),
    (2, "O002002", "My order was supposed to be here on Monday and tracking has not moved."),
    (3, "O002003", "You charged my card twice for the same order. Please fix this."),
    (4, "O002004", "Does the 40 litre pack fit under an airline seat?"),
    (5, "O002005", "Box was soaked and the sleeping bag inside is ruined."),
    (6, "O002006", "Still waiting after two weeks. Where is my parcel?"),
    (7, "O002007", "I returned the boots last month and have not seen the refund."),
    (8, "O002008", "Is the stove compatible with standard screw-on canisters?"),
    (9, "O002009", "One trekking pole is bent out of the box."),
    (10, "O002010", "The courier says delivered but nothing came to my address."),
    (11, "O002011", "The invoice shows the wrong VAT number for my company."),
    (12, "O002012", "What temperature is the down jacket rated for?"),
]

# Load once and never overwrite: a streaming table reads this, and a stream's source
# has to stay append-only (lesson 4.3).
spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.silver.support_messages
             (msg_id INT, order_id STRING, body STRING)""")
if spark.table("lakeshore.silver.support_messages").isEmpty():
    (spark.createDataFrame(MESSAGES, "msg_id INT, order_id STRING, body STRING")
     .write.mode("append").saveAsTable("lakeshore.silver.support_messages"))

display(spark.table("lakeshore.silver.support_messages").orderBy("msg_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Task-specific functions: the task is the API
# MAGIC
# MAGIC `ai_classify`, `ai_extract`, `ai_summarize`, `ai_mask`, `ai_translate` and friends.
# MAGIC You name the task, Databricks picks and runs the model. Pin the version: the docs
# MAGIC recommend passing `MAP('version', '2.1')`, because the output shape changed between
# MAGIC versions. In 2.1 the result is a `VARIANT`, so lesson 5.6 is how you read it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT msg_id, body,
# MAGIC        ai_classify(body,
# MAGIC                    '["damaged item", "late delivery", "billing", "product question"]',
# MAGIC                    MAP('version', '2.1')):response[0].value::string AS category
# MAGIC FROM lakeshore.silver.support_messages
# MAGIC WHERE msg_id <= 4 ORDER BY msg_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT msg_id,
# MAGIC        ai_extract(body, '["product", "problem"]', MAP('version', '2.1')) AS extracted
# MAGIC FROM lakeshore.silver.support_messages
# MAGIC WHERE msg_id <= 2 ORDER BY msg_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `ai_query`: you choose the model, you own the prompt
# MAGIC
# MAGIC The general-purpose function. Three arguments carry the exam's weight:
# MAGIC - `responseFormat`: a JSON schema, so the answer is parseable instead of prose.
# MAGIC - `failOnError => false`: returns a `STRUCT` of `result` and `errorMessage`, so one
# MAGIC   bad row doesn't kill the run.
# MAGIC - `modelParameters`: cap `max_tokens`, because tokens are the bill.

# COMMAND ----------

RESPONSE_FORMAT = """{"type": "json_schema", "json_schema": {"name": "triage",
  "schema": {"type": "object",
             "properties": {"urgency": {"type": "string", "enum": ["low", "medium", "high"]},
                            "needs_refund": {"type": "boolean"}},
             "required": ["urgency", "needs_refund"]},
  "strict": true}}"""

triage = spark.sql(f"""
    SELECT msg_id, reply.result AS triage_json, reply.errorMessage AS error_message
    FROM (SELECT msg_id,
                 ai_query('databricks-meta-llama-3-3-70b-instruct',
                          'Triage this outdoor-gear support message. ' || body,
                          responseFormat => '{RESPONSE_FORMAT}',
                          modelParameters => named_struct('max_tokens', 60, 'temperature', 0.0),
                          failOnError => false) AS reply
          FROM lakeshore.silver.support_messages
          WHERE msg_id <= 3)
    ORDER BY msg_id""")
display(triage)

answers = triage.collect()
assert all(r.error_message is None for r in answers), answers
assert all('"urgency"' in r.triage_json for r in answers), answers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Which one, for the given constraint
# MAGIC
# MAGIC | Constraint | Reach for |
# MAGIC |---|---|
# MAGIC | A standard task: classify, extract, summarize, mask, translate | **Task-specific function** |
# MAGIC | You need a specific model, or a custom prompt | **`ai_query`** |
# MAGIC | The output must fit a schema | `ai_query` with `responseFormat`, or `ai_extract` with a schema |
# MAGIC | One failed row mustn't fail the run | `ai_query` with `failOnError => false` |
# MAGIC | A model you serve yourself | `ai_query` on your endpoint |
# MAGIC
# MAGIC Both run on the compute you submit from, except Classic SQL warehouses, where AI
# MAGIC Functions are not available. The billing differs: task-specific functions bill as
# MAGIC `AI_FUNCTIONS`, and `ai_query` bills to the model serving endpoint you name. Your
# MAGIC compute is billed as well. Both show up in `system.billing.usage`.
# MAGIC
# MAGIC ## 5. In a pipeline
# MAGIC
# MAGIC `05-07-ai-triage-pipeline/triage.sql` holds two datasets. Open it beside this cell.
# MAGIC - A **streaming table**: reads each new message once, so each is classified once.
# MAGIC - A **materialized view** with `ai_query`, `LIMIT 6`, and row-level errors kept.
# MAGIC
# MAGIC That choice is the cost decision. Incremental refresh of a materialized view doesn't
# MAGIC support non-deterministic functions other than time functions, so read a model call as
# MAGIC a full recompute on each refresh (my inference from that rule; the page doesn't name
# MAGIC `ai_query`). A streaming table pays once per row, which is why inference on arriving
# MAGIC data belongs there. And it's why the `LIMIT` is there while you develop.
# MAGIC
# MAGIC This creates the pipeline with the Databricks SDK, so it runs the same every time.
# MAGIC In the UI it's **Jobs & Pipelines** → **New** → **ETL Pipeline**, with the same source file.

# COMMAND ----------

import os, time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary

PIPELINE_NAME = "lakeshore-ai-triage"
source = f"{os.getcwd()}/05-07-ai-triage-pipeline/triage.sql"

w = WorkspaceClient()
settings = dict(name=PIPELINE_NAME, catalog="lakeshore", schema="silver",
                serverless=True, development=True,
                libraries=[PipelineLibrary(file=FileLibrary(path=source))])

existing = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")]
if existing:
    pipeline_id = existing[0].pipeline_id
    w.pipelines.update(pipeline_id=pipeline_id, **settings)
else:
    pipeline_id = w.pipelines.create(**settings).pipeline_id

update_id = w.pipelines.start_update(pipeline_id=pipeline_id).update_id
while True:
    state = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(10)
print(f"pipeline {PIPELINE_NAME}: {state}")
assert state == "COMPLETED", f"see the update's event log for {pipeline_id}"

# COMMAND ----------

triaged = spark.table("lakeshore.silver.support_triaged")
replies = spark.table("lakeshore.silver.support_replies")
display(triaged.orderBy("msg_id"))
display(replies.orderBy("msg_id"))

allowed = {"damaged item", "late delivery", "billing", "product question"}
assert triaged.count() >= 1
assert {r.category for r in triaged.collect()} <= allowed
assert replies.count() <= 6

# COMMAND ----------

# MAGIC %md
# MAGIC A pipeline's expectation sits on the AI output like any other column: `known_category`
# MAGIC drops rows the model couldn't classify, and Section 8 goes deep on expectations.
# MAGIC
# MAGIC The pipeline stays in your workspace, idle. It uses compute only when it runs.
# MAGIC
# MAGIC ## ✅ Recap — and the section closes
# MAGIC
# MAGIC Task-specific functions when the task is standard, `ai_query` when you need the model
# MAGIC or the prompt. `responseFormat` for shape, `failOnError => false` for one bad row,
# MAGIC `LIMIT` and `max_tokens` for the bill. A streaming table pays once per row. You've now
# MAGIC got the language: windows, joins, arrays, UDFs, libraries, parameters, `VARIANT`,
# MAGIC model calls. Next section goes back to where data comes from, in far more formats than
# MAGIC our bus.

# COMMAND ----------

dbutils.notebook.exit("ai-functions-complete")
