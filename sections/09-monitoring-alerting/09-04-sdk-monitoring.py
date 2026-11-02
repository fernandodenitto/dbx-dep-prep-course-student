# Databricks notebook source
# MAGIC %md
# MAGIC # 9.4 — Monitoring jobs and pipelines from code
# MAGIC
# MAGIC The guide: "Use Databricks REST APIs / CLI and SDK for monitoring and analyzing jobs and pipelines."
# MAGIC Same API, three spellings. The CLI versions of everything below are in `09-04-monitoring-commands.md`.
# MAGIC
# MAGIC In a notebook the Databricks SDK for Python is already installed and `WorkspaceClient()` authenticates
# MAGIC as you, with no token to manage. This notebook only reads.
# MAGIC
# MAGIC > Prereq: lessons 8.2 and 8.6, so the pipeline and the job have run. Both names are widgets.

# COMMAND ----------

from itertools import islice

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("job_name", "lakeshore-platform-refresh")
dbutils.widgets.text("pipeline_name", "lakeshore-platform-sdp")

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Jobs: find it, list its runs

# COMMAND ----------

job_name = dbutils.widgets.get("job_name")
job = next(iter(w.jobs.list(name=job_name)), None)
assert job, f"No job called {job_name}. Create it in lesson 8.6."
print(f"{job_name}: {job.job_id}")

runs = list(islice(w.jobs.list_runs(job_id=job.job_id), 5))
for run in runs:
    print(run.run_id, run.status.state.value if run.status else None, run.run_page_url)
assert runs, "This job hasn't run yet. Run it once in lesson 8.6."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One run: every task's status
# MAGIC
# MAGIC `get_run` gives you the tasks, and each task has its own run id. That's the id `get_run_output` wants:
# MAGIC on a multi-task job the parent run id doesn't work there.

# COMMAND ----------

latest_finished = next((r for r in runs if r.status and r.status.state.value == "TERMINATED"), runs[0])
detail = w.jobs.get_run(run_id=latest_finished.run_id)
for task in detail.tasks:
    code = task.status.termination_details.code.value if task.status and task.status.termination_details else None
    print(f"{task.task_key:<22} {task.status.state.value if task.status else '':<12} {code or '':<16} {task.run_id}")

# COMMAND ----------

# Pick the notebook task and read what it returned with dbutils.notebook.exit
gate = next((t for t in detail.tasks if t.task_key == "quality_gate"), detail.tasks[0])
output = w.jobs.get_run_output(run_id=gate.run_id)
print(gate.task_key, "->", output.notebook_output.result if output.notebook_output else output.error)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pipelines: updates and events

# COMMAND ----------

pipeline_name = dbutils.widgets.get("pipeline_name")
pipeline = next((p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")
                 if p.name == pipeline_name), None)
assert pipeline, f"No pipeline called {pipeline_name}. Build it in lesson 8.2."
print(f"{pipeline_name}: {pipeline.pipeline_id}, state {pipeline.state.value}")

for update in w.pipelines.list_updates(pipeline_id=pipeline.pipeline_id, max_results=5).updates or []:
    print(update.update_id, update.state.value)

# COMMAND ----------

# Same diary as event_log(), through the API: the newest errors, if there are any
errors = list(islice(w.pipelines.list_pipeline_events(pipeline_id=pipeline.pipeline_id,
                                                      filter="level = 'ERROR'"), 5))
print(f"recent ERROR events: {len(errors)}")
for event in errors:
    print(event.timestamp, event.message[:160])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The REST call underneath
# MAGIC
# MAGIC The SDK and the CLI wrap the same endpoints, and the SDK's API client lets you call one by its path.
# MAGIC On a laptop this is the same request as `curl` with a token.

# COMMAND ----------

raw = w.api_client.do("GET", "/api/2.2/jobs/runs/list",
                      query={"job_id": job.job_id, "limit": 3})
print([r["run_id"] for r in raw.get("runs", [])])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC One-off question about a run → API, CLI or SDK. Trend across runs → `system.lakeflow`. A person
# MAGIC triaging → the UI. Being told → notifications.

# COMMAND ----------

dbutils.notebook.exit(f"sdk-monitoring-complete:{job.job_id}")
