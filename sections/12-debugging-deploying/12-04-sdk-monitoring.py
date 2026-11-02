# Databricks notebook source
# MAGIC %md
# MAGIC # 12.4 The Python SDK: the same door, from code
# MAGIC
# MAGIC The Databricks SDK for Python ships with the runtime, and `WorkspaceClient()` picks up
# MAGIC this notebook's identity with no configuration. Same REST API, typed objects instead of
# MAGIC JSON. The exam statement names it beside REST and the CLI for monitoring jobs and
# MAGIC pipelines.
# MAGIC
# MAGIC > Prereq: at least one job run, for example `lakeshore-debug-lab` from lesson 12.3, and the
# MAGIC > pipeline from Section 8. If a name is missing, the cell says so and moves on.

# COMMAND ----------

dbutils.widgets.text("job_name", "lakeshore-debug-lab")
dbutils.widgets.text("pipeline_name", "lakeshore-platform-sdp")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
print("calling the API as:", w.current_user.me().user_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Jobs: find, list runs, read one run's tasks

# COMMAND ----------

job_name = dbutils.widgets.get("job_name")
job = next(iter(w.jobs.list(name=job_name)), None)

if job is None:
    print(f"no job named {job_name}: run lesson 12.3 first")
else:
    for run in w.jobs.list_runs(job_id=job.job_id, limit=3):
        print(run.run_id, run.state.life_cycle_state, run.state.result_state)

    latest = next(iter(w.jobs.list_runs(job_id=job.job_id, limit=1)), None)
    if latest:
        detail = w.jobs.get_run(latest.run_id, include_history=True)
        for task in detail.tasks:
            print(f"{task.task_key:12} attempt {task.attempt_number} {task.state.result_state}")
        print("history entries, the original run included:", len(detail.repair_history or []))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipelines: find, list updates, read the event log

# COMMAND ----------

pipeline_name = dbutils.widgets.get("pipeline_name")
pipeline = next(iter(w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")), None)

if pipeline is None:
    print(f"no pipeline named {pipeline_name}: run Section 8 first")
else:
    print(pipeline.name, pipeline.state)
    for update in (w.pipelines.list_updates(pipeline.pipeline_id, max_results=3).updates or []):
        print(update.update_id, update.state, update.cause)

    events = w.pipelines.list_pipeline_events(pipeline.pipeline_id, max_results=5)
    for event in list(events)[:5]:
        print(event.event_type, event.level, event.message[:70])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC `w.jobs` and `w.pipelines` mirror the REST paths and the CLI's commands. The SDK reaching
# MAGIC your workspace from here also shows that serverless is not cut off from the API. Free
# MAGIC Edition limits outbound internet to a set of trusted domains, which is a different rule.
# MAGIC Outside automation still belongs on a machine or a CI runner, for reasons of identity
# MAGIC and scheduling, not reachability.

# COMMAND ----------

dbutils.notebook.exit("sdk-monitoring-complete")
