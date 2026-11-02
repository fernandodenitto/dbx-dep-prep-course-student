# Databricks notebook source
# MAGIC %md
# MAGIC # 12.8 Keep a Git folder in sync from a job
# MAGIC
# MAGIC The docs' fallback for production Git folders when you have no external CI/CD: a scheduled
# MAGIC job runs a small notebook that pulls the folder. On Free Edition it's also the one sync
# MAGIC path you can run end to end. Create the folder first, with the commands in
# MAGIC `12-08-git-folder-commands.md`, then pass its path here.

# COMMAND ----------

dbutils.widgets.text("git_folder", "")
dbutils.widgets.text("branch", "main")

git_folder = dbutils.widgets.get("git_folder")
branch = dbutils.widgets.get("branch")
assert git_folder, "Set the git_folder widget to the workspace path of the folder to sync"

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

repo_id = w.workspace.get_status(path=git_folder).object_id
before = w.repos.get(repo_id).head_commit_id

w.repos.update(repo_id, branch=branch)

after = w.repos.get(repo_id)
print(f"{git_folder}: {after.branch} at {after.head_commit_id} (was {before})")

# COMMAND ----------

dbutils.notebook.exit(f"synced:{after.branch}:{after.head_commit_id[:8]}")
