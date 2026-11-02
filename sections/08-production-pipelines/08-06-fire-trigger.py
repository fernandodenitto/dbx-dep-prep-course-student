# Databricks notebook source
# MAGIC %md
# MAGIC # 8.6 — Fire the file-arrival trigger
# MAGIC
# MAGIC Run this once before you add the trigger, so the folder exists. Run it again once the trigger
# MAGIC is active: a new file lands, and within a minute or so the job starts on its own.
# MAGIC
# MAGIC The folder holds marker files only, so firing the trigger changes no table. The pipeline reads
# MAGIC `topics/` and `cdc/`, not this folder. In production you'd point the trigger at the real landing
# MAGIC folder and skip the marker.

# COMMAND ----------

from datetime import datetime, timezone

folder = "/Volumes/lakeshore/landing/files/platform_trigger/"
dbutils.fs.mkdirs(folder)

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
dbutils.fs.put(f"{folder}fire-{stamp}.txt", f"fired at {stamp}", overwrite=True)

marker_files = [f.name for f in dbutils.fs.ls(folder)]
print(f"{len(marker_files)} marker file(s) in {folder}")
assert f"fire-{stamp}.txt" in marker_files

# COMMAND ----------

dbutils.notebook.exit("trigger-file-landed")
