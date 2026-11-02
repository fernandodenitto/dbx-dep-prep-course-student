# Databricks notebook source
# MAGIC %md
# MAGIC # Smoke check, deployed by the lakeshore-pro bundle
# MAGIC A thin entry point: parameters in, import, run, exit. Bundle variables arrive as job
# MAGIC parameters. `src_path` is an absolute path, because on serverless the working
# MAGIC directory is not guaranteed.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "lakeshore")
dbutils.widgets.text("output_schema", "")
dbutils.widgets.text("src_path", "")

catalog = dbutils.widgets.get("catalog")
output_schema = dbutils.widgets.get("output_schema")
sys.path.insert(0, dbutils.widgets.get("src_path") or os.path.abspath("../src"))

import lakeshore_pro

print(f"lakeshore_pro {lakeshore_pro.__version__} from {lakeshore_pro.__file__}")

# COMMAND ----------

MUST_EXIST = [
    f"{catalog}.bronze.bus_raw",
    f"{catalog}.silver.bus_orders",
    f"{catalog}.silver.sdp_orders_valid",
    f"{catalog}.gold.fact_order_items",
    f"{catalog}.gold.deletion_audit",
    f"{catalog}.{output_schema}.graded_orders",   # built by this bundle's own pipeline
]

missing = [t for t in MUST_EXIST if not spark.catalog.tableExists(t)]
for t in MUST_EXIST:
    print(f"{'MISSING' if t in missing else 'ok     '} {t}")

assert not missing, f"platform smoke check failed: {missing}"

# COMMAND ----------

dbutils.notebook.exit(f"smoke-ok:{catalog}:{len(MUST_EXIST)} tables")
