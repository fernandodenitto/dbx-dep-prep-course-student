# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.5 — Parameterized, modular notebook code
# MAGIC
# MAGIC The same notebook must run as: your interactive session, tonight's
# MAGIC job, and next month's backfill — with different inputs and NO edits.
# MAGIC That's parameterization, and it's the seam where notebooks meet jobs
# MAGIC (S8) and bundles (S12).
# MAGIC
# MAGIC > **↩️ Associate recap:** widgets — `dbutils.widgets` — are the
# MAGIC > notebook's parameter mechanism, and job `base_parameters` override
# MAGIC > widget defaults at run time. That mapping is the whole trick.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters first, validated early

# COMMAND ----------

dbutils.widgets.text("run_mode", "full")          # full | sample
dbutils.widgets.text("category", "ALL")           # ALL or a category name
dbutils.widgets.text("top_n", "5")

run_mode = dbutils.widgets.get("run_mode")
category = dbutils.widgets.get("category")
top_n = int(dbutils.widgets.get("top_n"))

VALID_MODES = {"full", "sample"}
assert run_mode in VALID_MODES, f"run_mode must be one of {VALID_MODES}"
assert top_n > 0, "top_n must be positive"
print(f"run_mode={run_mode} · category={category} · top_n={top_n}")

# COMMAND ----------

# MAGIC %md
# MAGIC Fail FAST and loud: a bad parameter should kill the run in cell one
# MAGIC with a readable message — not corrupt a table in cell nine. (S12's
# MAGIC debugging lesson meets these asserts again from the job side.)
# MAGIC
# MAGIC ## 2. Parameters drive the plan — not copy-pasted variants

# COMMAND ----------

from pyspark.sql.functions import col, desc, sum as fsum

base = (spark.table("lakeshore.gold.fact_order_items")
        .join(spark.table("lakeshore.gold.dim_product"), "product_id"))

if run_mode == "sample":
    base = base.limit(1000)                       # cheap dev iterations
if category != "ALL":
    base = base.filter(col("category") == category)

result = (base.groupBy("category", "product_id", "name")
          .agg(fsum("line_total").alias("revenue"),
               fsum("quantity").alias("units"))
          .orderBy(desc("revenue"))
          .limit(top_n))
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC One code path, parameterized — instead of `analysis_full.py`,
# MAGIC `analysis_sample.py`, `analysis_camping.py` drifting apart in a repo.
# MAGIC
# MAGIC ## 3. Reusable functions — and the honest limit of notebooks

# COMMAND ----------

def top_products(df, n, by="revenue"):
    """Top-n products by a measure — the kind of helper every team grows."""
    return (df.groupBy("product_id", "name")
            .agg(fsum("line_total").alias("revenue"),
                 fsum("quantity").alias("units"))
            .orderBy(desc(by)).limit(n))

top_products(base, 3).show(truncate=False)
top_products(base, 3, by="units").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Useful — and trapped in this notebook. You've now copy-pasted our
# MAGIC streaming plumbing across FOUR notebooks in this course; imagine this
# MAGIC helper drifting across forty. The escape routes:
# MAGIC - `%run ./helpers` — the legacy notebook-include: order-dependent,
# MAGIC   invisible to linters and tests. You'll see it in old codebases.
# MAGIC - **Python modules + `import`** — real files, testable, versionable.
# MAGIC   That's Section 12, and it's also the exam's answer ("modular
# MAGIC   development" is in the bundles skill statement verbatim).
# MAGIC
# MAGIC ## 4. How jobs feed these widgets (S8 preview)
# MAGIC
# MAGIC A job task running this notebook passes `base_parameters`
# MAGIC (e.g. `{"run_mode": "sample"}`) — each key overrides the widget's
# MAGIC default. Same notebook, different behavior per environment, zero
# MAGIC edits. Multi-task pipelines add `dbutils.jobs.taskValues` to pass
# MAGIC values BETWEEN tasks — Section 8 wires both live.

# COMMAND ----------

n = result.count()
assert 0 < n <= top_n, f"expected ≤{top_n} rows, got {n}"
print(f"✓ parameterized run produced {n} rows (mode={run_mode})")

# COMMAND ----------

dbutils.notebook.exit(f"parameterized-complete:{run_mode}")
