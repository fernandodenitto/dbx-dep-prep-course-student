# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.4 — Libraries: PyPI, wheels, source archives, and when they fight
# MAGIC
# MAGIC The guide: "Apply troubleshooting techniques to dependency conflicts and
# MAGIC installation failures for external libraries (PyPI, local wheels, source archives)
# MAGIC across serverless, pipeline, and bundle-deployed environments."
# MAGIC
# MAGIC So today: package a real library, install it, make it conflict on purpose and read
# MAGIC the error, then see where the same declaration lives in a job, a pipeline and a
# MAGIC bundle.
# MAGIC
# MAGIC > Run this from the course Git folder: the library's source sits beside this
# MAGIC > notebook, in `05-04-lakeshore-fmt/`. Serverless has two doors for a library, the
# MAGIC > Environment side pane and `%pip`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The Environment side pane (walk it on screen)
# MAGIC
# MAGIC Open **Environment** on the right. What to narrate:
# MAGIC - **Base environment**: Standard by default.
# MAGIC - **Dependencies**: anything valid in a `requirements.txt` — PyPI specs
# MAGIC   (`tabulate==0.9.0`), workspace files (`/Workspace/...`), volume paths
# MAGIC   (`/Volumes/<cat>/<schema>/<vol>/pkg.whl`).
# MAGIC - **+Add dependency**, then **Apply**: it installs and restarts Python. The
# MAGIC   environment is **cached**, so reopening the notebook doesn't reinstall.
# MAGIC - The **Installed** tab and the **pip logs** link: where you look when it fails.
# MAGIC - The rule that breaks sessions: **never install `pyspark`** (or anything that drags
# MAGIC   it in). The docs say doing so stops your session.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the artifacts
# MAGIC
# MAGIC `lakeshore-fmt` is a four-line library that declares one real dependency,
# MAGIC `tabulate>=0.9`. We build the wheel with `pip wheel` right here, pack the same source
# MAGIC as a source archive, and copy both to a volume, because that's the canonical way to
# MAGIC distribute private code. Everything is stdlib plus pip.

# COMMAND ----------

import os, shutil, subprocess, sys, tarfile, tempfile

LIBS = "/Volumes/lakeshore/landing/files/libs"
SOURCE = f"{os.getcwd()}/05-04-lakeshore-fmt"
VERSION = "1.0.0"

def pip(*args):
    """Run pip, and hand back its return code and its output to read."""
    done = subprocess.run([sys.executable, "-m", "pip", *args],
                          capture_output=True, text=True)
    return done.returncode, done.stdout + done.stderr

os.makedirs(LIBS, exist_ok=True)
work = tempfile.mkdtemp()

# pip writes build folders next to the source, so build from a temporary copy
shutil.copytree(SOURCE, f"{work}/src")
code, output = pip("wheel", "--no-deps", "--quiet", "-w", f"{work}/dist", f"{work}/src")
assert code == 0, output

# A source archive is just the source tree, tarred: pip builds it at install time
with tarfile.open(f"{work}/dist/lakeshore_fmt-{VERSION}.tar.gz", "w:gz") as archive:
    archive.add(SOURCE, arcname=f"lakeshore_fmt-{VERSION}")

for name in sorted(os.listdir(f"{work}/dist")):
    shutil.copy(f"{work}/dist/{name}", f"{LIBS}/{name}")
    print(f"{LIBS}/{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read a conflict before you have one
# MAGIC
# MAGIC Here's the failure the guide names. A teammate pinned `tabulate==0.8.10` for an old
# MAGIC report, and our wheel needs `>=0.9`. We ask pip to resolve both with `--dry-run`, so
# MAGIC this session stays clean, and read what it says. In a `%pip` cell, or in the pane's
# MAGIC **pip logs**, you'd see these same lines.

# COMMAND ----------

WHEEL = f"{LIBS}/lakeshore_fmt-{VERSION}-py3-none-any.whl"

code, output = pip("install", "--dry-run", WHEEL, "tabulate==0.8.10")
print(output[-700:])
assert code != 0 and "ResolutionImpossible" in output, "the two pins must conflict"

# COMMAND ----------

# MAGIC %md
# MAGIC Read it from the bottom: `ResolutionImpossible`, then the two requirements that can't
# MAGIC both hold, with who asked for each. The cure is to change a requirement, not to retry.
# MAGIC Either move the pin, or pin a version that both sides accept. Let's prove the second
# MAGIC one resolves, and show the other common failure: a version that doesn't exist.

# COMMAND ----------

code, output = pip("install", "--dry-run", "--quiet", WHEEL, "tabulate==0.9.0")
assert code == 0, output
print("wheel + tabulate==0.9.0: resolves")

code, output = pip("install", "--dry-run", "tabulate==99.0")
print(output[-260:])
assert code != 0 and "No matching distribution" in output

# The source archive: pip has to BUILD it first, which is why it's slower and needs a
# build backend it can fetch. It resolves the same dependency as the wheel.
code, output = pip("install", "--dry-run", f"{LIBS}/lakeshore_fmt-{VERSION}.tar.gz")
assert code == 0, output
print("source archive: built and resolved at install time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The real install: `%pip` from a volume path
# MAGIC
# MAGIC `%pip` changes the current session, and `restartPython()` reloads the interpreter so
# MAGIC the new package imports. Everything above this cell is gone from memory, so
# MAGIC re-import what you need. Variables do not survive the restart.

# COMMAND ----------

# MAGIC %pip install /Volumes/lakeshore/landing/files/libs/lakeshore_fmt-1.0.0-py3-none-any.whl --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from importlib.metadata import version

import lakeshore_fmt

rows = (spark.sql("""SELECT category, round(sum(line_total), 2) AS revenue
                     FROM lakeshore.gold.fact_order_items f
                     JOIN lakeshore.gold.dim_product p USING (product_id)
                     GROUP BY category ORDER BY revenue DESC""")
        .collect())
print(lakeshore_fmt.revenue_table([(r.category, r.revenue) for r in rows]))
print(f"\n✓ lakeshore-fmt {version('lakeshore-fmt')} from the volume, "
      f"tabulate {version('tabulate')} from PyPI, resolved for us")
assert version("lakeshore-fmt") == "1.0.0"

# COMMAND ----------

# MAGIC %md
# MAGIC That one install used two artifact kinds: a wheel by path, and a PyPI package pulled
# MAGIC in by the wheel's declared dependency.
# MAGIC
# MAGIC ## 5. The same declaration, in three places
# MAGIC
# MAGIC | | Notebook or job on serverless | Lakeflow pipeline | Bundle-deployed |
# MAGIC |---|---|---|---|
# MAGIC | Where it's declared | Environment side pane, `%pip` cell, or the job's environment | Pipeline **Settings** → **Pipeline environment** → **Edit environment** → **Add dependency** | `environments[].spec.dependencies` on a job; `environment.dependencies` on a pipeline |
# MAGIC | `%pip` + restart | Works | **No**: pipelines don't support `restartPython()` | Works in a notebook task |
# MAGIC | Wheel from a volume | Yes | Yes | Yes, or built from `artifacts` |
# MAGIC | Task `libraries:` | Not for serverless notebook tasks | Doesn't apply | Yes for whl and pypi on other task types |
# MAGIC | Init scripts | Not on serverless | **Not on serverless pipelines** | Not on serverless |
# MAGIC | First place to look | **pip logs**, then the task's output | The update's event log | `bundle validate`, then the run's output |
# MAGIC
# MAGIC Bundle YAML is taught in Section 12; this is the shape to recognise:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     report:
# MAGIC       environments:
# MAGIC         - environment_key: default
# MAGIC           spec:
# MAGIC             environment_version: "4"
# MAGIC             dependencies:
# MAGIC               - /Volumes/lakeshore/landing/files/libs/lakeshore_fmt-1.0.0-py3-none-any.whl
# MAGIC   pipelines:
# MAGIC     enrich:
# MAGIC       environment:
# MAGIC         dependencies:
# MAGIC           - tabulate==0.9.0
# MAGIC ```
# MAGIC
# MAGIC ## 6. Troubleshooting, the greatest hits
# MAGIC
# MAGIC | Symptom | Cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `ResolutionImpossible` | Two pins can't both hold | Move a pin; read who asked for what |
# MAGIC | `No matching distribution` | Version doesn't exist, or the index can't be reached | Check the version and the index |
# MAGIC | `ModuleNotFoundError` in a job, fine interactively | It was installed only in your session | Keep the `%pip` cell in the notebook, or declare it. Prefer declaring: pinned, cached, reproducible |
# MAGIC | Works on one run, native error on the next | Native wheel, and serverless doesn't guarantee `aarch64` or `x86_64` | Prefer `py3-none-any`, or ship both with a `platform_machine` marker |
# MAGIC | Session dies on install | You (or a dependency) installed `pyspark` | Remove it, reset the environment |
# MAGIC | New wheel, old behaviour | Same version number, cached environment | Bump the version |
# MAGIC | Pipeline update fails on `restartPython()` | Pipelines don't support it | Move to the pipeline's environment |
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Pane = declarative and cached; `%pip` = imperative and session-scoped. PyPI, wheel,
# MAGIC source archive: all three install by the same pip, with different costs. Conflicts
# MAGIC mean a requirement has to change, and pip tells you which. Next: parameterizing the
# MAGIC notebooks that use all this.

# COMMAND ----------

dbutils.notebook.exit("libraries-complete")
