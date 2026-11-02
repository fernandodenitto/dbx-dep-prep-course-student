# Databricks notebook source
# MAGIC %md
# MAGIC # 12.2 Project structure and imports: the debt, paid
# MAGIC
# MAGIC Several sections pasted the same streaming plumbing between notebooks and said
# MAGIC "Section 12 fixes this." Today it does: the logic moves into a real Python package,
# MAGIC `bundle/src/lakeshore_pro`, inside a real project with tests, a job and a pipeline.
# MAGIC The same files deploy as a Declarative Automation Bundle (formerly Databricks Asset
# MAGIC Bundle) in lesson 12.5.
# MAGIC
# MAGIC > Run this from the course Git folder. Prereq: `lakeshore.bronze.bus_raw` from lesson 3.2.

# COMMAND ----------

# MAGIC %pip install "pytest==8.*" --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What Python can already see
# MAGIC
# MAGIC The docs give two rules. A notebook's working directory goes on `sys.path`, and in a Git
# MAGIC folder **the root of the repo** goes there too. They also warn that on serverless the
# MAGIC working directory is not guaranteed, so build absolute paths. We measured it from a Git
# MAGIC folder on serverless jobs: it was the notebook's own folder, which the earlier notebooks'
# MAGIC bootstrap relies on. That's a measurement, not a guarantee, and a package with relative
# MAGIC imports is more robust, because they resolve from the package itself. Here we find the repo
# MAGIC root through the rule: it is the `sys.path` entry that holds `datagen/`.

# COMMAND ----------

import os
import sys

root = next((p for p in sys.path if os.path.isdir(f"{p}/datagen")), None)
assert root, "Run this notebook from the course Git folder: the repo root must be on sys.path"

BUNDLE = f"{root}/sections/12-debugging-deploying/bundle"
print("Git folder root:", root)

for folder, subfolders, files in os.walk(BUNDLE):
    subfolders[:] = sorted(s for s in subfolders if not s.startswith((".", "__")))
    depth = folder[len(BUNDLE):].count("/")
    print("  " * depth + (os.path.basename(folder) or "bundle") + "/")
    for name in sorted(files):
        print("  " * (depth + 1) + name)

# COMMAND ----------

# MAGIC %md
# MAGIC That tree is the project, and every file in it is used:
# MAGIC
# MAGIC | Folder | Holds | Who reads it |
# MAGIC |---|---|---|
# MAGIC | `src/lakeshore_pro/` | the logic, as a package | notebooks, the pipeline, the tests |
# MAGIC | `notebooks/`, `pipelines/` | **thin** entry points: parameters in, import, run, exit | the job, the pipeline |
# MAGIC | `tests/` | pytest, fake data | the job's first task, and CI |
# MAGIC | `resources/`, `databricks.yml` | the wiring, per environment | `databricks bundle` (12.5) |
# MAGIC | `ci/` | sample pipelines for your Git provider | GitHub Actions (12.8) |
# MAGIC | `pyproject.toml` | pytest settings, wheel metadata | pytest, `uv build` |
# MAGIC
# MAGIC ## 2. Absolute imports from a notebook, relative imports inside the package
# MAGIC
# MAGIC `src` is not the repo root, so we add it to `sys.path`, once, with an absolute path.

# COMMAND ----------

sys.path.insert(0, f"{BUNDLE}/src")

import lakeshore_pro as lk
from lakeshore_pro.schemas import ORDER_PAYLOAD

print("imported from:", lk.__file__)
print("public API:", [n for n in dir(lk) if not n.startswith("_") and callable(getattr(lk, n))])

# COMMAND ----------

# Inside the package, modules find each other with a dot. Read the real lines:
for module in ["__init__", "orders"]:
    with open(f"{BUNDLE}/src/lakeshore_pro/{module}.py") as source:
        print(module, "->", [line.strip() for line in source if line.startswith("from .")])

# COMMAND ----------

# The same dot from a notebook fails. A notebook is a top-level script, not a package member:
try:
    from .schemas import ORDER_PAYLOAD
    raise AssertionError("expected an ImportError")
except ImportError as error:
    print("from a notebook:", error)

# COMMAND ----------

# MAGIC %md
# MAGIC Two things are called "relative", and the exam scenarios mix them up:
# MAGIC
# MAGIC | | Relative **path** | Relative **import** |
# MAGIC |---|---|---|
# MAGIC | Looks like | `sys.path.append(os.path.abspath(".."))` | `from .schemas import ORDER_PAYLOAD` |
# MAGIC | Relative to | the working directory | the package the file belongs to |
# MAGIC | Works in | any file, if the working directory is what you assumed | files inside a package only |
# MAGIC | Risk | on serverless the working directory is not guaranteed | `ImportError` from a notebook or a top-level script |
# MAGIC
# MAGIC The package moves as one unit, because its internals use dots. That is why it can live
# MAGIC in a Git folder, in a bundle's deployed files, or in a wheel without changing a line.
# MAGIC
# MAGIC ## 3. The plumbing, imported and never pasted again

# COMMAND ----------

parsed = lk.parse_bus_orders(spark.table("lakeshore.bronze.bus_raw"))
graded = parsed.transform(lk.reject_reason).transform(lk.add_cart_total)

graded.select("order_id", "cart_total", "reject_reason").orderBy("order_id").show(5)

rejects = graded.filter("reject_reason IS NOT NULL").count()
valid = graded.filter("reject_reason IS NULL")
print(f"valid {valid.count()}, of which unique orders {valid.dropDuplicates(['order_id']).count()}")
print(f"rejects {rejects}")
assert rejects >= 3   # our batch-4 corruptions, recognized by module code

# COMMAND ----------

# MAGIC %md
# MAGIC The parsing and the ladder are the same rules Sections 3 and 8 use, now with one home.
# MAGIC This cell does not deduplicate, so `valid` counts the bus's duplicates too. Lesson 3.5's
# MAGIC watermark handles those, and it stays where it is: dedup is a property of a stream, not
# MAGIC of a helper function.
# MAGIC
# MAGIC What a module buys over `%run`:
# MAGIC
# MAGIC | | `%run ./helpers` (legacy include) | `import lakeshore_pro` |
# MAGIC |---|---|---|
# MAGIC | Namespace | dumps into yours, names collide | a proper module scope |
# MAGIC | Testable | not from pytest: it is a notebook | pytest, in the workspace and in CI |
# MAGIC | Reviewable | a notebook diff | a plain-file diff in a pull request |
# MAGIC | Packages | no | yes, including relative imports |
# MAGIC
# MAGIC ## 4. The real tests, run from here
# MAGIC
# MAGIC `tests/test_orders.py` uses the toolkit from lesson 12.1 against the package. The docs'
# MAGIC recipe for pytest in a workspace: no bytecode files, no cache provider, assert the code.

# COMMAND ----------

import pytest

sys.dont_write_bytecode = True
os.chdir(BUNDLE)
exit_code = pytest.main(["tests", "-v", "-p", "no:cacheprovider", f"--rootdir={BUNDLE}"])
assert exit_code == 0, "pytest failed: read the report above"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Source on `sys.path`, or a wheel?
# MAGIC
# MAGIC This project deploys source files and puts `src` on `sys.path`, which needs no build
# MAGIC step. A wheel is the same `src`, packaged. We tested this addition to the bundle on Free
# MAGIC Edition serverless, and the import then resolved from the job's Python environment:
# MAGIC
# MAGIC ```yaml
# MAGIC artifacts:                       # databricks.yml
# MAGIC   lakeshore_pro_whl:
# MAGIC     type: whl
# MAGIC     build: uv build --wheel
# MAGIC     path: .
# MAGIC ```
# MAGIC ```yaml
# MAGIC dependencies:                    # the job's environment spec
# MAGIC   - pytest==8.*
# MAGIC   - ../dist/*.whl
# MAGIC ```
# MAGIC
# MAGIC | Choose | When |
# MAGIC |---|---|
# MAGIC | Source on `sys.path` | one project, code and notebooks versioned together |
# MAGIC | Wheel | other projects install it, or you want a versioned, immutable artifact |
# MAGIC
# MAGIC ## Recap
# MAGIC
# MAGIC Logic in a package, thin entry points, tests beside the code, wiring in YAML. Next:
# MAGIC breaking a job on purpose, and repairing it.

# COMMAND ----------

dbutils.notebook.exit("project-structure-complete")
