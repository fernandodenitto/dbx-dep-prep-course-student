# Databricks notebook source
# MAGIC %md
# MAGIC # 1.4 — Fast-forward: rebuild the Lakeshore baseline
# MAGIC
# MAGIC One notebook, one run, and you have every baseline object this course reads:
# MAGIC
# MAGIC | Layer | Objects |
# MAGIC |---|---|
# MAGIC | Namespace | catalog `lakeshore`, schemas `landing/bronze/silver/gold`, volume `landing.files` |
# MAGIC | Files | batch_001 for all 6 entities (JSON/CSV/TSV), seed 2026 |
# MAGIC | Bronze | `bronze.products` (20 rows, constraints) |
# MAGIC | Silver | `customers`, `addresses`, `orders`, `order_items`, `payments`, `refunds` |
# MAGIC
# MAGIC **Idempotent by design**: every file and table is created only if missing.
# MAGIC - Coming from my Associate course? Run it anyway — it finds your files and
# MAGIC   tables, writes nothing over them, and reports what it left alone.
# MAGIC - Starting here? It builds the baseline in a couple of minutes.
# MAGIC
# MAGIC > **Before running:** clone the student repo
# MAGIC > `fernandodenitto/dbx-dep-prep-course-student` as a Git folder and run this
# MAGIC > notebook from where it sits: it finds `datagen/datagen.py` by walking up to
# MAGIC > the repo root. That file is course material, not plumbing: we read it
# MAGIC > together in the next lesson.

# COMMAND ----------

# The catalog is parameterized ONLY so you can sandbox a full rebuild without
# touching your real one (and so we can test this notebook). Students: leave
# the default.
dbutils.widgets.text("catalog", "lakeshore")
CATALOG = dbutils.widgets.get("catalog")
LANDING = f"/Volumes/{CATALOG}/landing/files"
print(f"Baseline target: catalog `{CATALOG}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Namespace — catalog, schemas, volume
# MAGIC
# MAGIC `IF NOT EXISTS` everywhere: this is what makes re-runs safe.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} "
          "COMMENT 'Lakeshore Outfitters — course data platform'")
for schema, comment in [("landing", "Raw incoming files"),
                        ("bronze", "Raw data, as ingested"),
                        ("silver", "Cleaned and validated"),
                        ("gold", "Business-level aggregates")]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.files "
          "COMMENT 'Landing zone for raw Lakeshore files'")
print("✓ namespace ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land batch_001 — six entities, three formats
# MAGIC
# MAGIC `datagen.py` is deterministic (seed 2026), so everybody's files hold the same
# MAGIC rows. A file that is already there stays there: its modification time doesn't
# MAGIC change either, which matters if a stream is watching that folder.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

from datagen import generate_all, write_csv, write_json

data = generate_all(seed=2026, n_customers=200, n_orders=1000)

targets = {
    "customers": (write_json, "customers_batch_001.json"),
    "orders":    (write_json, "orders_batch_001.json"),
    "addresses": (write_csv,  "addresses_batch_001.tsv"),
    "products":  (write_csv,  "products.csv"),
    "payments":  (write_csv,  "payments_batch_001.csv"),
    "refunds":   (write_csv,  "refunds_batch_001.csv"),
}
for entity, (writer, filename) in targets.items():
    folder = f"{LANDING}/{entity}"
    path = f"{folder}/{filename}"
    os.makedirs(folder, exist_ok=True)
    if os.path.exists(path):
        print(f"= {entity:<10} {filename} is there — untouched")
    elif filename.endswith(".tsv"):
        writer(data[entity], path, delimiter="\t")
        print(f"✓ {entity:<10} → {filename} ({len(data[entity])} rows)")
    else:
        writer(data[entity], path)
        print(f"✓ {entity:<10} → {filename} ({len(data[entity])} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze + silver core — created only if missing
# MAGIC
# MAGIC Each table below has the same columns as the Associate course's table (that
# MAGIC course built them lesson by lesson; here we fast-forward). The guard is
# MAGIC `spark.catalog.tableExists`: your existing tables are never replaced.

# COMMAND ----------

def ensure(table_name, build_fn):
    """Create `table_name` via build_fn() only if it doesn't exist yet."""
    full = f"{CATALOG}.{table_name}"
    if spark.catalog.tableExists(full):
        print(f"= {full} exists — untouched ({spark.table(full).count()} rows)")
    else:
        build_fn(full)
        print(f"✓ {full} built ({spark.table(full).count()} rows)")

# COMMAND ----------

# bronze.products: the fixed catalog, with the Associate's constraints
def build_products(full):
    spark.sql(f"""
        CREATE TABLE {full} (
          product_id STRING NOT NULL,
          name       STRING NOT NULL,
          category   STRING,
          unit_price DOUBLE)
    """)
    spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT positive_price CHECK (unit_price > 0)")
    (spark.createDataFrame(data["products"])
     .select("product_id", "name", "category", "unit_price")
     .write.mode("append").saveAsTable(full))

ensure("bronze.products", build_products)

# COMMAND ----------

# silver.customers: typed, one row per customer (QUALIFY keeps the newest)
def build_customers(full):
    spark.sql(f"""
        CREATE TABLE {full} AS
        SELECT customer_id, first_name, last_name, email,
               CAST(created_at AS TIMESTAMP) AS created_at
        FROM json.`{LANDING}/customers/customers_batch_001.json`
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY customer_id ORDER BY created_at DESC) = 1
    """)

ensure("silver.customers", build_customers)

# COMMAND ----------

# silver.addresses: landed LONG (one row per address), wanted WIDE (one row per
# customer). Section 10 joins on shipping_state.
from pyspark.sql.functions import col, max as max_, when

def build_addresses(full):
    long = spark.read.csv(f"{LANDING}/addresses/addresses_batch_001.tsv",
                          header=True, sep="\t", inferSchema=True)
    wide = [max_(when(col("address_type") == kind, col(field))).alias(f"{kind}_{field}")
            for kind in ("shipping", "billing")
            for field in ("street", "city", "state", "postcode")]
    long.groupBy("customer_id").agg(*wide).write.saveAsTable(full)

ensure("silver.addresses", build_addresses)

# COMMAND ----------

# silver.orders (headers) + silver.order_items (exploded carts).
# Schema is DECLARED, not inferred: at silver, types are a contract.
from pyspark.sql.functions import explode, lit, size, to_timestamp

ORDERS_SCHEMA = ("order_id STRING, customer_id STRING, order_ts STRING, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")

def read_orders():
    return (spark.read.schema(ORDERS_SCHEMA)
            .json(f"{LANDING}/orders/orders_batch_001.json")
            .select("*", col("_metadata.file_path").alias("source_file"))
            .withColumn("order_ts", to_timestamp("order_ts")))

def build_orders(full):
    (read_orders()
     .withColumn("sales_channel", lit(None).cast("string"))
     .withColumn("n_items", size(col("items")))
     .select("order_id", "customer_id", "order_ts",
             "sales_channel", "n_items", "source_file")
     .write.saveAsTable(full))

def build_order_items(full):
    (read_orders()
     .select("order_id", "customer_id", "order_ts",
             explode(col("items")).alias("item"))
     .select("order_id", "customer_id", "order_ts",
             col("item.product_id").alias("product_id"),
             col("item.quantity").alias("quantity"),
             col("item.unit_price").alias("unit_price"))
     .withColumn("line_total", col("quantity") * col("unit_price"))
     .write.saveAsTable(full))

ensure("silver.orders", build_orders)
ensure("silver.order_items", build_order_items)

# COMMAND ----------

# silver.payments: timestamps typed, numeric status decoded to words
from pyspark.sql.functions import to_date, when

def build_payments(full):
    (spark.read.option("header", True).option("inferSchema", True)
     .csv(f"{LANDING}/payments/payments_batch_001.csv")
     .withColumn("payment_ts", to_timestamp(col("payment_ts")))
     .withColumn("payment_date", to_date(col("payment_ts")))
     .withColumn("status",
                 when(col("status_code") == 1, "completed")
                 .when(col("status_code") == 2, "pending")
                 .when(col("status_code") == 3, "failed")
                 .otherwise("unknown"))
     .select("payment_id", "order_id", "payment_ts", "payment_date",
             "amount", "method", "status")
     .write.saveAsTable(full))

ensure("silver.payments", build_payments)

# COMMAND ----------

# silver.refunds: the packed "REASON|source" column split in two
from pyspark.sql.functions import split

def build_refunds(full):
    (spark.read.option("header", True).option("inferSchema", True)
     .csv(f"{LANDING}/refunds/refunds_batch_001.csv")
     .select("refund_id", "payment_id",
             to_timestamp(col("refund_ts")).alias("refund_ts"),
             col("amount").alias("refund_amount"),
             split(col("reason"), r"\|")[0].alias("reason"),
             split(col("reason"), r"\|")[1].alias("refund_source"))
     .write.saveAsTable(full))

ensure("silver.refunds", build_refunds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify the baseline
# MAGIC
# MAGIC The two starting points really differ. A finished Associate course leaves
# MAGIC 1,080 orders, and 10 customers with a NULL email. A fresh run gives 1,000
# MAGIC orders and no NULL emails. **No lesson in this course depends on exact counts
# MAGIC of baseline tables.** What must hold:

# COMMAND ----------

checks = {
    f"{CATALOG}.bronze.products":     20,    # exactly: fixed catalog
    f"{CATALOG}.silver.customers":    200,   # exactly: deduped pool
    f"{CATALOG}.silver.addresses":    200,   # exactly: one wide row per customer
    f"{CATALOG}.silver.orders":       1000,  # at least
    f"{CATALOG}.silver.order_items":  1000,  # at least
    f"{CATALOG}.silver.payments":     1000,  # at least
    f"{CATALOG}.silver.refunds":      1,     # at least
}
for table, minimum in checks.items():
    n = spark.table(table).count()
    assert n >= minimum, f"{table}: expected ≥ {minimum}, got {n}"
    print(f"✓ {table}: {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Fast-forward complete
# MAGIC
# MAGIC The Lakeshore lakehouse is live: raw files in the landing volume, a
# MAGIC constrained bronze catalog table, and a typed silver core. If you want the
# MAGIC full story of how each of these was designed, that's the Associate course.
# MAGIC From here on we build forward: this baseline is our production starting
# MAGIC point, and the next lesson upgrades our data generator for Professional-grade
# MAGIC feeds.
# MAGIC
# MAGIC One more setup step lives outside this notebook: the Databricks CLI on your
# MAGIC own machine, installed and logged in once. Sections 9 to 12 use it. The lesson
# MAGIC shows the commands; check with `databricks current-user me`.

# COMMAND ----------

dbutils.notebook.exit("fast-forward-complete")
