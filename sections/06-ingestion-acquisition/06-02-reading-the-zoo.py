# Databricks notebook source
# MAGIC %md
# MAGIC # 6.2 — Reading the zoo
# MAGIC
# MAGIC The six formats the exam guide names, read live: **Delta Lake, Parquet, JSON, Iceberg,
# MAGIC CSV and Binary.** JSON and CSV are already in the landing volume. This notebook writes
# MAGIC what's missing into a `zoo/` corner and reads every one of them.
# MAGIC
# MAGIC > Prereqs: fast-forward. Self-contained and idempotent. The demo tables
# MAGIC > (`bronze.zoo_iceberg`, `bronze.zoo_invoices`, `bronze.zoo_parquet_drops`) and the
# MAGIC > Auto Loader folders are removed at the end, so no later lesson sees them.

# COMMAND ----------

ZOO = "/Volumes/lakeshore/landing/files/zoo"
sample = (spark.table("lakeshore.silver.payments")
          .select("payment_id", "order_id", "amount", "method", "status")
          .orderBy("payment_id").limit(200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parquet: the file format carries its own types
# MAGIC Nobody declares anything on the way back. That's what "self-describing" means.

# COMMAND ----------

expected = [(f.name, f.dataType) for f in sample.schema.fields]

sample.coalesce(1).write.mode("overwrite").format("parquet").save(f"{ZOO}/parquet")
parquet_df = spark.read.format("parquet").load(f"{ZOO}/parquet")
got = [(f.name, f.dataType) for f in parquet_df.schema.fields]

assert parquet_df.count() == 200 and got == expected, got
print("Parquet round trip: 200 rows, typed schema preserved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. JSON and CSV: structure yes, reliable types no
# MAGIC Both through the same reader. Options are where these two differ, and `sep` makes a
# MAGIC TSV just another CSV.

# COMMAND ----------

json_df = spark.sql("""SELECT * FROM read_files(
    '/Volumes/lakeshore/landing/files/customers/',
    format => 'json')""")
csv_df = spark.sql("""SELECT * FROM read_files(
    '/Volumes/lakeshore/landing/files/payments/',
    format => 'csv', header => 'true')""")
tsv_df = spark.sql("""SELECT * FROM read_files(
    '/Volumes/lakeshore/landing/files/addresses/',
    format => 'csv', sep => '\\t', header => 'true')""")
print(f"json {json_df.count()} · csv {csv_df.count()} · tsv {tsv_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Binary: one row per file, not one row per record

# COMMAND ----------

bin_df = (spark.read.format("binaryFile")
          .load("/Volumes/lakeshore/landing/files/products/products.csv"))
bin_df.select("path", "modificationTime", "length").show(truncate=60)
assert bin_df.count() == 1
print("first bytes:", bin_df.first()["content"][:40])

# COMMAND ----------

# MAGIC %md
# MAGIC That's a CSV, read as bytes on purpose. One row, one file, the raw bytes in `content`.
# MAGIC It's the door for images, PDFs and audio, not a faster way to read tables.
# MAGIC
# MAGIC ## 4. The same formats, incrementally: Auto Loader
# MAGIC `spark.read` reads the whole folder every time. "Ingest PDFs as they arrive" means Auto Loader,
# MAGIC with `cloudFiles.format` set to `binaryFile`. Same four columns, and a checkpoint remembers
# MAGIC which files it has seen. The docs list eight values for that option: `avro`, `binaryFile`, `csv`,
# MAGIC `json`, `orc`, `parquet`, `text`, `xml`.
# MAGIC
# MAGIC Our "PDFs" are small text files with a `.pdf` name. `binaryFile` never looks inside, so it
# MAGIC can't tell, and that's the point.

# COMMAND ----------

INCOMING = f"{ZOO}/incoming"
CHECKPOINTS = f"{ZOO}/_checkpoints/zoo_autoloader"

def reset_auto_loader_demo():
    """Remove only this lesson's folders and tables: lesson 6.6 has its own checkpoint in zoo/."""
    for table in ("zoo_invoices", "zoo_parquet_drops"):
        spark.sql(f"DROP TABLE IF EXISTS lakeshore.bronze.{table}")
    for path in (INCOMING, CHECKPOINTS):
        dbutils.fs.rm(path, True)

def load_new_files(fmt, glob, source, target):
    """One Auto Loader run: only files its checkpoint hasn't seen, then stop."""
    (spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", fmt)
     .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{target}/schema")
     .option("pathGlobFilter", glob)
     .load(source)
     .writeStream
     .option("checkpointLocation", f"{CHECKPOINTS}/{target}/stream")
     .trigger(availableNow=True)
     .toTable(f"lakeshore.bronze.{target}")
     .awaitTermination())

reset_auto_loader_demo()

# COMMAND ----------

def drop_invoice(n):
    dbutils.fs.put(f"{INCOMING}/invoices/invoice-{n:03}.pdf", f"%PDF-1.4 invoice {n}", overwrite=True)

drop_invoice(1)
drop_invoice(2)
load_new_files("binaryFile", "*.pdf", f"{INCOMING}/invoices", "zoo_invoices")
assert spark.table("lakeshore.bronze.zoo_invoices").count() == 2

drop_invoice(3)
load_new_files("binaryFile", "*.pdf", f"{INCOMING}/invoices", "zoo_invoices")
invoices = spark.table("lakeshore.bronze.zoo_invoices")
assert invoices.count() == 3 and invoices.select("path").distinct().count() == 3
invoices.select("path", "length").orderBy("path").show(truncate=False)
print("Second run read one new file. Nothing was read twice.")

# COMMAND ----------

# MAGIC %md
# MAGIC `binaryFile` has a fixed schema, so nothing is inferred. The docs still recommend setting
# MAGIC `cloudFiles.schemaLocation` for it, which is why it's there. Parquet is different: Auto Loader
# MAGIC reads the types from the files and, across files, takes the widest. Two drops, two runs, and the
# MAGIC types survive without a hint.

# COMMAND ----------

first_drop = sample.limit(150)
first_drop.write.mode("append").format("parquet").save(f"{INCOMING}/parquet")
load_new_files("parquet", "*.parquet", f"{INCOMING}/parquet", "zoo_parquet_drops")
assert spark.table("lakeshore.bronze.zoo_parquet_drops").count() == 150

sample.exceptAll(first_drop).write.mode("append").format("parquet").save(f"{INCOMING}/parquet")
load_new_files("parquet", "*.parquet", f"{INCOMING}/parquet", "zoo_parquet_drops")
drops = spark.table("lakeshore.bronze.zoo_parquet_drops")
got = {f.name: f.dataType for f in drops.schema.fields}

assert drops.count() == 200, drops.count()
assert all(got[name] == dtype for name, dtype in expected), got
print("Parquet through Auto Loader: 150, then 200 rows, typed schema preserved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta and Iceberg: table formats, read as tables
# MAGIC Both are Parquet files plus a metadata layer, and both are read by name in Unity
# MAGIC Catalog. Lesson 6.6 goes deeper on Iceberg. Today, just the proof that it's the same
# MAGIC reading door.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_iceberg")
spark.sql("""CREATE TABLE lakeshore.bronze.zoo_iceberg
             (payment_id STRING, amount DOUBLE) USING ICEBERG""")
(sample.selectExpr("CAST(payment_id AS STRING) AS payment_id", "CAST(amount AS DOUBLE) AS amount")
 .createOrReplaceTempView("zoo_sample"))
spark.sql("INSERT INTO lakeshore.bronze.zoo_iceberg SELECT * FROM zoo_sample")

formats = {}
for table in ("lakeshore.silver.payments", "lakeshore.bronze.zoo_iceberg"):
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    formats[table] = detail.format
    print(f"{table:<34} format={detail.format:<8} rows={spark.table(table).count()}")

assert formats == {"lakeshore.silver.payments": "delta",
                   "lakeshore.bronze.zoo_iceberg": "iceberg"}, formats

# COMMAND ----------

# MAGIC %md
# MAGIC Same `spark.table`, same SQL, two table formats. "Ingesting Delta" or "ingesting
# MAGIC Iceberg" usually means reading another team's table by name, as a batch or as a stream.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC | Format | Read with |
# MAGIC |---|---|
# MAGIC | Parquet | `spark.read.format("parquet")`, types included |
# MAGIC | JSON, CSV | `read_files(..., format, options)`, declare a schema or accept inference |
# MAGIC | Binary | `.format("binaryFile")`: path, modificationTime, length, content |
# MAGIC | Binary or Parquet, as files arrive | Auto Loader: `cloudFiles.format`, a schema location, a checkpoint |
# MAGIC | Delta, Iceberg | by table name, batch or stream |
# MAGIC
# MAGIC Beyond the exam: Spark also reads ORC, Avro, XML (with `rowTag`) and plain text. The
# MAGIC previous guide listed them; the current one doesn't.
# MAGIC
# MAGIC Next: Auto Loader under stress, when the schema changes mid-stream.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_iceberg")
reset_auto_loader_demo()
dbutils.notebook.exit("zoo-complete")
