# Databricks notebook source
# MAGIC %md
# MAGIC # 6.2 — Reading the zoo
# MAGIC
# MAGIC The six formats the exam guide names, read live: **Delta Lake, Parquet, JSON, Iceberg,
# MAGIC CSV and Binary.** JSON and CSV are already in the landing volume. This notebook writes
# MAGIC what's missing into a `zoo/` corner and reads every one of them.
# MAGIC
# MAGIC > Prereqs: fast-forward. Self-contained and idempotent. The one demo table,
# MAGIC > `bronze.zoo_iceberg`, is dropped at the end.

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
# MAGIC ## 4. Delta and Iceberg: table formats, read as tables
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
# MAGIC | Delta, Iceberg | by table name, batch or stream |
# MAGIC
# MAGIC Beyond the exam: Spark also reads ORC, Avro, XML (with `rowTag`) and plain text. The
# MAGIC previous guide listed them; the current one doesn't.
# MAGIC
# MAGIC Next: Auto Loader under stress, when the schema changes mid-stream.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.zoo_iceberg")
dbutils.notebook.exit("zoo-complete")
