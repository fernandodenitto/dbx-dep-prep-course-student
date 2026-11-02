# Databricks notebook source
# MAGIC %md
# MAGIC # 8.5 — Read what AUTO CDC built
# MAGIC
# MAGIC Lessons 8.4 and 8.5 read from this notebook. `pipeline/02_customers_cdc.py` holds both flows, so
# MAGIC one update builds both tables. Nothing here writes: we compare the tables AUTO CDC (formerly
# MAGIC APPLY CHANGES) made with the ones you built by hand in Sections 3 and 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.4: SCD type 1, one row per surviving customer

# COMMAND ----------

scd1 = spark.table("lakeshore.silver.sdp_customers_scd1")
rows, keys = scd1.count(), scd1.select("customer_id").distinct().count()
print(f"sdp_customers_scd1: {rows} rows, {keys} customers")
assert rows == keys, "SCD1 keeps one row per key"
assert "__END_AT" not in scd1.columns, "SCD1 has no validity columns"

# COMMAND ----------

# MAGIC %md
# MAGIC Cross-check against `gold.customer_current`, the Section 4 mirror. It stops at the last time you ran
# MAGIC lesson 4.3, and the pipeline reads every CDC file in the folder, including the batch 7.6 added. So we
# MAGIC compare where both have the customer, and a small count here is that later batch, not a bug.

# COMMAND ----------

if spark.catalog.tableExists("lakeshore.gold.customer_current"):
    disagree = spark.sql("""
        SELECT count(*) FROM lakeshore.silver.sdp_customers_scd1 d
        JOIN lakeshore.gold.customer_current m USING (customer_id)
        WHERE NOT (d.email <=> m.email)""").first()[0]
    print(f"customers present in both whose email disagrees: {disagree}")
else:
    print("gold.customer_current isn't here yet: run lesson 4.3 to get the comparison.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.5: SCD type 2, the same flow with one digit changed

# COMMAND ----------

display(spark.sql("""SELECT customer_id, count(*) AS versions
                     FROM lakeshore.silver.sdp_customers_scd2
                     GROUP BY customer_id HAVING count(*) >= 2
                     ORDER BY versions DESC, customer_id LIMIT 5"""))

# COMMAND ----------

# One customer's chain: __END_AT of a version equals __START_AT of the next.
customer = spark.sql("""SELECT customer_id
                        FROM (SELECT customer_id, count(*) AS versions
                              FROM lakeshore.silver.sdp_customers_scd2
                              GROUP BY customer_id)
                        WHERE versions >= 2
                        ORDER BY versions DESC, customer_id LIMIT 1""").first()[0]
chain = spark.sql("""SELECT customer_id, email, __START_AT, __END_AT
                     FROM lakeshore.silver.sdp_customers_scd2
                     WHERE customer_id = :customer ORDER BY __START_AT""",
                  args={"customer": customer})
display(chain)

versions = chain.collect()
for older, newer in zip(versions, versions[1:]):
    assert older["__END_AT"] == newer["__START_AT"], "the chain has a gap"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deletes close the version. History stays
# MAGIC
# MAGIC A customer with a DELETE event has no open version in SCD2, and no row at all in SCD1.

# COMMAND ----------

closed = spark.sql("""SELECT count(DISTINCT customer_id) FROM lakeshore.silver.sdp_customers_scd2
                      WHERE customer_id NOT IN (SELECT customer_id
                                                FROM lakeshore.silver.sdp_customers_scd2
                                                WHERE __END_AT IS NULL)""").first()[0]
print(f"customers with history but no open version: {closed}")
in_scd1 = spark.sql("""SELECT count(*) FROM lakeshore.silver.sdp_customers_scd1
                       WHERE customer_id NOT IN (SELECT customer_id
                                                 FROM lakeshore.silver.sdp_customers_scd2
                                                 WHERE __END_AT IS NULL)""").first()[0]
assert in_scd1 == 0, "a customer closed in SCD2 should be gone from SCD1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The hand-built dimension
# MAGIC
# MAGIC Same model, different spelling: `valid_from`, `valid_to` and `is_current` by hand, `__START_AT` and
# MAGIC `__END_AT` from AUTO CDC, holding `sequence_by` values rather than event timestamps. Version counts
# MAGIC can differ because Section 3 filtered no-op updates, and AUTO CDC versions any tracked change.

# COMMAND ----------

if spark.catalog.tableExists("lakeshore.silver.dim_customer_scd2"):
    by_hand = spark.table("lakeshore.silver.dim_customer_scd2").count()
    declared = spark.table("lakeshore.silver.sdp_customers_scd2").count()
    print(f"versions by hand: {by_hand} · versions from AUTO CDC: {declared}")
else:
    print("silver.dim_customer_scd2 isn't here yet: run lesson 3.6 to get the comparison.")

# COMMAND ----------

dbutils.notebook.exit(f"scd-compared:{rows}")
