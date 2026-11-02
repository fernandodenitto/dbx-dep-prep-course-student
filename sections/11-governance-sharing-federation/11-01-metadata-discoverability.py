# Databricks notebook source
# MAGIC %md
# MAGIC # 11.1 — Metadata & discoverability
# MAGIC
# MAGIC A lakehouse nobody can FIND things in is a swamp with grants. The guide's statement:
# MAGIC "Demonstrate understanding of Unity Catalog tags and comments as mechanisms for adding
# MAGIC metadata to securable objects to improve data discoverability." Securable objects, plural:
# MAGIC not just tables. Comments and tags at every level, then the part that writes itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Comments: on every kind of securable

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON CATALOG lakeshore IS 'Lakeshore Outfitters lakehouse. Owner: data platform.';
# MAGIC
# MAGIC COMMENT ON SCHEMA lakeshore.gold IS 'Consumption layer: facts, dimensions, aggregates.';
# MAGIC
# MAGIC COMMENT ON VOLUME lakeshore.landing.files IS 'Raw files as they land. Personal data: yes.';
# MAGIC
# MAGIC COMMENT ON TABLE lakeshore.gold.fact_order_items IS
# MAGIC   'Order line items (grain: one row per line). Fed by silver.order_items;
# MAGIC    joins dim_product on product_id. Owner: data platform.';
# MAGIC
# MAGIC ALTER TABLE lakeshore.gold.fact_order_items
# MAGIC   ALTER COLUMN line_total
# MAGIC   COMMENT 'quantity * unit_price at order time (order currency)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comments are data too. Each level has its own information_schema view.
# MAGIC SELECT 'schema' AS level, schema_name AS name, comment
# MAGIC FROM lakeshore.information_schema.schemata WHERE schema_name = 'gold'
# MAGIC UNION ALL
# MAGIC SELECT 'volume', volume_name, comment
# MAGIC FROM lakeshore.information_schema.volumes WHERE volume_name = 'files'
# MAGIC UNION ALL
# MAGIC SELECT 'table', table_name, comment
# MAGIC FROM lakeshore.information_schema.tables WHERE table_name = 'fact_order_items'
# MAGIC UNION ALL
# MAGIC SELECT 'column', column_name, comment
# MAGIC FROM lakeshore.information_schema.columns
# MAGIC WHERE table_name = 'fact_order_items' AND column_name = 'line_total';

# COMMAND ----------

# MAGIC %md
# MAGIC Anyone with BROWSE can read a comment, which is what makes it discovery: they find the
# MAGIC table before they have access to it. Comments surface in Catalog Explorer and in workspace
# MAGIC search. State the grain, the source, the owner.
# MAGIC
# MAGIC Who may write one differs, and that's a question the exam can ask:
# MAGIC
# MAGIC | On | You need |
# MAGIC |---|---|
# MAGIC | Catalog, schema, volume | owner or MANAGE |
# MAGIC | Table or column | owner, or MODIFY + SELECT, plus the usage keys |
# MAGIC | View or materialized view | **owner. MANAGE isn't enough** |
# MAGIC
# MAGIC `COMMENT ON FUNCTION` doesn't parse (measured): set a function's comment in `CREATE
# MAGIC FUNCTION ... COMMENT`, as lesson 10.8 did. **AI-generated comments** in Catalog Explorer
# MAGIC draft table and column descriptions: edit them, then save. A wrong description is worse
# MAGIC than none.
# MAGIC
# MAGIC ## 2. Tags: at every level

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON SCHEMA lakeshore.gold `domain` = `sales`;
# MAGIC
# MAGIC SET TAG ON VOLUME lakeshore.landing.files `contains_pii` = `yes`;
# MAGIC
# MAGIC SET TAG ON TABLE lakeshore.silver.bus_customers_masked `data_classification` = `deidentified`;
# MAGIC SET TAG ON TABLE lakeshore.silver.token_vault `data_classification` = `restricted`;
# MAGIC
# MAGIC SET TAG ON COLUMN lakeshore.silver.token_vault.token `data_classification` = `restricted`;
# MAGIC
# MAGIC SET TAG ON FUNCTION lakeshore.silver.f_contact_mask `domain` = `security`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Governance you can SELECT. One view per level.
# MAGIC SELECT 'schema' AS level, schema_name AS name, tag_name, tag_value
# MAGIC FROM lakeshore.information_schema.schema_tags
# MAGIC UNION ALL
# MAGIC SELECT 'volume', volume_name, tag_name, tag_value
# MAGIC FROM lakeshore.information_schema.volume_tags
# MAGIC UNION ALL
# MAGIC SELECT 'table', table_name, tag_name, tag_value
# MAGIC FROM lakeshore.information_schema.table_tags
# MAGIC UNION ALL
# MAGIC SELECT 'column', concat(table_name, '.', column_name), tag_name, tag_value
# MAGIC FROM lakeshore.information_schema.column_tags
# MAGIC ORDER BY level, name;

# COMMAND ----------

# MAGIC %md
# MAGIC Comments are prose for humans; **tags are key-value pairs for machines**: classification
# MAGIC sweeps ("list everything restricted"), search, cost attribution. The rules:
# MAGIC
# MAGIC - **To tag:** you own the object, or hold APPLY TAG on it plus the usage keys.
# MAGIC - **Inheritance:** a tag on a catalog or schema applies to the objects beneath it for ABAC
# MAGIC   evaluation and for search, but **never down to columns**.
# MAGIC - **Limits:** 50 tags per object, and one column per statement.
# MAGIC - **Plain text:** tag data may be replicated globally. Never put personal data in a tag.
# MAGIC - **Search** needs exact terms.
# MAGIC
# MAGIC ## 3. Regular tags, governed tags, system tags
# MAGIC
# MAGIC Every tag above is a regular tag: any key, any value, by anyone with APPLY TAG. So you'll end
# MAGIC up with `PII`, `pii` and `has_pii` (keys are case sensitive). A **governed tag** fixes that:
# MAGIC an account-level definition of the allowed values, and an ASSIGN permission on who may apply
# MAGIC it. Only governed tags work in ABAC policies. **System tags** are governed tags Databricks
# MAGIC predefines, like `class.email_address` from lesson 10.4.

# COMMAND ----------

# Lesson 10.8 created this governed tag. Off-list values are refused.
try:
    spark.sql("SET TAG ON COLUMN lakeshore.silver.token_vault.token `lakeshore_class` = `whatever`")
    raise AssertionError("expected the value to be refused")
except Exception as e:
    assert "UC_TAG_POLICY_VALUE_NOT_ALLOWED" in str(e), str(e)[:300]
    print("✓ a governed tag refused a value outside its list")

# A system governed tag: Databricks defines the key and its values.
spark.sql("SET TAG ON TABLE lakeshore.gold.fact_order_items `system.certification_status` = `certified`")
print("✓ fact_order_items is now marked certified, and Catalog Explorer shows it")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lineage: the metadata you don't write
# MAGIC
# MAGIC > **Beyond the exam.** The October guide's governance section is tags, comments and
# MAGIC > inheritance. Lineage isn't named. It stays for two minutes because you'll use it at work.
# MAGIC
# MAGIC Unity Catalog records lineage from query execution. In Catalog Explorer, the Lineage tab draws
# MAGIC the graph. Programmatically:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table_full_name, target_table_full_name,
# MAGIC        entity_type, max(event_time) AS last_seen
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE 'lakeshore.%'
# MAGIC   AND source_table_full_name IS NOT NULL
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY last_seen DESC
# MAGIC LIMIT 15;

# COMMAND ----------

tags = spark.sql("""SELECT count(*) FROM lakeshore.information_schema.table_tags""").first()[0]
col_tags = spark.sql("""SELECT count(*) FROM lakeshore.information_schema.column_tags""").first()[0]
assert tags >= 3 and col_tags >= 1, (tags, col_tags)
print(f"✓ {tags} table tags and {col_tags} column tags on lakeshore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Comments make objects FINDABLE, on any securable, and who may write one varies. Tags make
# MAGIC governance QUERYABLE, inherit except to columns, and shouldn't carry secrets. Governed tags
# MAGIC control the vocabulary, and only they drive ABAC. Next: sharing the governed lakehouse
# MAGIC beyond the workspace.

# COMMAND ----------

dbutils.notebook.exit("metadata-complete")
