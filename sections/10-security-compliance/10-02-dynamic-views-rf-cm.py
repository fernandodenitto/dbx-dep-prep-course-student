# Databricks notebook source
# MAGIC %md
# MAGIC # 10.2 — Dynamic views vs row filters & column masks
# MAGIC
# MAGIC Two ways to make a table show different truths to different people:
# MAGIC wrap it in a **dynamic view**, or attach **row filters / column masks**
# MAGIC to the table itself. We build both on a demo table and settle the fork for one
# MAGIC table at a time. Doing it for a hundred tables is a third mechanism, ABAC, and it
# MAGIC gets its own lesson (10.8).
# MAGIC
# MAGIC > Reads `silver.customers` and `silver.addresses`, both built by the fast-forward
# MAGIC > (1.4). Demo objects (`silver.sec_customers` + two governance functions) are
# MAGIC > dropped at the end.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demo table: customers + their shipping state (never touch canon)
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.sec_customers AS
# MAGIC SELECT c.customer_id, c.first_name, c.last_name, c.email,
# MAGIC        a.shipping_state
# MAGIC FROM lakeshore.silver.customers c
# MAGIC LEFT JOIN lakeshore.silver.addresses a USING (customer_id);

# COMMAND ----------

# The filter below hides Colorado. Prove there is something to hide, or the test is empty.
co_before = spark.sql("""SELECT count(*) FROM lakeshore.silver.sec_customers
                         WHERE shipping_state = 'CO'""").first()[0]
print(f"Colorado customers before any policy: {co_before}")
assert co_before > 0, "no CO rows: the row-filter check below would prove nothing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The dynamic view — logic in a separate object
# MAGIC
# MAGIC `is_account_group_member()` inside a view. We test BOTH branches live:
# MAGIC everyone is in `account users` (true branch), nobody is in the
# MAGIC imaginary `marketing` group (false branch).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lakeshore.silver.vw_sec_customers AS
# MAGIC SELECT customer_id, first_name, shipping_state,
# MAGIC        CASE WHEN is_account_group_member('marketing')
# MAGIC             THEN email ELSE regexp_replace(email, '^[^@]+', '****')
# MAGIC        END AS email
# MAGIC FROM lakeshore.silver.sec_customers;
# MAGIC
# MAGIC SELECT * FROM lakeshore.silver.vw_sec_customers LIMIT 5;

# COMMAND ----------

masked = spark.sql("""SELECT count(*) FROM lakeshore.silver.vw_sec_customers
                      WHERE email NOT LIKE '****@%'""").first()[0]
assert masked == 0, "we are not in 'marketing' — every email must be masked"
print("✓ dynamic view masks for non-members (that's us)")

# COMMAND ----------

# MAGIC %md
# MAGIC The view is the product: grant SELECT on the VIEW, never on the base table. The
# MAGIC docs give dynamic views this job: exposing a curated, transformed or joined version
# MAGIC of your data to users who don't have access to the underlying tables.
# MAGIC
# MAGIC ## 2. Row filter + column mask — policy ON the table
# MAGIC
# MAGIC Same goals, no second object: SQL UDFs attached to the table itself.
# MAGIC Every path to the table — SQL, Python, BI tools — passes through them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The policy functions: plain SQL UDFs with a governance job
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.f_state_filter(state STRING)
# MAGIC RETURN is_account_group_member('marketing') OR state IS NULL OR state <> 'CO';
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.f_email_mask(email STRING)
# MAGIC RETURN CASE WHEN is_account_group_member('marketing') THEN email
# MAGIC             ELSE concat('****', substr(email, instr(email, '@'))) END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.sec_customers
# MAGIC   SET ROW FILTER lakeshore.silver.f_state_filter ON (shipping_state);
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.sec_customers
# MAGIC   ALTER COLUMN email SET MASK lakeshore.silver.f_email_mask;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the TABLE directly: Colorado rows gone, emails masked
# MAGIC SELECT customer_id, email, shipping_state
# MAGIC FROM lakeshore.silver.sec_customers LIMIT 5;

# COMMAND ----------

co_rows = spark.sql("""SELECT count(*) FROM lakeshore.silver.sec_customers
                       WHERE shipping_state = 'CO'""").first()[0]
raw_emails = spark.sql("""SELECT count(*) FROM lakeshore.silver.sec_customers
                          WHERE email NOT LIKE '****@%'""").first()[0]
print(f"CO rows visible: {co_rows} · unmasked emails: {raw_emails}")
assert co_rows == 0 and raw_emails == 0

# COMMAND ----------

# MAGIC %md
# MAGIC No view in sight: the TABLE now enforces both policies for everyone who isn't in
# MAGIC `marketing`, on every access path. That's the operational difference: one object to
# MAGIC govern, no you-forgot-to-use-the-view risk.
# MAGIC
# MAGIC ## 3. The fork, with its third column
# MAGIC
# MAGIC | | Dynamic view | Table-level row filter / mask | ABAC policy (lesson 10.8) |
# MAGIC |---|---|---|---|
# MAGIC | Where the logic lives | separate VIEW object | ON one table, per table | on a catalog or schema, selected by governed tag |
# MAGIC | Who manages it | view owner | table owner or MANAGE | MANAGE on the scope: table owners can't remove it |
# MAGIC | Table 41 arrives | write another view | another `ALTER TABLE` | covered once its column is tagged |
# MAGIC | Reshaping/joins | ✅ any SELECT | ❌ filter/mask only | ❌ filter/mask only |
# MAGIC | Bypass risk | the base table, if misgranted | none: every path filtered | none: every path filtered |
# MAGIC | OpenSharing | D2D recipients can read views | table-level policies: **can't be shared** | can be shared if the share owner is exempt; the policy doesn't govern the recipient |
# MAGIC | Time travel | normal | not supported | Beta, for qualifying tables and compute |
# MAGIC | Clones | normal | not supported | only for principals in EXCEPT |
# MAGIC
# MAGIC The docs' rule of thumb: a curated, joined or reshaped exposure is a dynamic view;
# MAGIC table-specific logic is a table-level filter or mask; the same rule across many
# MAGIC tables is ABAC, which is what they recommend at scale.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cleanup (demo objects only)
# MAGIC DROP VIEW IF EXISTS lakeshore.silver.vw_sec_customers;
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.sec_customers;
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.f_state_filter;
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.f_email_mask;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Two mechanisms, one intent. Views curate and reshape; table policies govern in
# MAGIC place with no bypass. Both ride `is_account_group_member`. Sharing, time travel and
# MAGIC clones are where table-level policies cost you. Next: what to do when the requirement
# MAGIC isn't hiding data but de-identifying it.

# COMMAND ----------

dbutils.notebook.exit("dynamic-views-rf-cm-complete")
