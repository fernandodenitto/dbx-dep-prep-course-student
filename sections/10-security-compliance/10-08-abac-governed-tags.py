# Databricks notebook source
# MAGIC %md
# MAGIC # 10.8 — ABAC: one policy, attached at a scope, selected by governed tags
# MAGIC
# MAGIC In 10.2 you attached a mask to one column of one table, and the guide's statement is about
# MAGIC what happens next: "Apply attribute-based access control (ABAC) policies with governed
# MAGIC tags to enforce row filters and column masks at scale."
# MAGIC
# MAGIC The Professional questions aren't "what is ABAC". They are: where do you attach it, what
# MAGIC does a schema tag do versus a column tag, who is exempt, and what breaks. So we break it.
# MAGIC
# MAGIC Demo objects (dropped at the end): `silver.abac_customers`, `gold.abac_contacts`,
# MAGIC `bronze.abac_leads`, four functions and every policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. The governed tag: an account-level object
# MAGIC
# MAGIC Governed tags aren't created with SQL. An account admin defines the key and its allowed
# MAGIC values. From your terminal it looks like this:
# MAGIC
# MAGIC ```
# MAGIC databricks tag-policies create-tag-policy --json '{
# MAGIC   "tag_key": "lakeshore_class",
# MAGIC   "description": "Lakeshore data classes for access policies",
# MAGIC   "values": [{"name": "pii_email"}, {"name": "pii_name"},
# MAGIC              {"name": "geo_state"}, {"name": "regional"}]
# MAGIC }'
# MAGIC ```
# MAGIC
# MAGIC This cell does the same through the API if the tag is missing, so the notebook can be
# MAGIC re-run. It also removes anything an aborted run left behind. `DROP POLICY` has no `IF
# MAGIC EXISTS`, so you look first.

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

TAG = "lakeshore_class"
api = WorkspaceClient().api_client
try:
    api.do("GET", f"/api/2.1/tag-policies/{TAG}")
    print("governed tag already exists:", TAG)
except NotFound:
    api.do("POST", "/api/2.1/tag-policies", body={
        "tag_key": TAG,
        "description": "Lakeshore data classes for access policies",
        "values": [{"name": "pii_email"}, {"name": "pii_name"},
                   {"name": "geo_state"}, {"name": "regional"}]})
    print("governed tag created:", TAG)

POLICIES = [("mask_email", "CATALOG lakeshore"),
            ("mask_email_rival", "CATALOG lakeshore"),
            ("filter_regional_silver", "SCHEMA lakeshore.silver")]

def drop_policies():
    """DROP POLICY has no IF EXISTS, so check SHOW POLICIES first."""
    for name, scope in POLICIES:
        existing = [r["Policy Name"] for r in spark.sql(f"SHOW POLICIES ON {scope}").collect()]
        if name in existing:
            spark.sql(f"DROP POLICY {name} ON {scope}")
            print("dropped", name)

drop_policies()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tables in two schemas, and the functions
# MAGIC
# MAGIC The functions are ordinary Unity Catalog functions, the same shape as 10.2. ABAC doesn't
# MAGIC invent a new masking mechanism. It changes how the function gets *selected*. Notice the
# MAGIC filter takes a second argument: one function, reusable by policies with different
# MAGIC constants.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.abac_customers AS
# MAGIC SELECT customer_id, first_name, email, a.shipping_state
# MAGIC FROM lakeshore.silver.customers c
# MAGIC LEFT JOIN lakeshore.silver.addresses a USING (customer_id)
# MAGIC WHERE email IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.abac_contacts AS
# MAGIC SELECT customer_id, email, a.shipping_state AS ship_state
# MAGIC FROM lakeshore.silver.customers c
# MAGIC LEFT JOIN lakeshore.silver.addresses a USING (customer_id)
# MAGIC WHERE email IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.f_keep_domain(v STRING) RETURNS STRING
# MAGIC COMMENT 'Hides the local part of an email'
# MAGIC RETURN concat('****', substr(v, instr(v, '@')));
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.f_redact(v STRING) RETURNS STRING
# MAGIC COMMENT 'Hides the whole value'
# MAGIC RETURN '****';
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.f_hide_state(state STRING, hidden STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Keeps rows outside the hidden state'
# MAGIC RETURN state IS NULL OR state <> hidden;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Governed means a controlled vocabulary

# COMMAND ----------

def must_fail(statement, error):
    """Run a statement that has to be rejected, and check it's rejected for that reason."""
    try:
        spark.sql(statement).collect()
    except Exception as e:
        assert error in str(e), str(e)[:400]
        print(f"✓ rejected: {error}")
        return
    raise AssertionError(f"expected {error}: {statement}")

must_fail(f"SET TAG ON COLUMN lakeshore.silver.abac_customers.email `{TAG}` = `secret_stuff`",
          "UC_TAG_POLICY_VALUE_NOT_ALLOWED")
must_fail("""CREATE POLICY p ON SCHEMA lakeshore.silver COLUMN MASK lakeshore.silver.f_redact
             TO `account users` FOR TABLES MATCH COLUMNS has_tag('some_loose_tag') AS c
             ON COLUMN c""",
          "UC_INVALID_POLICY_CONDITION")

# COMMAND ----------

# MAGIC %md
# MAGIC The value has to be on the allowed list, and a policy can't key on an ordinary tag: it
# MAGIC fails with "Unknown tag policy key". You also need ASSIGN on the governed tag, plus APPLY
# MAGIC TAG on the object, to put it on a column. That's the separation of duties ABAC is built
# MAGIC for: an admin defines the vocabulary, a policy author writes the rule, a data steward
# MAGIC classifies columns.
# MAGIC
# MAGIC ## 3. One policy at the CATALOG

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON COLUMN lakeshore.silver.abac_customers.email `lakeshore_class` = `pii_email`;
# MAGIC SET TAG ON COLUMN lakeshore.gold.abac_contacts.email    `lakeshore_class` = `pii_email`;
# MAGIC
# MAGIC CREATE OR REPLACE POLICY mask_email
# MAGIC   ON CATALOG lakeshore
# MAGIC   COMMENT 'Hide the local part of anything classified pii_email'
# MAGIC   COLUMN MASK lakeshore.silver.f_keep_domain
# MAGIC   TO `account users`
# MAGIC   FOR TABLES
# MAGIC   MATCH COLUMNS has_tag_value('lakeshore_class', 'pii_email') AS e
# MAGIC   ON COLUMN e;

# COMMAND ----------

def unmasked(table, column="email"):
    return spark.sql(f"SELECT count(*) FROM {table} WHERE {column} NOT LIKE '****@%'").first()[0]

def wait_until_masked(table, column="email", seconds=180):
    """The docs warn a tag change can take a few minutes to take effect."""
    deadline = time.time() + seconds
    while unmasked(table, column) and time.time() < deadline:
        time.sleep(15)
    assert unmasked(table, column) == 0, f"{table}.{column} still has clear values"

wait_until_masked("lakeshore.silver.abac_customers")
wait_until_masked("lakeshore.gold.abac_contacts")
print("✓ one policy, two schemas, and neither table is named in it")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, email FROM lakeshore.gold.abac_contacts ORDER BY customer_id LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC The docs recommend the highest applicable level, usually the catalog. Now the payoff.
# MAGIC **A table that doesn't exist yet**, in a third schema. Nobody edits the policy.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP rather than REPLACE: a replaced table keeps an earlier run's column tags.
# MAGIC DROP TABLE IF EXISTS lakeshore.bronze.abac_leads;
# MAGIC CREATE TABLE lakeshore.bronze.abac_leads (lead_id INT, contact STRING);
# MAGIC INSERT INTO lakeshore.bronze.abac_leads VALUES (1, 'ada@north.example'), (2, 'bob@north.example');

# COMMAND ----------

assert unmasked("lakeshore.bronze.abac_leads", "contact") == 2
print("Before classification: in the clear. The policy covers tables in scope, but nothing "
      "says this column holds an email.")
spark.sql("SET TAG ON COLUMN lakeshore.bronze.abac_leads.contact `lakeshore_class` = `pii_email`")
wait_until_masked("lakeshore.bronze.abac_leads", "contact")
print("✓ after classification: masked, and the policy was never touched")

# COMMAND ----------

# MAGIC %md
# MAGIC Say the honest half too: **ABAC is only as good as your tagging.** Between `CREATE TABLE`
# MAGIC and `SET TAG`, that column was readable. Automated classification (lesson 10.4) closes that
# MAGIC gap in paid workspaces, usually within 24 hours, and a policy can match its tags directly
# MAGIC with `has_tag('class.email_address')`.
# MAGIC
# MAGIC ## 4. Scope: a row filter on one schema, and table-level conditions
# MAGIC
# MAGIC Attach a row filter to `silver` only. Two conditions decide what it hits:
# MAGIC
# MAGIC - `WHEN` tests **table** tags, including tags inherited from the schema or catalog.
# MAGIC - `MATCH COLUMNS` tests **column** tags. Those are never inherited.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON COLUMN lakeshore.silver.abac_customers.shipping_state `lakeshore_class` = `geo_state`;
# MAGIC SET TAG ON COLUMN lakeshore.gold.abac_contacts.ship_state        `lakeshore_class` = `geo_state`;
# MAGIC SET TAG ON TABLE lakeshore.silver.abac_customers `lakeshore_class` = `regional`;
# MAGIC SET TAG ON TABLE lakeshore.gold.abac_contacts    `lakeshore_class` = `regional`;
# MAGIC
# MAGIC CREATE OR REPLACE POLICY filter_regional_silver
# MAGIC   ON SCHEMA lakeshore.silver
# MAGIC   COMMENT 'Hide Colorado from regional silver tables'
# MAGIC   ROW FILTER lakeshore.silver.f_hide_state
# MAGIC   TO `account users`
# MAGIC   FOR TABLES
# MAGIC   WHEN has_tag_value('lakeshore_class', 'regional')
# MAGIC   MATCH COLUMNS has_tag_value('lakeshore_class', 'geo_state') AS st
# MAGIC   USING COLUMNS (st, 'CO');

# COMMAND ----------

def co_rows(table, column):
    return spark.sql(f"SELECT count(*) FROM {table} WHERE {column} = 'CO'").first()[0]

deadline = time.time() + 180
while co_rows("lakeshore.silver.abac_customers", "shipping_state") and time.time() < deadline:
    time.sleep(15)
silver_co = co_rows("lakeshore.silver.abac_customers", "shipping_state")
gold_co = co_rows("lakeshore.gold.abac_contacts", "ship_state")
print(f"Colorado rows: silver {silver_co}, gold {gold_co}")
assert silver_co == 0 and gold_co > 0, (silver_co, gold_co)
print("✓ same tags on both tables. The policy's scope is what separates them")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reading what applies: three different questions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attached HERE: nothing, the table has no policy of its own
# MAGIC SHOW POLICIES ON TABLE lakeshore.silver.abac_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What actually applies to the table, including parent scopes. This is your audit answer.
# MAGIC SHOW EFFECTIVE POLICIES ON TABLE lakeshore.silver.abac_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The inventory, as a query
# MAGIC SELECT policy_name, policy_type, on_securable_type, catalog_name, schema_name,
# MAGIC        to_principals, except_principals, when_condition, match_columns
# MAGIC FROM lakeshore.information_schema.abac_policy_definitions;

# COMMAND ----------

effective = {r["Policy Name"] for r in spark.sql(
    "SHOW EFFECTIVE POLICIES ON TABLE lakeshore.silver.abac_customers").collect()}
assert {"mask_email", "filter_regional_silver"} <= effective, effective
own = spark.sql("SHOW POLICIES ON TABLE lakeshore.silver.abac_customers").count()
assert own == 0, own
print("✓ SHOW POLICIES says what is attached, SHOW EFFECTIVE POLICIES says what applies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. TO and EXCEPT, and the rule that bites pipelines
# MAGIC
# MAGIC `CREATE OR REPLACE POLICY` replaces the entire definition, so you restate every clause.
# MAGIC Exempting yourself shows what EXCEPT does. A group named in EXCEPT has to exist:
# MAGIC otherwise you get `PRINCIPAL_DOES_NOT_EXIST`.

# COMMAND ----------

me = spark.sql("SELECT current_user()").first()[0]
def mask_policy(except_clause=""):
    return f"""CREATE OR REPLACE POLICY mask_email ON CATALOG lakeshore
               COMMENT 'Hide the local part of anything classified pii_email'
               COLUMN MASK lakeshore.silver.f_keep_domain
               TO `account users` {except_clause}
               FOR TABLES
               MATCH COLUMNS has_tag_value('lakeshore_class', 'pii_email') AS e
               ON COLUMN e"""

spark.sql(mask_policy(f"EXCEPT `{me}`"))
print("exempt, clear values visible:", unmasked("lakeshore.gold.abac_contacts") > 0)
assert unmasked("lakeshore.gold.abac_contacts") > 0

spark.sql(mask_policy())
wait_until_masked("lakeshore.gold.abac_contacts")
print("✓ back in the policy")

# COMMAND ----------

# MAGIC %md
# MAGIC Now the rule everyone learns the hard way. Whoever **writes** a derived table reads
# MAGIC through the policy like anyone else, and what they read is what gets stored.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.abac_baked AS
# MAGIC SELECT customer_id, email FROM lakeshore.gold.abac_contacts;

# COMMAND ----------

baked = spark.sql("""SELECT count(*) FROM lakeshore.gold.abac_baked
                     WHERE email NOT LIKE '****@%'""").first()[0]
assert baked == 0, baked
print("✓ the copy stored masked values. They're now the data, with no policy to lift")

# COMMAND ----------

# MAGIC %md
# MAGIC The same thing happens to pipelines, and the docs name the fix. A materialized view or
# MAGIC streaming table is refreshed as the pipeline owner or run-as identity. If that identity is
# MAGIC subject to a policy, the table **permanently contains masked or filtered data**. So add the
# MAGIC run-as identity to `EXCEPT` on the policies for every table the pipeline reads, and use
# MAGIC `TO` for the people who get masked data. Exempt only trusted identities: an exempt
# MAGIC principal sees everything.
# MAGIC
# MAGIC ## 7. Conflicts fail closed, and they take the whole table with them

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY mask_email_rival
# MAGIC   ON CATALOG lakeshore
# MAGIC   COLUMN MASK lakeshore.silver.f_redact
# MAGIC   TO `account users`
# MAGIC   FOR TABLES
# MAGIC   MATCH COLUMNS has_tag_value('lakeshore_class', 'pii_email') AS e
# MAGIC   ON COLUMN e;

# COMMAND ----------

deadline = time.time() + 180
while time.time() < deadline:
    try:
        spark.sql("SELECT customer_id FROM lakeshore.gold.abac_contacts LIMIT 1").collect()
        time.sleep(15)
    except Exception as e:
        assert "MULTIPLE_MASKS" in str(e), str(e)[:400]
        print("✓ even a query that never touches email fails: COLUMN_MASKS_FEATURE_NOT_SUPPORTED.MULTIPLE_MASKS")
        break
else:
    raise AssertionError("expected the conflict to block the table")

spark.sql("DROP POLICY mask_email_rival ON CATALOG lakeshore")

# COMMAND ----------

# MAGIC %md
# MAGIC Only one distinct mask can resolve per column per user, and one distinct filter per table
# MAGIC per user. Two different ones, and Databricks blocks access rather than guessing which one
# MAGIC you meant. Notice what that means operationally: a colleague's new policy at the catalog
# MAGIC can take down every query on a table in your schema, including ones that never read that
# MAGIC column. Two policies are fine if they resolve to the same function with the same
# MAGIC arguments.
# MAGIC
# MAGIC ## 8. What ABAC costs you

# COMMAND ----------

wait_until_masked("lakeshore.gold.abac_contacts")
must_fail("""CREATE OR REPLACE TABLE lakeshore.gold.abac_clone
             DEEP CLONE lakeshore.gold.abac_contacts""",
          "TABLE_CLONE_SOURCE_NOT_SUPPORTED")
must_fail("SELECT * FROM lakeshore.gold.abac_contacts VERSION AS OF 0",
          "TIME_TRAVEL")

# COMMAND ----------

# MAGIC %md
# MAGIC | | What the docs say |
# MAGIC |---|---|
# MAGIC | Clones | not on tables with ABAC policies, unless you're in EXCEPT of every policy on them (above: rejected) |
# MAGIC | Time travel | Beta for qualifying tables and compute (above: rejected on this compute) |
# MAGIC | OpenSharing | shareable only if the share owner is exempt, and then the policy doesn't govern the recipient |
# MAGIC | Compute | serverless, or DBR 16.4+ on standard and dedicated. Older runtimes can't read these tables |
# MAGIC | AI Search indexes | an index built from the table doesn't enforce the policy |
# MAGIC | Quotas | 100 policies per catalog or schema, 50 per table, 20 principals per TO and EXCEPT |
# MAGIC
# MAGIC ## 9. Cleanup

# COMMAND ----------

drop_policies()
for t in ["silver.abac_customers", "gold.abac_contacts", "bronze.abac_leads", "gold.abac_baked"]:
    spark.sql(f"DROP TABLE IF EXISTS lakeshore.{t}")
for f in ["f_keep_domain", "f_redact", "f_hide_state"]:
    spark.sql(f"DROP FUNCTION IF EXISTS lakeshore.silver.{f}")

left = spark.sql("SHOW POLICIES ON CATALOG lakeshore").filter("`Policy Name` LIKE 'mask_email%'").count()
left += spark.sql("SHOW POLICIES ON SCHEMA lakeshore.silver").count()
assert left == 0, f"{left} policies survived cleanup"
print("✓ no policies left. An orphaned catalog-level policy isn't a tidiness issue: it would "
      "keep masking")

# COMMAND ----------

# MAGIC %md
# MAGIC The governed tag stays: it's an account-level object created outside this lesson. To
# MAGIC remove it: `databricks tag-policies delete-tag-policy lakeshore_class`.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Governed tags are an account-level vocabulary · a policy attaches at a scope and selects by
# MAGIC tag, so new tables are covered once classified · `WHEN` reads table tags with inheritance,
# MAGIC `MATCH COLUMNS` reads column tags with none · `SHOW EFFECTIVE POLICIES` answers what applies
# MAGIC · add run-as identities to EXCEPT, or stored data stays masked · conflicting masks block the
# MAGIC whole table.

# COMMAND ----------

dbutils.notebook.exit("abac-complete")
