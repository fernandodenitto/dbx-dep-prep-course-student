# Databricks notebook source
# MAGIC %md
# MAGIC # 10.1 — UC privileges, inheritance & workspace ACLs
# MAGIC
# MAGIC Security starts with who can do what. Unity Catalog's model in one breath:
# MAGIC **privileges on securables, inherited down the tree, including objects that
# MAGIC don't exist yet. Ownership is the one thing that does not inherit.**
# MAGIC
# MAGIC > **↩️ Associate recap:** the grants trio (USE CATALOG + USE SCHEMA + SELECT) and
# MAGIC > the idea of inheritance were Associate ground. Professional depth: reading where
# MAGIC > a grant comes from, future objects, catalog-level grants, what doesn't inherit,
# MAGIC > the full ladder, and workspace-object ACLs per resource.
# MAGIC
# MAGIC Demo tables: `gold.grants_demo_before`, `gold.grants_demo_after`,
# MAGIC `silver.grants_demo_elsewhere`. All dropped at the end.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the current state: always the first move

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG lakeshore;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS me,
# MAGIC        is_account_group_member('account users') AS in_account_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Three keys, granted at the schema
# MAGIC
# MAGIC To SELECT one table, a principal needs three privileges: USE CATALOG, USE SCHEMA,
# MAGIC SELECT. We create one table **before** the grant, grant on the schema, then create
# MAGIC a second table **after** it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.grants_demo_before (id INT);
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG lakeshore TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakeshore.gold TO `account users`;
# MAGIC GRANT SELECT ON SCHEMA lakeshore.gold TO `account users`;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.grants_demo_after (id INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nobody granted anything on this table. Read the last two columns.
# MAGIC SHOW GRANTS ON TABLE lakeshore.gold.grants_demo_before;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This table did not exist when we granted.
# MAGIC SHOW GRANTS ON TABLE lakeshore.gold.grants_demo_after;

# COMMAND ----------

def grants_on(table):
    """SHOW GRANTS as (principal, privilege, object type, object key) tuples."""
    return [tuple(r) for r in spark.sql(f"SHOW GRANTS ON TABLE {table}").collect()]

expected = ("account users", "SELECT", "SCHEMA", "lakeshore.gold")
for t in ["lakeshore.gold.grants_demo_before", "lakeshore.gold.grants_demo_after"]:
    assert expected in grants_on(t), grants_on(t)
print("✓ SHOW GRANTS on the table lists the inherited row, and names the schema it came from")

# COMMAND ----------

# MAGIC %md
# MAGIC Two facts, both visible in that output:
# MAGIC
# MAGIC - **`SHOW GRANTS` on a table shows what it inherits.** `ObjectType` and `ObjectKey`
# MAGIC   tell you where the grant actually lives. If you read somewhere that `SHOW GRANTS`
# MAGIC   only lists direct grants, you just disproved it.
# MAGIC - **The grant covered a table created afterwards.** Privileges on a container apply
# MAGIC   to all current and future children.
# MAGIC
# MAGIC The same answer as a query you can filter and schedule:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT grantee, privilege_type, table_name, inherited_from
# MAGIC FROM lakeshore.information_schema.table_privileges
# MAGIC WHERE table_name LIKE 'grants_demo%'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. One level up: a grant on the catalog
# MAGIC
# MAGIC Grant SELECT on the catalog, then create a table in a **different** schema. No
# MAGIC schema-level or table-level SELECT exists for it.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON CATALOG lakeshore TO `account users`;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.grants_demo_elsewhere (id INT);
# MAGIC
# MAGIC SHOW GRANTS ON TABLE lakeshore.silver.grants_demo_elsewhere;

# COMMAND ----------

expected = ("account users", "SELECT", "CATALOG", "lakeshore")
assert expected in grants_on("lakeshore.silver.grants_demo_elsewhere")
print("✓ the catalog grant reaches a table created later, in another schema")

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT alone still isn't enough to read it: that principal also needs USE SCHEMA on
# MAGIC `silver`. Inheritance hands down the privilege, and the usage keys stay a separate
# MAGIC gate. To make a whole catalog readable in one statement, grant all three at the top:
# MAGIC `GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG lakeshore TO ...`.
# MAGIC
# MAGIC ## 4. You can't carve out with REVOKE, and there is no DENY for SELECT

# COMMAND ----------

# REVOKE on a table that only inherits: it runs, and it changes nothing.
spark.sql("REVOKE SELECT ON TABLE lakeshore.gold.grants_demo_before FROM `account users`")
still = ("account users", "SELECT", "SCHEMA", "lakeshore.gold")
assert still in grants_on("lakeshore.gold.grants_demo_before")
print("✓ REVOKE succeeded and the inherited row is still there")

# The DENY statement belongs to the legacy hive_metastore.
try:
    spark.sql("DENY SELECT ON TABLE lakeshore.gold.grants_demo_before TO `account users`")
    raise AssertionError("DENY should not be accepted in Unity Catalog")
except Exception as e:
    assert "UC_COMMAND_NOT_SUPPORTED" in str(e), e
    print("✓ DENY rejected: UC_COMMAND_NOT_SUPPORTED")

# COMMAND ----------

# MAGIC %md
# MAGIC So a carve-out is a design decision: a separate schema, or table-by-table grants.
# MAGIC Schemas are your permission boundaries.
# MAGIC
# MAGIC The precise wording, because "no DENY exists" used to be safe and now isn't quite:
# MAGIC there is no DENY for data privileges in Unity Catalog. **ABAC DENY policies** are in
# MAGIC Beta and cover one privilege only, MANAGE ACCESS CONTROL. They don't touch SELECT or
# MAGIC MODIFY, and `SHOW GRANTS` doesn't reflect them (`SHOW EFFECTIVE POLICIES` does).
# MAGIC Lesson 10.8 is where ABAC lives.
# MAGIC
# MAGIC ## 5. What does not inherit

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_schema, table_name, table_owner
# MAGIC FROM lakeshore.information_schema.tables
# MAGIC WHERE table_name LIKE 'grants_demo%';

# COMMAND ----------

me = spark.sql("SELECT current_user()").first()[0]
assert not [g for g in grants_on("lakeshore.gold.grants_demo_after") if g[0] == me]
print(f"✓ {me} owns this table, and SHOW GRANTS does not list the owner")

# COMMAND ----------

# MAGIC %md
# MAGIC | Thing | Inherits downward? | The detail the exam likes |
# MAGIC |---|---|---|
# MAGIC | SELECT, MODIFY, CREATE TABLE, USE SCHEMA... on a catalog or schema | ✅ | current and **future** children |
# MAGIC | MANAGE on a container | ✅ | MANAGE on a catalog needs no usage privileges at all |
# MAGIC | **Ownership** | ❌ | the parent's owner doesn't own the child, but can manage it |
# MAGIC | **Metastore-level grants** | ❌ | CREATE CATALOG and friends stay at the metastore. READ METADATA is the exception |
# MAGIC
# MAGIC Owners hold all capabilities on their object without any grant row, which is why
# MAGIC you were not in that output. An audit that only reads grants misses owners.
# MAGIC
# MAGIC ## 6. The ladder above SELECT

# COMMAND ----------

# MAGIC %md
# MAGIC | Privilege | Lets you | Note |
# MAGIC |---|---|---|
# MAGIC | BROWSE | see that objects exist, read metadata in Catalog Explorer | granted at catalog level, no USE needed |
# MAGIC | READ METADATA | read-only metadata visibility | a child of MANAGE |
# MAGIC | SELECT / MODIFY | read / write data | INSERT, UPDATE, DELETE are Beta children of MODIFY |
# MAGIC | CREATE TABLE / SCHEMA... | create children | on containers |
# MAGIC | ALL PRIVILEGES | everything applicable | **excludes** MANAGE, READ METADATA, EXTERNAL USE |
# MAGIC | MANAGE | grant, revoke, transfer, drop | doesn't include SELECT, but you can grant it to yourself |
# MAGIC | **Ownership** | all capabilities on the object | one principal only, transferable (`ALTER ... OWNER TO`) |
# MAGIC
# MAGIC Least-privilege casting: humans get SELECT via groups on gold; pipelines run as
# MAGIC service principals with MODIFY where they write; MANAGE and ownership stay with the
# MAGIC platform team. Grant to groups, never to people.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Leave the workspace as we found it
# MAGIC REVOKE SELECT ON CATALOG lakeshore FROM `account users`;
# MAGIC REVOKE SELECT ON SCHEMA lakeshore.gold FROM `account users`;
# MAGIC REVOKE USE SCHEMA ON SCHEMA lakeshore.gold FROM `account users`;
# MAGIC REVOKE USE CATALOG ON CATALOG lakeshore FROM `account users`;
# MAGIC DROP TABLE IF EXISTS lakeshore.gold.grants_demo_before;
# MAGIC DROP TABLE IF EXISTS lakeshore.gold.grants_demo_after;
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.grants_demo_elsewhere;

# COMMAND ----------

grants = spark.sql("SHOW GRANTS ON CATALOG lakeshore").collect()
leftover = [g for g in grants if g["Principal"] == "account users"]
assert not leftover, f"revoke incomplete: {leftover}"
print("✓ grants demo cleaned: catalog back to baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Workspace-object ACLs: the OTHER permission system
# MAGIC
# MAGIC Unity Catalog governs data. Notebooks, jobs, pipelines and warehouses have their own
# MAGIC ACLs, and the guide names them: "Unity Catalog securable objects and workspace
# MAGIC resources". The levels differ per resource, so a flat list won't answer a scenario:
# MAGIC
# MAGIC | Resource | Levels | "Can run it, can't change it" |
# MAGIC |---|---|---|
# MAGIC | Job | CAN VIEW · CAN MANAGE RUN · IS OWNER · CAN MANAGE | **CAN MANAGE RUN** (run, cancel, override parameters) |
# MAGIC | Lakeflow pipeline | CAN VIEW · CAN RUN · CAN MANAGE · IS OWNER | **CAN RUN** (start and stop updates) |
# MAGIC | SQL warehouse | CAN VIEW · CAN MONITOR · CAN USE · IS OWNER · CAN MANAGE | **CAN USE** (start it, run queries) |
# MAGIC | Notebook | CAN VIEW · CAN RUN · CAN EDIT · CAN MANAGE | **CAN RUN** |
# MAGIC | Folder | CAN VIEW · CAN RUN · CAN EDIT · CAN MANAGE | objects inherit the folder's settings |
# MAGIC | Secret scope | READ · WRITE · MANAGE | lesson 10.6 |
# MAGIC
# MAGIC The trap: a user who can't SELECT a table might still CAN VIEW a notebook whose saved
# MAGIC results display it. Results in a notebook are workspace content, not governed data.
# MAGIC Least privilege walks both trees. This workspace has one user, so this part is
# MAGIC described rather than demoed: open the Permissions dialog on any job to see the levels.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Three keys to a table · `SHOW GRANTS` and `inherited_from` show inherited rows and
# MAGIC their source · grants reach future objects · ownership and metastore grants don't
# MAGIC inherit · no DENY for data privileges, so schemas are boundaries · workspace ACLs are
# MAGIC a second, parallel system. Next: making one table show different truths to different
# MAGIC people.

# COMMAND ----------

dbutils.notebook.exit("uc-privileges-complete")
