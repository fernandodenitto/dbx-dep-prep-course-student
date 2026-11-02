# Databricks notebook source
# MAGIC %md
# MAGIC # 10.6 — Secrets
# MAGIC
# MAGIC > **Beyond the exam.** The October guide doesn't name secrets. This lesson stays, short,
# MAGIC > because lesson 10.4 left a debt, and because every real platform needs it.
# MAGIC
# MAGIC Lessons 10.3 and 10.4 hard-coded a salt in the notebook. A salt in source code is a
# MAGIC credential in a git repo. Today it moves into a **secret scope**.
# MAGIC
# MAGIC > Setup, from your LOCAL terminal (CLI installed and logged in at lesson 1.4). The second
# MAGIC > command prompts for the value: type `lakeshore-salt-2026` to keep the same pseudonyms as
# MAGIC > lesson 10.4. Type anything else and you've just rotated the salt, which is a lesson too:
# MAGIC > the assert at the end of this notebook will say so.
# MAGIC > ```
# MAGIC > databricks secrets create-scope lakeshore-secrets
# MAGIC > databricks secrets put-secret lakeshore-secrets pii_salt
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read a secret — and fail to print it

# COMMAND ----------

salt = dbutils.secrets.get(scope="lakeshore-secrets", key="pii_salt")
print("the secret:", salt)
print("its length:", len(salt))

# COMMAND ----------

# MAGIC %md
# MAGIC `[REDACTED]`. The notebook output NEVER shows secret values — that's
# MAGIC redaction, and it protects you from your own print statements, saved
# MAGIC revisions, and screen shares. The VALUE is in the variable (the length
# MAGIC printed fine); the DISPLAY is redacted. It's a guardrail against accidents, not
# MAGIC a control against intent: the docs warn that a determined user with read access can
# MAGIC still get the value out.

# COMMAND ----------

for s in dbutils.secrets.list(scope="lakeshore-secrets"):
    print(s.key)
assert salt and len(salt) > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Close the loop: the salt leaves our source code

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, sha2

masked = (spark.table("lakeshore.silver.customers")
          .select("customer_id",
                  sha2(concat("email", lit(salt)), 256).alias("email_hash"))
          .limit(3))
masked.show(truncate=40)

# COMMAND ----------

# Did you type the same salt? The bus marks updated emails with a 'new.' prefix (lesson 10.4).
candidates = (spark.table("lakeshore.silver.customers")
              .select(sha2(concat(lit("new."), col("email"), lit(salt)), 256).alias("email_hash")))
same = candidates.join(spark.table("lakeshore.silver.bus_customers_masked"), "email_hash").count()
if same:
    print(f"✓ same salt: {same} pseudonyms still line up with lesson 10.4")
else:
    print("Different salt: no pseudonym lines up with 10.4's table. That's what rotation does: "
          "every hash changes, and joins to old data stop working.")

# COMMAND ----------

# MAGIC %md
# MAGIC The code no longer contains the salt: rotate the secret, and nobody edits a notebook. The
# MAGIC classic use is credentials: `dbutils.secrets.get` feeding a JDBC password, and lesson 11.3
# MAGIC feeds a federation connection the same way. Lessons 10.3 and 10.4 still show the literal
# MAGIC on purpose, so you can see what's wrong with it. In your own pipeline, this line replaces it.
# MAGIC
# MAGIC ## 3. The governance side
# MAGIC
# MAGIC - Scopes carry **ACLs** (`databricks secrets put-acl`: READ / WRITE / MANAGE per principal):
# MAGIC   the platform team MANAGEs, pipelines READ. Single-user workspace here, so shown as
# MAGIC   commands, exercised in teams. These aren't Unity Catalog grants.
# MAGIC - Secrets are for SECRETS: connection strings, tokens, salts. Business config belongs in
# MAGIC   widgets and job parameters (S5's contract).
# MAGIC - **There are now two kinds.** Besides scopes, Unity Catalog secrets (GA since August 2026)
# MAGIC   live at `catalog.schema.secret` and are governed by Unity Catalog privileges. The card
# MAGIC   in lesson 10.7 has the fork.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Scope + key via CLI · `dbutils.secrets.get` in code · output redaction is a guardrail ·
# MAGIC ACLs govern access · rotation without code edits. Next, a reference card, then the lesson
# MAGIC the guide actually headlines: ABAC.

# COMMAND ----------

dbutils.notebook.exit("secrets-complete")
