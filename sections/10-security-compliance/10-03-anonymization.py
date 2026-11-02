# Databricks notebook source
# MAGIC %md
# MAGIC # 10.3 — Anonymization & pseudonymization
# MAGIC
# MAGIC Masking (last lesson) hides data from the wrong eyes. This lesson
# MAGIC CHANGES the data so the person can't be identified — while keeping it
# MAGIC useful. The guide names four techniques: **hashing, tokenization,
# MAGIC suppression, generalization**, "e.g., using column masks and related Unity Catalog
# MAGIC features". All four, on our customers, and then the same idea inside a mask.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vocabulary that decides exam answers
# MAGIC
# MAGIC - **Pseudonymization**: identity replaced by a stand-in that CAN be
# MAGIC   linked back (with a key/vault). Still personal data under GDPR —
# MAGIC   but far safer to process. Hashing, tokenization.
# MAGIC - **Anonymization**: link destroyed for good. Suppression,
# MAGIC   generalization (done right). No re-identification, no key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hashing — consistent stand-ins that still JOIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        sha2(concat(email, 'lakeshore-salt-2026'), 256) AS email_hash,
# MAGIC        email
# MAGIC FROM lakeshore.silver.customers LIMIT 3;

# COMMAND ----------

# The property that makes hashed keys useful: determinism → joins survive.
# And the salt is load-bearing: change it and the stand-ins are unlinkable.
a = spark.sql("""SELECT sha2(concat(email, 'lakeshore-salt-2026'), 256) AS h
                 FROM lakeshore.silver.customers ORDER BY customer_id LIMIT 1""").first()[0]
a2 = spark.sql("""SELECT sha2(concat(email, 'lakeshore-salt-2026'), 256) AS h
                  FROM lakeshore.silver.customers ORDER BY customer_id LIMIT 1""").first()[0]
b = spark.sql("""SELECT sha2(concat(email, 'other-salt'), 256) AS h
                 FROM lakeshore.silver.customers ORDER BY customer_id LIMIT 1""").first()[0]
print("same input + same salt  →", a[:16], "…")
print("same input + same salt  →", a2[:16], "… (identical: joins work)")
print("same input + OTHER salt →", b[:16], "… (unlinkable)")
assert a == a2 and a != b

# COMMAND ----------

# MAGIC %md
# MAGIC Salted `sha2` gives every dataset the same stand-in for the same
# MAGIC person — analytics and joins keep working, names don't travel. Two
# MAGIC cautions the exam probes: unsalted hashes of low-cardinality data are
# MAGIC crackable by brute force (hash every possible phone number and match),
# MAGIC and the salt is now a SECRET (lesson 10.6's subject).
# MAGIC
# MAGIC ## 3. Tokenization — the reversible stand-in, by design

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A token vault: random tokens ↔ identities, stored SEPARATELY, locked
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.token_vault AS
# MAGIC SELECT customer_id,
# MAGIC        concat('TOK-', sha2(concat(customer_id, uuid()), 256)) AS token
# MAGIC FROM lakeshore.silver.customers;
# MAGIC
# MAGIC SELECT * FROM lakeshore.silver.token_vault LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC Tokens are RANDOM (note the uuid — no formula connects token to
# MAGIC identity). Re-identification exists exactly once, in the vault, which
# MAGIC gets the tightest grants in the house. Hash = derivable stand-in;
# MAGIC token = stored stand-in. That's the exam's distinction.
# MAGIC
# MAGIC ## 4. Suppression & generalization — the anonymizers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.token,
# MAGIC        -- suppression: names simply don't travel
# MAGIC        -- generalization: exact signup ts → cohort month
# MAGIC        date_trunc('MONTH', c.created_at)          AS signup_cohort,
# MAGIC        -- generalization: email → provider domain only
# MAGIC        substr(c.email, instr(c.email, '@') + 1)   AS email_domain
# MAGIC FROM lakeshore.silver.customers c
# MAGIC JOIN lakeshore.silver.token_vault v USING (customer_id)
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Suppression: the column doesn't exist downstream. Generalization:
# MAGIC precision traded for privacy — month not timestamp, domain not
# MAGIC address, age band not birthday. The honest caveat (say it): a few
# MAGIC quasi-identifiers COMBINED can still single someone out — that's why
# MAGIC k-anonymity thinking exists: every published combination should match
# MAGIC a crowd, not a person.
# MAGIC
# MAGIC ## 5. The technique table

# COMMAND ----------

# MAGIC %md
# MAGIC | Technique | Reversible? | Joins survive? | Class |
# MAGIC |---|---|---|---|
# MAGIC | Salted hash | no formula back (salt = secret) | ✅ same salt | pseudonymization |
# MAGIC | Tokenization | ✅ via the locked vault | ✅ | pseudonymization |
# MAGIC | Suppression | ❌ gone | ❌ | anonymization |
# MAGIC | Generalization | ❌ precision lost | coarse only | anonymization |
# MAGIC
# MAGIC ## 6. The same techniques, applied at read time
# MAGIC
# MAGIC Everything above changed the data before it travels. A column mask applies a
# MAGIC technique when somebody reads: the stored value stays, and what you get depends on
# MAGIC who you are. Databricks also ships a built-in `mask()` for character-level redaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mask(): upper case to X, lower case to x, digits to n. Built in, no UDF needed.
# MAGIC SELECT customer_id, email, mask(email) AS masked_email
# MAGIC FROM lakeshore.silver.customers
# MAGIC WHERE email IS NOT NULL
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC Put any of these inside the function a column mask calls and you've got hashing or
# MAGIC generalization on read. **The fork:** transform in the pipeline when nobody downstream
# MAGIC should ever hold the value (lesson 10.4). Mask at read when some people legitimately
# MAGIC need it and others don't (lessons 10.2 and 10.8). A mask hides, and it doesn't remove:
# MAGIC under GDPR the stored value is still personal data.

# COMMAND ----------

n = spark.table("lakeshore.silver.token_vault").count()
assert n == spark.table("lakeshore.silver.customers").count()
print(f"✓ vault covers all {n} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Pseudonymize to keep working (hash for derivable consistency, tokens
# MAGIC for governed reversibility); anonymize to stop identifying (suppress,
# MAGIC generalize — mind the quasi-identifiers). The vault stays as a course
# MAGIC object: the next lesson's discussion leans on it, and lesson 10.5 has to erase from
# MAGIC it. Next: masking applied IN MOTION — the compliant PII pipeline.

# COMMAND ----------

dbutils.notebook.exit("anonymization-complete")
