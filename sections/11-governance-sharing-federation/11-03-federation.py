# Databricks notebook source
# MAGIC %md
# MAGIC # 11.3 — Lakehouse Federation, on a real Postgres
# MAGIC
# MAGIC The guide: "Configure Lakehouse Federation with governance across supported source
# MAGIC systems, applying UC permissions and connection-level credentials."
# MAGIC
# MAGIC Free Edition has no external database, but it does include one **Lakebase Postgres
# MAGIC project**. So we create a real Postgres, put a table in it, federate it through Unity
# MAGIC Catalog, join it to the lakehouse, and tear it all down.
# MAGIC
# MAGIC > Free Edition allows **one Lakebase project per account**. If you already have one under
# MAGIC > another name, change `PROJECT` below to it and skip the teardown of the project.
# MAGIC > Everything here also has a CLI form: `databricks postgres create-project lakeshore-pg`,
# MAGIC > `list-endpoints`, `generate-database-credential`, `delete-project`.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A Postgres to federate
# MAGIC
# MAGIC New Lakebase instances are Autoscaling projects, driven by the Postgres API. A project gets
# MAGIC a `production` branch and a `primary` compute. Lakebase isn't on the exam: it's just the
# MAGIC free database we're pointing at.

# COMMAND ----------

import time

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists

PROJECT = "lakeshore-pg"
ENDPOINT = f"projects/{PROJECT}/branches/production/endpoints/primary"
SCOPE = "lakeshore-secrets"

w = WorkspaceClient()
api = w.api_client
try:
    api.do("GET", f"/api/2.0/postgres/projects/{PROJECT}")
    print("project already exists:", PROJECT)
except NotFound:
    api.do("POST", "/api/2.0/postgres/projects", query={"project_id": PROJECT}, body={})
    print("project requested:", PROJECT)

for _ in range(40):
    try:
        endpoint = api.do("GET", f"/api/2.0/postgres/{ENDPOINT}")
        if endpoint["status"].get("hosts", {}).get("host"):
            break
    except NotFound:
        pass
    time.sleep(15)
HOST = endpoint["status"]["hosts"]["host"]
print("host is regional, and it's ready:", HOST.split(".", 1)[1])

# COMMAND ----------

me = spark.sql("SELECT current_user()").first()[0]
token = api.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT})["token"]

with psycopg.connect(host=HOST, port=5432, dbname="databricks_postgres", user=me,
                     password=token, sslmode="require", connect_timeout=60) as conn:
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS public.suppliers (
                         supplier_id INT PRIMARY KEY, name TEXT, country TEXT, category TEXT)""")
        cur.execute("TRUNCATE public.suppliers")
        cur.execute("""INSERT INTO public.suppliers VALUES
                         (1, 'Alpine Thread',    'IT', 'Apparel'),
                         (2, 'Northwind Canvas', 'CA', 'Hiking'),
                         (3, 'Harbor Buckle',    'PT', 'Footwear'),
                         (4, 'Cedar Stove Co',   'US', 'Camping')""")
    conn.commit()
print("✓ public.suppliers is in Postgres, and Databricks has no copy of it")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The credential, kept out of the SQL
# MAGIC
# MAGIC The connection is where the credentials live, so that's where the guide's "connection-level
# MAGIC credentials" happens. The docs recommend `secret()` over plaintext. The honest caveat: a
# MAGIC Lakebase OAuth token **expires after an hour**, so this connection stops working soon after
# MAGIC this lesson. That's fine for a demo. In production you store a durable database credential:
# MAGIC here, a native Postgres role.

# COMMAND ----------

from databricks.sdk.errors import DatabricksError

try:
    w.secrets.create_scope(scope=SCOPE)
except DatabricksError as e:
    assert "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e), e
w.secrets.put_secret(scope=SCOPE, key="pg_user", string_value=me)
w.secrets.put_secret(scope=SCOPE, key="pg_token", string_value=token)

# COMMAND ----------

spark.sql("DROP CATALOG IF EXISTS lakeshore_pg CASCADE")
spark.sql("DROP CONNECTION IF EXISTS lakeshore_pg_conn")

spark.sql(f"""
    CREATE CONNECTION lakeshore_pg_conn TYPE postgresql
    OPTIONS (
      host '{HOST}',
      port '5432',
      user secret('{SCOPE}', 'pg_user'),
      password secret('{SCOPE}', 'pg_token')
    )""")

spark.sql("""
    CREATE FOREIGN CATALOG lakeshore_pg
    USING CONNECTION lakeshore_pg_conn
    OPTIONS (database 'databricks_postgres')""")

# COMMAND ----------

# MAGIC %md
# MAGIC Two securables. The **connection** holds the path and credentials. The **foreign catalog**
# MAGIC mirrors one database into the three-level namespace. Look at what the connection shows:
# MAGIC `Read-only: true`, and no credentials anywhere.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CONNECTION lakeshore_pg_conn;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query it, and join it to the lakehouse

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN lakeshore_pg;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A Postgres table joined to a Delta table, in one Spark SQL statement.
# MAGIC SELECT s.name AS supplier, s.country, count(*) AS products_in_category
# MAGIC FROM lakeshore_pg.public.suppliers s
# MAGIC JOIN lakeshore.bronze.products p ON p.category = s.category
# MAGIC GROUP BY s.name, s.country
# MAGIC ORDER BY products_in_category DESC;

# COMMAND ----------

n = spark.table("lakeshore_pg.public.suppliers").count()
assert n == 4, n
print(f"✓ {n} rows read through Unity Catalog, none of them ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. See the pushdown
# MAGIC
# MAGIC Query federation translates your SQL and sends it to the source. `EXPLAIN FORMATTED` shows
# MAGIC what went down. So does the foreign-scan node in the query profile.

# COMMAND ----------

plan = spark.sql("""EXPLAIN FORMATTED
    SELECT country, count(*) FROM lakeshore_pg.public.suppliers
    WHERE country <> 'IT' GROUP BY country""").first()[0]
print(plan[:900])
assert "Scan JDBC" in plan and "PushedDown" in plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read-only, and governed like anything else

# COMMAND ----------

try:
    spark.sql("INSERT INTO lakeshore_pg.public.suppliers VALUES (5, 'x', 'y', 'z')").collect()
    raise AssertionError("expected the write to be refused")
except Exception as e:
    assert "PERMISSION_DENIED" in str(e), str(e)[:300]
    print("✓ write refused: only READ credentials can be retrieved for foreign tables")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The usual three keys, on the foreign catalog. Inheritance works as in lesson 10.1.
# MAGIC GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG lakeshore_pg TO `account users`;
# MAGIC SHOW GRANTS ON TABLE lakeshore_pg.public.suppliers;

# COMMAND ----------

rows = [tuple(r) for r in spark.sql("SHOW GRANTS ON TABLE lakeshore_pg.public.suppliers").collect()]
assert ("account users", "SELECT", "CATALOG", "lakeshore_pg") in rows, rows
spark.sql("REVOKE USE CATALOG, USE SCHEMA, SELECT ON CATALOG lakeshore_pg FROM `account users`")

# COMMAND ----------

# MAGIC %md
# MAGIC | Who | Needs | For |
# MAGIC |---|---|---|
# MAGIC | Connection creator | CREATE CONNECTION on the metastore | holding credentials |
# MAGIC | Foreign catalog creator | CREATE CATALOG on the metastore, **and** ownership of the connection or CREATE FOREIGN CATALOG on it | mounting a database |
# MAGIC | Connection user | USE CONNECTION on the connection | using it directly |
# MAGIC | Analyst | USE CATALOG + USE SCHEMA + SELECT on the foreign catalog | reading tables |
# MAGIC
# MAGIC The consequence people miss: **every query reaches the source as the connection's one
# MAGIC identity.** Postgres sees a single user. Who may read what is decided in Unity Catalog, not
# MAGIC by the source's own per-user permissions. So the credential you store should have the
# MAGIC narrowest read access the source allows.
# MAGIC
# MAGIC ## 6. Tear it down

# COMMAND ----------

spark.sql("DROP CATALOG IF EXISTS lakeshore_pg CASCADE")
spark.sql("DROP CONNECTION IF EXISTS lakeshore_pg_conn")
w.secrets.delete_secret(scope=SCOPE, key="pg_user")
w.secrets.delete_secret(scope=SCOPE, key="pg_token")
api.do("DELETE", f"/api/2.0/postgres/projects/{PROJECT}")
print("✓ catalog, connection, secrets and project removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Connection (credentials, read-only) + foreign catalog (the namespace) · `secret()` over
# MAGIC plaintext · pushdown visible in `EXPLAIN FORMATTED` · writes refused · ordinary grants
# MAGIC decide who reads, and the source sees one identity. Next: the case where neither party may
# MAGIC see the other's rows.

# COMMAND ----------

dbutils.notebook.exit("federation-complete")
