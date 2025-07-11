# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate Catalog Object Permissions
# MAGIC This notebook reads all object permissions from a source catalog and generates SQL commands
# MAGIC to grant the same privileges on the objects in a destination catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Source and Destination Catalogs

# COMMAND ----------

try:
    dbutils.widgets.text("1.source_catalog", "source_catalog")
    dbutils.widgets.text("2.destination_catalog", "destination_catalog")
    source_catalog = dbutils.widgets.get("1.source_catalog")
    destination_catalog = dbutils.widgets.get("2.destination_catalog")
except NameError:
    # When running as a standard Python script (e.g., for testing), define values here
    source_catalog = "source_catalog"
    destination_catalog = "destination_catalog"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Object Permissions From the Source Catalog

# COMMAND ----------

query = f"""
SELECT
  object_schema,
  object_name,
  object_type,
  principal,
  privilege_type
FROM system.information_schema.object_privileges
WHERE object_catalog = '{source_catalog}'
"""

try:
    priv_df = spark.sql(query)
except Exception:
    fallback_query = f"""
    SELECT
      tp.table_schema AS object_schema,
      tp.table_name AS object_name,
      t.table_type AS object_type,
      tp.grantee AS principal,
      tp.privilege_type AS privilege_type
    FROM system.information_schema.table_privileges tp
    LEFT JOIN system.information_schema.tables t
      ON tp.table_catalog = t.table_catalog
     AND tp.table_schema = t.table_schema
     AND tp.table_name = t.table_name
    WHERE tp.table_catalog = '{source_catalog}'
    UNION ALL
    SELECT
      volume_schema AS object_schema,
      volume_name AS object_name,
      'VOLUME' AS object_type,
      grantee AS principal,
      privilege_type
    FROM system.information_schema.volume_privileges
    WHERE volume_catalog = '{source_catalog}'
    """

    priv_df = spark.sql(fallback_query)
privileges = [
    {
        "schema": row["object_schema"],
        "name": row["object_name"],
        "type": row["object_type"],
        "principal": row["principal"],
        "privilege": row["privilege_type"],
    }
    for row in priv_df.collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate GRANT Commands

# COMMAND ----------

grant_cmds = []

# Retrieve tables in the destination catalog to check which tables were cloned
dest_tables_query = f"""
SELECT table_schema, table_name
FROM system.information_schema.tables
WHERE table_catalog = '{destination_catalog}'
"""

try:
    dest_table_df = spark.sql(dest_tables_query)
    dest_tables = {
        f"{row['table_schema']}.{row['table_name']}" for row in dest_table_df.collect()
    }
except Exception:
    dest_tables = set()

for p in privileges:
    if p["type"] == "TABLE" and f"{p['schema']}.{p['name']}" not in dest_tables:
        continue
    object_identifier = f"{destination_catalog}.{p['schema']}.{p['name']}"
    grant_cmds.append(
        f"GRANT {p['privilege']} ON {object_identifier} TO `{p['principal']}`;"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Commands

# COMMAND ----------

for cmd in grant_cmds:
    print(cmd)
