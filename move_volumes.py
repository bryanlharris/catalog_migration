# Databricks notebook source
# MAGIC %md
# MAGIC # Move Volumes Between Catalogs
# MAGIC This notebook reads all volumes from a source catalog and generates SQL commands to drop those volumes from the source catalog and recreate them in a destination catalog with the same schema and location.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Source and Destination Catalogs

# COMMAND ----------

# Create widgets for user input
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
# MAGIC ## Read Volumes From the Source Catalog

# COMMAND ----------

query = f"""
SELECT volume_catalog, volume_schema, volume_name, storage_location
FROM system.information_schema.volumes
WHERE volume_catalog = '{source_catalog}'
"""

volumes_df = spark.sql(query)
volumes = [
    {
        "catalog": row["volume_catalog"],
        "schema": row["volume_schema"],
        "name": row["volume_name"],
        "location": row["storage_location"],
    }
    for row in volumes_df.collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate SQL Commands

# COMMAND ----------

unmount_cmds = []
mount_cmds = []
for v in volumes:
    unmount_cmds.append(
        f"DROP VOLUME IF EXISTS {v['catalog']}.{v['schema']}.{v['name']};"
    )
    mount_cmds.append(
        f"""CREATE SCHEMA IF NOT EXISTS {destination_catalog}.{v['schema']};
CREATE EXTERNAL VOLUME {destination_catalog}.{v['schema']}.{v['name']} LOCATION '{v['location']}';"""
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Commands

# COMMAND ----------

print("-- Commands to drop volumes from the source catalog --")
for cmd in unmount_cmds:
    print(cmd)

print("\n-- Commands to recreate volumes in the destination catalog --")
for cmd in mount_cmds:
    print(cmd)
