# Databricks notebook source
# MAGIC %md
# MAGIC # Clone a Catalog
# MAGIC This notebook uses [DiscoverX](https://github.com/databrickslabs/discoverx) to clone all schemas and tables from a source catalog into a destination catalog using Delta Lake `CLONE`.
# MAGIC
# MAGIC Specify the source and destination catalogs with the widgets below and run all cells.

# COMMAND ----------

# MAGIC %pip install dbl-discoverx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure source and destination catalogs

# COMMAND ----------

# Create widgets for user input
dbutils.widgets.text("1.source_catalog", "source_catalog")
dbutils.widgets.text("2.destination_catalog", "destination_catalog")

source_catalog = dbutils.widgets.get("1.source_catalog")
destination_catalog = dbutils.widgets.get("2.destination_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone all tables using DiscoverX

# COMMAND ----------

from discoverx import DX

dx = DX()

spark.sql(f"CREATE CATALOG IF NOT EXISTS {destination_catalog}")


def clone_table(table_info):
    """Clone a single table into the destination catalog."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination_catalog}.{table_info.schema}")
    try:
        spark.sql(
            f"""CREATE OR REPLACE TABLE {destination_catalog}.{table_info.schema}.{table_info.table} CLONE {table_info.catalog}.{table_info.schema}.{table_info.table}"""
        )
        return {
            "source": f"{table_info.catalog}.{table_info.schema}.{table_info.table}",
            "destination": f"{destination_catalog}.{table_info.schema}.{table_info.table}",
            "success": True,
            "info": None,
        }
    except Exception as err:
        return {
            "source": f"{table_info.catalog}.{table_info.schema}.{table_info.table}",
            "destination": f"{destination_catalog}.{table_info.schema}.{table_info.table}",
            "success": False,
            "info": str(err),
        }


# Apply clone function to all tables in the source catalog
results = dx.from_tables(f"{source_catalog}.*.*").map(clone_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show cloning results

# COMMAND ----------

results.display()
