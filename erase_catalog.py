try:
    dbutils.widgets.text("1.catalog", "catalog")
    catalog = dbutils.widgets.get("1.catalog")
except NameError:
    catalog = "do_not_use"

spark.sql(f"USE CATALOG `{catalog}`")

skip_schemas = {"information_schema", "pg_catalog"}

schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog}`")
schemas = [row[0] for row in schemas_df.collect() if row[0].lower() not in skip_schemas]

for schema in schemas:
    funcs_df = spark.sql(f"SHOW USER FUNCTIONS IN `{catalog}`.`{schema}`")
    for f in funcs_df.collect():
        func_name = f[0]
        print(f"DROP FUNCTION IF EXISTS `{catalog}`.`{schema}`.`{func_name}`;")

# Drop tables and views
for schema in schemas:
    tables_df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    for t in tables_df.collect():
        desc = spark.sql(f"DESCRIBE EXTENDED `{catalog}`.`{schema}`.`{t.tableName}`").collect()
        t_type = next((row.data_type for row in desc if str(row.col_name).upper() == 'TYPE'), '').upper().replace('_', ' ')
        if t_type == 'VIEW':
            print(f"DROP VIEW IF EXISTS `{catalog}`.`{schema}`.`{t.tableName}`;")
        elif t_type == 'MATERIALIZED_VIEW':
            print(f"DROP MATERIALIZED VIEW IF EXISTS `{catalog}`.`{schema}`.`{t.tableName}`;")
        else:
            print(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{t.tableName}`;")
    print(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}`;")

# Drop external volumes
volume_query = f"""
SELECT volume_schema, volume_name
FROM system.information_schema.volumes
WHERE volume_catalog = '{catalog}'
  AND volume_type = 'EXTERNAL'
"""
volumes_df = spark.sql(volume_query)
for v in volumes_df.collect():
    print(f"DROP VOLUME IF EXISTS `{catalog}`.`{v.volume_schema}`.`{v.volume_name}`;")

print(f"-- Catalog `{catalog}` cleanup commands generated. --")
