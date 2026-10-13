# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Backup (migrated from JCL IDCAMS REPRO)
# MAGIC
# MAGIC The mainframe backed up the VSAM file with `IDCAMS REPRO`. On Delta you get
# MAGIC versioning for free, but an explicit snapshot table is a convenient,
# MAGIC queryable backup.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mf_poc")
dbutils.widgets.text("schema", "accounting")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

snapshot = (spark.table(f"{catalog}.{schema}.silver_account")
            .withColumn("_backup_at", F.current_timestamp()))
snapshot.write.mode("overwrite").format("delta") \
        .saveAsTable(f"{catalog}.{schema}.backup_account")

# Alternative (no copy): time travel is the native backup.
print("accounts backed up:", snapshot.count())
spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.silver_account") \
     .select("version", "timestamp", "operation").show(5, truncate=False)
