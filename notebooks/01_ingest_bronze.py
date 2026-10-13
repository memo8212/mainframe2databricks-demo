# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest to Bronze (from the VSAM/DB2 Parquet bridge)
# MAGIC
# MAGIC Migrated from JCL: the unloaded extracts land in the bridge and are
# MAGIC ingested into Bronze Delta tables with audit columns.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mf_poc")
dbutils.widgets.text("schema", "accounting")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bridge = "/Volumes/mf_poc/landing/bridge"     # VSAM/DB2 unload (Parquet)

# COMMAND ----------

# MAGIC %md ## Account master (from VSAM ACCTMAST)
(spark.read.parquet(f"{bridge}/accounts")
     .withColumn("_ingested_at", F.current_timestamp())
     .withColumn("_source", F.lit("ACCTMAST"))
     .write.mode("overwrite").format("delta")
     .saveAsTable(f"{catalog}.{schema}.bronze_account"))

# MAGIC %md ## Transactions (from the sequential TRANS file)
(spark.read.parquet(f"{bridge}/transactions")
     .withColumn("_ingested_at", F.current_timestamp())
     .withColumn("_source", F.lit("PROD.TRANS.DAILY"))
     .write.mode("overwrite").format("delta")
     .saveAsTable(f"{catalog}.{schema}.bronze_transaction"))

print("bronze loaded: bronze_account, bronze_transaction")
