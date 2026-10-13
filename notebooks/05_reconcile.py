# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Reconcile (mainframe vs Databricks)
# MAGIC
# MAGIC Compares the migrated Gold result against the legacy (mainframe) extract
# MAGIC unloaded to the bridge, and fails the task on any mismatch.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mf_poc")
dbutils.widgets.text("schema", "accounting")
dbutils.widgets.text("tolerance", "0.01")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tol = float(dbutils.widgets.get("tolerance"))

legacy = spark.read.parquet("/Volumes/mf_poc/landing/bridge/ACCTMAST.parquet") \
              .select("ACCT_NO", "ACCT_BALANCE", "ACCT_STATUS")
current = spark.table(f"{catalog}.{schema}.silver_account") \
               .select("ACCT_NO", "ACCT_BALANCE", "ACCT_STATUS")

j = legacy.alias("mf").join(current.alias("dbx"), "ACCT_NO", "outer")

n_mismatch = (j.filter(
    (F.abs(F.col("mf.ACCT_BALANCE") - F.col("dbx.ACCT_BALANCE")) > tol) |
    (F.col("mf.ACCT_STATUS") != F.col("dbx.ACCT_STATUS")) |
    F.col("mf.ACCT_NO").isNull() | F.col("dbx.ACCT_NO").isNull()
).count())

print("mismatches:", n_mismatch)
assert n_mismatch == 0, f"reconciliation FAILED: {n_mismatch} mismatches"
print("reconciliation PASSED")
