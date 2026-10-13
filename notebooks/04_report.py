# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Report (migrated from COBOL ACCTRPT)
# MAGIC
# MAGIC End-of-night report: totals by account type, list of overdrawn accounts.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mf_poc")
dbutils.widgets.text("schema", "accounting")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

acct = spark.table(f"{catalog}.{schema}.silver_account")

# COMMAND ----------

by_type = (acct.groupBy("ACCT_TYPE")
           .agg(F.count("*").alias("accounts"),
                F.round(F.sum("ACCT_BALANCE"), 2).alias("total_balance"),
                F.sum(F.when(F.col("ACCT_STATUS") == "O", 1).otherwise(0)).alias("overdrawn"))
           .orderBy("ACCT_TYPE"))
by_type.write.mode("overwrite").format("delta") \
       .saveAsTable(f"{catalog}.{schema}.gold_account_summary")
by_type.show()

overdrawn = (acct.filter("ACCT_STATUS = 'O'")
             .select("ACCT_NO", "ACCT_NAME", "ACCT_BALANCE")
             .orderBy("ACCT_BALANCE"))
overdrawn.write.mode("overwrite").format("delta") \
         .saveAsTable(f"{catalog}.{schema}.gold_overdrawn_accounts")
print("overdrawn accounts:", overdrawn.count())
