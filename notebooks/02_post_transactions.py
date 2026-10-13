# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Post Transactions (migrated from COBOL ACCTPOST)
# MAGIC
# MAGIC Applies the daily transaction file to the account master: CR/DR postings,
# MAGIC overdraft status, savings interest. Money is handled in **integer cents**
# MAGIC to stay exact (COMP-3 parity).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mf_poc")
dbutils.widgets.text("schema", "accounting")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

INTEREST_RATE = 0.05

# COMMAND ----------

# MAGIC %md ## S1/S2: sort + net postings per account
acct = spark.table(f"{catalog}.{schema}.bronze_account")
txn = spark.table(f"{catalog}.{schema}.bronze_transaction").orderBy("ACCT_NO")

signed = txn.withColumn(
    "signed_cents",
    (F.when(F.col("TRAN_TYPE") == "CR", F.round(F.col("TRAN_AMOUNT") * 100))
      .otherwise(-F.round(F.col("TRAN_AMOUNT") * 100))).cast("long"))
net = signed.groupBy("ACCT_NO").agg(F.sum("signed_cents").alias("NET_CENTS"))

# COMMAND ----------

# MAGIC %md ## S3: apply postings, overdraft, savings interest
updates = (acct
    .withColumn("BALANCE_CENTS", F.round(F.col("ACCT_BALANCE") * 100).cast("long"))
    .join(net, "ACCT_NO", "left")
    .withColumn("NET_CENTS", F.coalesce("NET_CENTS", F.lit(0)).cast("long"))
    .withColumn("NEW_BALANCE_CENTS", F.col("BALANCE_CENTS") + F.col("NET_CENTS"))
    .withColumn("ACCT_STATUS",
                F.when((F.col("NEW_BALANCE_CENTS") < 0) & (F.col("ACCT_TYPE") == "CH"), "O")
                 .otherwise("A"))
    .withColumn("INTEREST_CENTS",
                F.when((F.col("ACCT_TYPE") == "SA") & (F.col("NEW_BALANCE_CENTS") > 0),
                       F.round(F.col("NEW_BALANCE_CENTS") * INTEREST_RATE))
                 .otherwise(F.lit(0.0)).cast("long"))
    .withColumn("ACCT_BALANCE",
                F.round((F.col("NEW_BALANCE_CENTS") + F.col("INTEREST_CENTS")) / 100, 2))
    .withColumn("ACCT_LAST_DATE", F.lit("20240131"))
    .select("ACCT_NO", "ACCT_NAME", "ACCT_TYPE", "ACCT_BALANCE", "ACCT_STATUS", "ACCT_LAST_DATE"))

# COMMAND ----------

# MAGIC %md ## S4: MERGE into the account master (VSAM KSDS -> Delta keyed table)
updates.createOrReplaceTempView("acct_updates")
spark.sql(f"""
    MERGE INTO {catalog}.{schema}.silver_account AS t
    USING acct_updates AS s ON t.ACCT_NO = s.ACCT_NO
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("posting complete")
