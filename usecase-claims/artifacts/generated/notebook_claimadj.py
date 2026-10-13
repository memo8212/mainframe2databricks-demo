# Databricks notebook source
# MAGIC %md
# MAGIC # CLAIMADJ (migrated from COBOL) — claims adjudication
# MAGIC Applies coverage, deductible and coinsurance to the daily claims.

# COMMAND ----------
from pyspark.sql import functions as F, Window

COVERED = ["MED", "SUR", "LAB"]                 # COS/ALT not covered
COINS = {"GOLD": 0.90, "SILVER": 0.80, "BRONZE": 0.70}

# COMMAND ----------
# MAGIC %md ## S1: sort claims (JCL STEP010)
claims = spark.table("bronze_claim").orderBy("claim_id")
members = spark.table("silver_member")

# COMMAND ----------
# MAGIC %md ## S2: coverage + stateful deductible (COBOL READ/REWRITE loop)
coins = F.create_map([F.lit(k) for kv in COINS.items() for k in kv])
joined = (claims.join(members, "member_id", "inner")
    .withColumn("claim_cents", F.round(F.col("claim_amount") * 100).cast("long"))
    .withColumn("covered", F.col("service_code").isin(*COVERED))
    .withColumn("cov_cents", F.when("covered", F.col("claim_cents")).otherwise(F.lit(0)))
    .withColumn("ded_cents", F.round(F.col("deductible_rem") * 100).cast("long")))

w = Window.partitionBy("member_id").orderBy("claim_id").rowsBetween(Window.unboundedPreceding, -1)
prior = F.coalesce(F.sum("cov_cents").over(w), F.lit(0))
adj = (joined
    .withColumn("available", F.greatest(F.lit(0), F.col("ded_cents") - prior))
    .withColumn("ded_applied", F.least(F.col("cov_cents"), F.col("available")))
    .withColumn("eligible", F.col("cov_cents") - F.col("ded_applied"))
    .withColumn("paid_cents", F.round(F.col("eligible") * coins[F.col("plan_type")]))
    .withColumn("status", F.when(~F.col("covered"), "DENIED")
                          .when(F.col("paid_cents") > 0, "APPROVED").otherwise("NO-PAY")))

# COMMAND ----------
# MAGIC %md ## S3: write adjudicated claims + updated member accumulators
adj.select("claim_id", "member_id", "status",
           F.round(F.col("paid_cents") / 100, 2).alias("paid_amount")) \
   .write.mode("overwrite").saveAsTable("gold_claim_adjudicated")
print("adjudication complete")
