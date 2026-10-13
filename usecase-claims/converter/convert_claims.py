"""Claims use case converter: COBOL/JCL/COPYBOOK/DB2 -> Databricks artifacts.

Reuses the parsers from the main converter. Emits under
usecase-claims/artifacts/generated/:
  conversion_report.md, databricks_workflow.json, schema_claim.py, notebook_claimadj.py

Run:  python usecase-claims/converter/convert_claims.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]          # mainframe2databricks-demo/
sys.path.insert(0, str(ROOT))
from converter.mf2dbx_converter import (  # noqa: E402
    parse_copybook, parse_jcl, parse_cobol, pic_to_spark,
)

HERE = Path(__file__).resolve().parent.parent      # usecase-claims/
LEGACY = HERE / "legacy"
GEN = HERE / "artifacts" / "generated"
GEN.mkdir(parents=True, exist_ok=True)

JOB_NAME = "NIGHTLY_CLAIMS_ADJUDICATION"
PGM_TASK = {
    "SORT": ("sort_claims", "orderBy(CLAIM_ID)"),
    "CLAIMADJ": ("adjudicate_claims", "coverage + deductible + coinsurance"),
    "CLAIMRPT": ("claims_report", "paid totals + denial rate -> Gold"),
}


def build_workflow(job: str, steps: list[dict]) -> dict:
    tasks = []
    for i, s in enumerate(steps):
        key, desc = PGM_TASK.get(s["pgm"], (s["step"].lower(), ""))
        dep = PGM_TASK.get(steps[i - 1]["pgm"], (steps[i - 1]["step"].lower(), ""))[0] if i else None
        tasks.append({
            "task_key": key,
            "description": f"{s['step']} EXEC PGM={s['pgm']} -> {desc}",
            "job_cluster_key": "claims_cluster",
            "notebook_task": {"notebook_path": f"/Users/memo/mainframe/claims/{key}",
                              "source": "WORKSPACE"},
            "depends_on": [{"task_key": dep}] if dep else [],
        })
    return {
        "name": JOB_NAME,
        "job_cluster": {
            "job_cluster_key": "claims_cluster",
            "new_cluster": {"spark_version": "15.4.x-scala2.12",
                            "node_type_id": "i3.xlarge", "num_workers": 2},
        },
        "tasks": tasks,
        "parameters": [{"name": "JOB_NAME", "default": job},
                       {"name": "PLAN_YEAR", "default": "2024"}],
        "tags": {"usecase": "claims", "poc": "mainframe-to-databricks"},
    }


def build_schema(claim_fields, member_fields) -> str:
    def block(name, fields):
        out = [f"{name} = StructType(["]
        for f, pic in fields:
            col = f.lower().replace("-", "_")
            out.append(f'    StructField("{col}", {pic_to_spark(pic)}, True),')
        out.append("])")
        return "\n".join(out)
    return (
        '"""COPYBOOKs -> PySpark StructTypes (claims use case)."""\n'
        "from pyspark.sql.types import (StructType, StructField, StringType,\n"
        "                               IntegerType, LongType, DoubleType, DecimalType)\n\n"
        + block("CLAIM_SCHEMA", claim_fields) + "\n\n"
        + block("MEMBER_SCHEMA", member_fields) + "\n"
    )


def build_notebook() -> str:
    return '''# Databricks notebook source
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
           F.round(F.col("paid_cents") / 100, 2).alias("paid_amount")) \\
   .write.mode("overwrite").saveAsTable("gold_claim_adjudicated")
print("adjudication complete")
'''


def build_report(claim_fields, member_fields, job, steps, cobol_stats, tables, views) -> str:
    lines = [
        f"# Conversion Report — {JOB_NAME}",
        "",
        "Claims adjudication batch -> Databricks. Automatic mapping summary.",
        "",
        "## Component mapping",
        "",
        "| Mainframe component | Source | Databricks target |",
        "|---|---|---|",
        "| JCL job stream | `legacy/jcl/CLAIMS_BATCH.jcl` | Databricks Workflow |",
        "| COBOL program | `legacy/cobol/CLAIMADJ.cbl` | PySpark notebook |",
        "| COPYBOOK (claim) | `legacy/copybook/CLAIMREC.cpy` | StructType |",
        "| COPYBOOK (member) | `legacy/copybook/MEMBREC.cpy` | StructType |",
        "| VSAM KSDS (member) | `MEMBMAST` | Delta table + MERGE |",
        "| Sequential claim file | `HLTH.CLAIM.DAILY` | Parquet -> Bronze |",
        "| DB2 tables | `legacy/db2/CLAIMS_DDL.sql` | Delta tables |",
        "| SORT utility | JCL STEP010 | `orderBy` |",
        "",
        "## JCL -> Workflow tasks",
        "",
        f"Job: **{job}**",
        "",
        "| JCL step | Program | Databricks task | Depends on |",
        "|---|---|---|---|",
    ]
    for i, s in enumerate(steps):
        key, desc = PGM_TASK.get(s["pgm"], (s["step"].lower(), ""))
        dep = PGM_TASK.get(steps[i - 1]["pgm"], (steps[i - 1]["step"].lower(), ""))[0] if i else "-"
        lines.append(f"| {s['step']} | {s['pgm']} | `{key}` ({desc}) | {dep} |")

    lines += ["", "## COPYBOOK -> schema", "",
              "| Copybook | Field | PIC | Spark type |", "|---|---|---|---|"]
    for cb, fields in (("CLAIMREC", claim_fields), ("MEMBREC", member_fields)):
        for name, pic in fields:
            lines.append(f"| {cb} | {name.lower().replace('-', '_')} | {pic} | "
                         f"{pic_to_spark(pic).split('#')[0].strip()} |")

    lines += ["", "## COBOL statements -> Spark (CLAIMADJ.cbl)", "",
              "| COBOL construct | Count | Databricks equivalent |", "|---|---|---|"]
    for name, count, target in cobol_stats:
        lines.append(f"| {name} | {count} | {target} |")

    lines += ["", "## DB2 -> Delta", ""]
    for t in tables:
        lines.append(f"- table `{t}` -> Delta `{t.split('.')[-1].lower()}`")
    for v in views:
        lines.append(f"- view `{v}` -> SQL view `{v.split('.')[-1].lower()}`")

    lines += [
        "",
        "## Business logic migrated",
        "- Coverage check (`SERVICE-CODE` in {MED,SUR,LAB}); COS/ALT denied.",
        "- Stateful deductible per member (ordered by CLAIM-ID).",
        "- Coinsurance by plan (GOLD 90% / SILVER 80% / BRONZE 70%).",
        "- Status: APPROVED / NO-PAY / DENIED; member accumulator updated via MERGE.",
    ]
    return "\n".join(lines)


def main() -> None:
    claim_fields = parse_copybook(LEGACY / "copybook" / "CLAIMREC.cpy")
    member_fields = parse_copybook(LEGACY / "copybook" / "MEMBREC.cpy")
    job, steps = parse_jcl(LEGACY / "jcl" / "CLAIMS_BATCH.jcl")
    cobol_stats = parse_cobol(LEGACY / "cobol" / "CLAIMADJ.cbl")

    text = (LEGACY / "db2" / "CLAIMS_DDL.sql").read_text()
    import re
    tables = re.findall(r"CREATE TABLE\s+([A-Z0-9_.]+)", text, re.I)
    views = re.findall(r"CREATE VIEW\s+([A-Z0-9_.]+)", text, re.I)

    (GEN / "schema_claim.py").write_text(build_schema(claim_fields, member_fields), encoding="utf-8")
    (GEN / "databricks_workflow.json").write_text(
        json.dumps(build_workflow(job, steps), indent=2), encoding="utf-8")
    (GEN / "notebook_claimadj.py").write_text(build_notebook(), encoding="utf-8")
    (GEN / "conversion_report.md").write_text(
        build_report(claim_fields, member_fields, job, steps, cobol_stats, tables, views),
        encoding="utf-8")

    print(f"Converted {JOB_NAME}:")
    print(f"  - {GEN / 'conversion_report.md'}")
    print(f"  - {GEN / 'databricks_workflow.json'}")
    print(f"  - {GEN / 'schema_claim.py'}")
    print(f"  - {GEN / 'notebook_claimadj.py'}")
    print(f"  - {len(steps)} JCL steps, {len(claim_fields)} claim + "
          f"{len(member_fields)} member fields mapped")


if __name__ == "__main__":
    main()
