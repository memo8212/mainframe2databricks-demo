"""Mainframe -> Databricks converter (POC).

Reads the legacy artifacts under legacy/ and emits Databricks-ready outputs:

  artifacts/generated/conversion_report.md    component + statement mapping
  artifacts/generated/databricks_workflow.json  JCL job stream -> Workflow DAG
  artifacts/generated/schema_acct.py            COPYBOOK -> StructType
  artifacts/generated/notebook_acctpost.py      COBOL program -> PySpark notebook

Mapping rules (POC-level, pragmatic):
  JCL step (EXEC PGM=)   -> Workflow task (notebook), depends_on prior step
  JCL JOB card           -> Workflow parameters (job name/class/msgclass)
  COBOL COPYBOOK         -> StructType schema
  COBOL READ/REWRITE     -> point lookup + Delta MERGE (keyed update)
  COBOL sequential browse-> ORDER BY / range scan
  COBOL COMPUTE          -> withColumn expression
  COBOL IF/EVALUATE      -> when/otherwise
  COBOL PERFORM          -> helper function / loop over DataFrame ops
  SORT utility           -> orderBy
  IDCAMS REPRO           -> Delta table copy (or time-travel backup)
  DB2 table              -> Delta table
  DB2 VIEW               -> Databricks SQL view
  CICS transaction       -> Databricks SQL / serving endpoint
  VSAM KSDS              -> Delta table + business key + MERGE upsert

Run:  python converter/mf2dbx_converter.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEGACY = ROOT / "legacy"
GEN = ROOT / "artifacts" / "generated"
GEN.mkdir(parents=True, exist_ok=True)

JOB_NAME = "NIGHTLY_ACCOUNT_POSTING"


# ----------------------------------------------------------------- copybook
def parse_copybook(path: Path) -> list[tuple[str, str]]:
    fields = []
    for line in path.read_text().splitlines():
        m = re.match(r"\s*\d{2}\s+([A-Z0-9-]+)\s+PIC\s+([A-Z0-9()V]+)", line)
        if m:
            fields.append((m.group(1), m.group(2)))
    return fields


def pic_to_spark(pic: str) -> str:
    p = pic.upper()
    if p.startswith("X"):
        return f"StringType()  # {pic}"
    if "V" in p and ("S9" in p or p.startswith("9")):
        return f"DecimalType(13, 2)  # {pic}"
    if p.startswith("V"):
        return f"DoubleType()  # {pic}"
    if p.startswith("9"):
        digits = re.search(r"9\((\d+)\)", p)
        n = int(digits.group(1)) if digits else 1
        return (f"LongType()  # {pic}" if n > 9 else f"IntegerType()  # {pic}")
    return f"StringType()  # {pic}"


def build_schema_source(fields: list[tuple[str, str]]) -> str:
    lines = [
        '"""COPYBOOK ACCTREC.cpy -> PySpark StructType."""',
        "from pyspark.sql.types import (StructType, StructField, StringType,",
        "                               IntegerType, LongType, DoubleType, DecimalType)",
        "",
        "ACCT_SCHEMA = StructType([",
    ]
    for name, pic in fields:
        col = name.lower().replace("-", "_")
        lines.append(f'    StructField("{col}", {pic_to_spark(pic)}, True),')
    lines += ["])", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------- JCL
def parse_jcl(path: Path) -> tuple[str, list[dict]]:
    job = "UNKNOWN"
    steps = []
    for line in path.read_text().splitlines():
        if line.startswith("//") and " JOB " in line:
            job = line[2:].split()[0]
        m = re.match(r"//([A-Z0-9]+)\s+EXEC\s+PGM=([A-Z0-9]+)", line)
        if m:
            steps.append({"step": m.group(1), "pgm": m.group(2)})
    return job, steps


PGM_TASK = {
    "SORT": ("sort_transactions", "orderBy(ACCT_NO) -> sorted staging"),
    "ACCTPOST": ("post_transactions", "apply CR/DR + overdraft + interest (MERGE)"),
    "IDCAMS": ("backup_vsam", "Delta table copy / time-travel backup"),
    "ACCTRPT": ("build_report", "aggregate report -> Gold table"),
}


def build_workflow(job: str, steps: list[dict]) -> dict:
    tasks = []
    for i, s in enumerate(steps):
        task_key, desc = PGM_TASK.get(s["pgm"], (s["step"].lower(), "migrated task"))
        tasks.append({
            "task_key": task_key,
            "description": f"{s['step']} EXEC PGM={s['pgm']} -> {desc}",
            "job_cluster_key": "mf_cluster",
            "notebook_task": {
                "notebook_path": f"/Users/memo/mainframe/{task_key}",
                "source": "WORKSPACE",
            },
            "depends_on": [{"task_key": PGM_TASK.get(steps[i - 1]["pgm"],
                            (steps[i - 1]["step"].lower(), ""))[0]}] if i > 0 else [],
        })
    return {
        "name": JOB_NAME,
        "job_cluster": {
            "job_cluster_key": "mf_cluster",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "spark_conf": {"spark.sql.parquet.compression.codec": "snappy"},
            },
        },
        "tasks": tasks,
        "parameters": [
            {"name": "JOB_NAME", "default": job},
            {"name": "INTEREST_RATE", "default": "0.05"},
        ],
        "tags": {"poc": "mainframe-to-databricks"},
    }


# -------------------------------------------------------------------- COBOL
COBOL_RULES = [
    (r"\bREAD\b", "READ", "point lookup (join / MERGE match on key)"),
    (r"\bREWRITE\b", "REWRITE", "Delta MERGE UPDATE"),
    (r"\bWRITE\b", "WRITE", "Delta write / append"),
    (r"\bCOMPUTE\b|\bADD\b|\bSUBTRACT\b|\bMULTIPLY\b", "COMPUTE", "withColumn arithmetic"),
    (r"\bIF\b|\bEVALUATE\b", "IF/EVALUATE", "when/otherwise"),
    (r"\bPERFORM\b", "PERFORM", "loop / helper function"),
    (r"\bSORT\b", "SORT", "orderBy"),
    (r"\bCICS\b", "CICS", "SQL endpoint / serving"),
]


def parse_cobol(path: Path) -> list[tuple[str, str, str]]:
    text = path.read_text()
    out = []
    for pattern, name, target in COBOL_RULES:
        n = len(re.findall(pattern, text, re.I))
        if n:
            out.append((name, str(n), target))
    return out


# ---------------------------------------------------------------------- DB2
def parse_db2(path: Path) -> tuple[list[str], list[str]]:
    text = path.read_text()
    tables = re.findall(r"CREATE TABLE\s+([A-Z0-9_.]+)", text, re.I)
    views = re.findall(r"CREATE VIEW\s+([A-Z0-9_.]+)", text, re.I)
    return tables, views


# ------------------------------------------------------------------ notebook
def build_notebook(fields: list[tuple[str, str]]) -> str:
    cols = ", ".join(f[0].lower().replace("-", "_") for f in fields)
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # ACCTPOST (migrated from COBOL)
# MAGIC Applies the daily transaction file to the account master.
# MAGIC Generated by converter/mf2dbx_converter.py — review before production.

# COMMAND ----------
from pyspark.sql import functions as F

INTEREST_RATE = 0.05
ACCT_COLS = "{cols}"

# COMMAND ----------
# MAGIC %md ## S1: sort transactions (JCL STEP010, SORT utility)
trans = (spark.read.table("bronze_transaction")
         .orderBy("acct_no"))

# COMMAND ----------
# MAGIC %md ## S2: net postings per account (COBOL READ/REWRITE loop)
signed = trans.withColumn(
    "signed_amount",
    F.when(F.col("tran_type") == "CR", F.col("tran_amount"))
     .otherwise(-F.col("tran_amount")))
net = signed.groupBy("acct_no").agg(F.round(F.sum("signed_amount"), 2).alias("net_amount"))

# COMMAND ----------
# MAGIC %md ## S3: apply to master, overdraft + interest (COBOL COMPUTE/IF)
acct = spark.read.table("silver_account")
upd = (acct.join(net, "acct_no", "left")
       .withColumn("net_amount", F.coalesce("net_amount", F.lit(0.0)))
       .withColumn("new_balance", F.round(F.col("acct_balance") + F.col("net_amount"), 2))
       .withColumn("acct_status",
                   F.when((F.col("new_balance") < 0) & (F.col("acct_type") == "CH"), "O")
                    .otherwise("A"))
       .withColumn("interest",
                   F.when((F.col("acct_type") == "SA") & (F.col("new_balance") > 0),
                          F.round(F.col("new_balance") * INTEREST_RATE, 2)).otherwise(0.0))
       .withColumn("acct_balance", F.round(F.col("new_balance") + F.col("interest"), 2))
       .drop("net_amount", "new_balance", "interest"))

# COMMAND ----------
# MAGIC %md ## S4: MERGE into the account master (VSAM KSDS -> Delta keyed table)
upd.createOrReplaceTempView("acct_updates")
spark.sql("""
    MERGE INTO silver_account AS t
    USING acct_updates AS s ON t.acct_no = s.acct_no
    WHEN MATCHED THEN UPDATE SET *
""")
print("posting complete")
'''


# ------------------------------------------------------------------- report
def build_report(fields, job, steps, cobol_stats, tables, views) -> str:
    lines = [
        f"# Conversion Report — {JOB_NAME}",
        "",
        "Mainframe batch job -> Databricks workflow. Automatic mapping summary.",
        "",
        "## Component mapping",
        "",
        "| Mainframe component | Source | Databricks target |",
        "|---|---|---|",
        "| JCL job stream | `legacy/jcl/NIGHTLY_BATCH.jcl` | Databricks Workflow (job + tasks + deps) |",
        "| COBOL program | `legacy/cobol/ACCTPOST.cbl` | PySpark notebook |",
        "| COPYBOOK | `legacy/copybook/ACCTREC.cpy` | PySpark StructType schema |",
        "| VSAM KSDS | `legacy/vsam/ACCTMAST.vsam.txt` | Delta table + business key + MERGE |",
        "| DB2 tables | `legacy/db2/ACCT_DDL.sql` | Delta tables |",
        "| DB2 views | `legacy/db2/ACCT_DDL.sql` | Databricks SQL views |",
        "| CICS transaction | `legacy/cobol/INQTRAN.cbl` | Databricks SQL / serving endpoint |",
        "| SORT utility | JCL STEP010 | `orderBy` |",
        "| IDCAMS REPRO | JCL STEP030 | Delta table copy / time travel |",
        "",
        "## JCL job stream -> Workflow tasks",
        "",
        f"Job: **{job}**",
        "",
        "| JCL step | Program | Databricks task | Depends on |",
        "|---|---|---|---|",
    ]
    for i, s in enumerate(steps):
        tk, desc = PGM_TASK.get(s["pgm"], (s["step"].lower(), ""))
        dep = PGM_TASK.get(steps[i - 1]["pgm"], (steps[i - 1]["step"].lower(), ""))[0] if i else "-"
        lines.append(f"| {s['step']} | {s['pgm']} | `{tk}` ({desc}) | {dep} |")

    lines += ["", "## COPYBOOK -> schema", "", "| Field | PIC | Spark type |", "|---|---|---|"]
    for name, pic in fields:
        lines.append(f"| {name.lower().replace('-', '_')} | {pic} | {pic_to_spark(pic).split('#')[0].strip()} |")

    lines += ["", "## COBOL statements -> Spark (ACCTPOST.cbl)", "",
              "| COBOL construct | Count | Databricks equivalent |", "|---|---|---|"]
    for name, count, target in cobol_stats:
        lines.append(f"| {name} | {count} | {target} |")

    lines += ["", "## DB2 -> Delta", ""]
    for t in tables:
        lines.append(f"- table `{t}` -> Delta table `{t.split('.')[-1].lower()}`")
    for v in views:
        lines.append(f"- view `{v}` -> Databricks SQL view `{v.split('.')[-1].lower()}`")

    lines += [
        "",
        "## Bridge format",
        "VSAM/DB2 are unloaded to Parquet, ingested to Bronze, then MERGEd into "
        "Silver/Gold Delta tables. Validation compares legacy vs Databricks output.",
    ]
    return "\n".join(lines)


def main() -> None:
    fields = parse_copybook(LEGACY / "copybook" / "ACCTREC.cpy")
    job, steps = parse_jcl(LEGACY / "jcl" / "NIGHTLY_BATCH.jcl")
    cobol_stats = parse_cobol(LEGACY / "cobol" / "ACCTPOST.cbl")
    tables, views = parse_db2(LEGACY / "db2" / "ACCT_DDL.sql")

    (GEN / "schema_acct.py").write_text(build_schema_source(fields), encoding="utf-8")
    (GEN / "databricks_workflow.json").write_text(
        json.dumps(build_workflow(job, steps), indent=2), encoding="utf-8")
    (GEN / "notebook_acctpost.py").write_text(build_notebook(fields), encoding="utf-8")
    (GEN / "conversion_report.md").write_text(
        build_report(fields, job, steps, cobol_stats, tables, views), encoding="utf-8")

    print(f"Converted {JOB_NAME}:")
    print(f"  - {GEN / 'conversion_report.md'}")
    print(f"  - {GEN / 'databricks_workflow.json'}")
    print(f"  - {GEN / 'schema_acct.py'}")
    print(f"  - {GEN / 'notebook_acctpost.py'}")
    print(f"  - {len(steps)} JCL steps, {len(fields)} copybook fields mapped")


if __name__ == "__main__":
    main()
