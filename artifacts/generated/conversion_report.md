# Conversion Report — NIGHTLY_ACCOUNT_POSTING

Mainframe batch job -> Databricks workflow. Automatic mapping summary.

## Component mapping

| Mainframe component | Source | Databricks target |
|---|---|---|
| JCL job stream | `legacy/jcl/NIGHTLY_BATCH.jcl` | Databricks Workflow (job + tasks + deps) |
| COBOL program | `legacy/cobol/ACCTPOST.cbl` | PySpark notebook |
| COPYBOOK | `legacy/copybook/ACCTREC.cpy` | PySpark StructType schema |
| VSAM KSDS | `legacy/vsam/ACCTMAST.vsam.txt` | Delta table + business key + MERGE |
| DB2 tables | `legacy/db2/ACCT_DDL.sql` | Delta tables |
| DB2 views | `legacy/db2/ACCT_DDL.sql` | Databricks SQL views |
| CICS transaction | `legacy/cobol/INQTRAN.cbl` | Databricks SQL / serving endpoint |
| SORT utility | JCL STEP010 | `orderBy` |
| IDCAMS REPRO | JCL STEP030 | Delta table copy / time travel |

## JCL job stream -> Workflow tasks

Job: **NIGHTLY**

| JCL step | Program | Databricks task | Depends on |
|---|---|---|---|
| STEP010 | SORT | `sort_transactions` (orderBy(ACCT_NO) -> sorted staging) | - |
| STEP020 | ACCTPOST | `post_transactions` (apply CR/DR + overdraft + interest (MERGE)) | sort_transactions |
| STEP030 | IDCAMS | `backup_vsam` (Delta table copy / time-travel backup) | post_transactions |
| STEP040 | ACCTRPT | `build_report` (aggregate report -> Gold table) | backup_vsam |

## COPYBOOK -> schema

| Field | PIC | Spark type |
|---|---|---|
| acct_no | X(10) | StringType() |
| acct_name | X(30) | StringType() |
| acct_type | X(02) | StringType() |
| acct_balance | S9(11)V99 | DecimalType(13, 2) |
| acct_status | X(01) | StringType() |
| acct_last_date | 9(08) | IntegerType() |

## COBOL statements -> Spark (ACCTPOST.cbl)

| COBOL construct | Count | Databricks equivalent |
|---|---|---|
| READ | 10 | point lookup (join / MERGE match on key) |
| REWRITE | 2 | Delta MERGE UPDATE |
| COMPUTE | 7 | withColumn arithmetic |
| IF/EVALUATE | 6 | when/otherwise |
| PERFORM | 7 | loop / helper function |

## DB2 -> Delta

- table `ACCTDB.ACCOUNT` -> Delta table `account`
- table `ACCTDB.TRANSACTION` -> Delta table `transaction`
- view `ACCTDB.V_ACCT_BALANCE` -> Databricks SQL view `v_acct_balance`

## Bridge format
VSAM/DB2 are unloaded to Parquet, ingested to Bronze, then MERGEd into Silver/Gold Delta tables. Validation compares legacy vs Databricks output.