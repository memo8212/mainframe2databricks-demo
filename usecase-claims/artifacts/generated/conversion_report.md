# Conversion Report — NIGHTLY_CLAIMS_ADJUDICATION

Claims adjudication batch -> Databricks. Automatic mapping summary.

## Component mapping

| Mainframe component | Source | Databricks target |
|---|---|---|
| JCL job stream | `legacy/jcl/CLAIMS_BATCH.jcl` | Databricks Workflow |
| COBOL program | `legacy/cobol/CLAIMADJ.cbl` | PySpark notebook |
| COPYBOOK (claim) | `legacy/copybook/CLAIMREC.cpy` | StructType |
| COPYBOOK (member) | `legacy/copybook/MEMBREC.cpy` | StructType |
| VSAM KSDS (member) | `MEMBMAST` | Delta table + MERGE |
| Sequential claim file | `HLTH.CLAIM.DAILY` | Parquet -> Bronze |
| DB2 tables | `legacy/db2/CLAIMS_DDL.sql` | Delta tables |
| SORT utility | JCL STEP010 | `orderBy` |

## JCL -> Workflow tasks

Job: **CLAIMJOB**

| JCL step | Program | Databricks task | Depends on |
|---|---|---|---|
| STEP010 | SORT | `sort_claims` (orderBy(CLAIM_ID)) | - |
| STEP020 | CLAIMADJ | `adjudicate_claims` (coverage + deductible + coinsurance) | sort_claims |
| STEP030 | CLAIMRPT | `claims_report` (paid totals + denial rate -> Gold) | adjudicate_claims |

## COPYBOOK -> schema

| Copybook | Field | PIC | Spark type |
|---|---|---|---|
| CLAIMREC | claim_id | X(10) | StringType() |
| CLAIMREC | member_id | X(08) | StringType() |
| CLAIMREC | service_code | X(03) | StringType() |
| CLAIMREC | claim_amount | S9(9)V99 | DecimalType(13, 2) |
| CLAIMREC | service_date | 9(08) | IntegerType() |
| MEMBREC | member_id | X(08) | StringType() |
| MEMBREC | member_name | X(30) | StringType() |
| MEMBREC | plan_type | X(06) | StringType() |
| MEMBREC | deductible_rem | S9(7)V99 | DecimalType(13, 2) |
| MEMBREC | plan_year | 9(04) | IntegerType() |

## COBOL statements -> Spark (CLAIMADJ.cbl)

| COBOL construct | Count | Databricks equivalent |
|---|---|---|
| READ | 4 | point lookup (join / MERGE match on key) |
| REWRITE | 1 | Delta MERGE UPDATE |
| WRITE | 2 | Delta write / append |
| COMPUTE | 6 | withColumn arithmetic |
| IF/EVALUATE | 10 | when/otherwise |
| PERFORM | 3 | loop / helper function |

## DB2 -> Delta

- table `HLTHDB.MEMBER` -> Delta `member`
- table `HLTHDB.CLAIM` -> Delta `claim`
- table `HLTHDB.CLAIM_ADJUDICATED` -> Delta `claim_adjudicated`
- table `HLTHDB.SERVICE_CODE` -> Delta `service_code`

## Business logic migrated
- Coverage check (`SERVICE-CODE` in {MED,SUR,LAB}); COS/ALT denied.
- Stateful deductible per member (ordered by CLAIM-ID).
- Coinsurance by plan (GOLD 90% / SILVER 80% / BRONZE 70%).
- Status: APPROVED / NO-PAY / DENIED; member accumulator updated via MERGE.