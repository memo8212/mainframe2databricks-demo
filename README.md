# Mainframe -> Databricks Migration POC

End-to-end demo: a classic **mainframe nightly batch** (JCL + COBOL + VSAM + DB2
+ CICS) migrated to **Databricks** (Workflows + PySpark + Delta), with a
legacy-vs-Databricks validation report.

The same shape as the `sas2databricks-demo`, but for mainframe components.

## The legacy application

A nightly **account posting** job:

```
JCL NIGHTLY_BATCH
  STEP010  SORT   transactions by account number
  STEP020  ACCTPOST  apply CR/DR to the account master (VSAM KSDS), overdraft check
  STEP030  IDCAMS REPRO  back up the VSAM file
  STEP040  ACCTRPT   end-of-night report
CICS INQTRAN   online balance inquiry (random read of ACCTMAST)
DB2 ACCTDB.ACCOUNT / TRANSACTION  side-by-side relational copy
```

## Component mapping

| Mainframe | Databricks target |
|---|---|
| JCL job stream (steps, COND, JOB card) | Workflow (job + tasks + `depends_on` + parameters) |
| COBOL program (procedural logic) | PySpark notebook |
| COPYBOOK (record layout) | PySpark `StructType` schema |
| VSAM **KSDS** (keyed, random I/O) | Delta table + business key + `MERGE` upsert |
| VSAM sequential browse | `ORDER BY` / range scan |
| DB2 tables / views | Delta tables / Databricks SQL views |
| CICS transaction | Databricks SQL / serving endpoint |
| SORT utility | `orderBy` |
| IDCAMS REPRO | Delta table copy / time travel |
| Sequential/unloaded files | Parquet bridge -> Auto Loader / Bronze |

## Structure

```
legacy/            mainframe source artifacts
  jcl/NIGHTLY_BATCH.jcl
  cobol/ACCTPOST.cbl, ACCTRPT.cbl, INQTRAN.cbl
  copybook/ACCTREC.cpy
  vsam/ACCTMAST.vsam.txt        (cluster def + record layout)
  db2/ACCT_DDL.sql
converter/mf2dbx_converter.py   legacy -> workflow JSON + schema + notebook + report
runner/run_demo.py              legacy -> parquet bridge -> Databricks -> validation
data/generate_mainframe_data.py synthetic VSAM/DB2 extracts (CSV)
artifacts/                      all generated outputs
```

## Run

```bash
pip install pyspark pyarrow pandas          # or: pip install -r requirements.txt

python data/generate_mainframe_data.py      # synthetic accounts + transactions
python converter/mf2dbx_converter.py        # emit Databricks artifacts + report
python runner/run_demo.py                   # run both sides + validation report
```

## Outputs (in `artifacts/`)

- `generated/conversion_report.md` — component + statement mapping
- `generated/databricks_workflow.json` — JCL job stream as a Workflow DAG
- `generated/schema_acct.py` — COPYBOOK as a PySpark `StructType`
- `generated/notebook_acctpost.py` — ACCTPOST as a PySpark notebook
- `output/validation_report.md` — legacy vs Databricks parity
- `output/legacy/`, `output/dbx/`, `output/bridge/` — results and the Parquet bridge

## What the demo proves

1. **Structure migrates mechanically.** JCL steps become workflow tasks;
   `COND`/ordering become `depends_on`; the JOB card becomes parameters.
2. **Record layouts migrate to schemas.** The COPYBOOK drives the `StructType`.
3. **Procedural logic migrates to declarative transforms.** COBOL
   `READ`/`REWRITE`/`COMPUTE`/`IF` become joins, `withColumn`, `when/otherwise`,
   and a Delta `MERGE`.
4. **Keyed files become Delta tables.** The VSAM KSDS key becomes a business key
   with an idempotent `MERGE` (no more `FILE STATUS` handling).
5. **Parity is provable.** The runner compares the legacy result to the
   Databricks result row-by-row and writes a pass/fail report.

## Notes / scope

- The converter is a **POC**: it parses the common constructs (`EXEC PGM=`,
  `PIC` clauses, key verbs) to demonstrate mapping, not a full COBOL/JCL parser.
  Production tools (e.g. Databricks Labs **Bladebridge** for the broader
  migration toolchain) go further.
- The runner re-implements the COBOL logic in **pandas** (legacy stand-in) and
  **PySpark** (target) so both sides are executable locally without a mainframe.
- EBCDIC/COMP-3 decoding, SMF, and file status handling are out of scope for the
  POC; the bridge uses Parquet (already decoded).

## License

MIT — see `LICENSE`.
