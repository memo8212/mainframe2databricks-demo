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
converter/
  mf2dbx_converter.py           legacy -> workflow JSON + schema + notebook + report
  mf_codec.py                   EBCDIC/COMP-3 encode/decode (copybook-driven)
runner/
  run_demo.py                   legacy -> parquet bridge -> Databricks -> validation
  ebcdic_comp3_demo.py          encode CSV -> EBCDIC/COMP-3, decode back, verify
data/generate_mainframe_data.py synthetic VSAM/DB2 extracts (CSV)
notebooks/                      migrated notebooks (used by the Asset Bundle)
resources/                      Asset Bundle resources + uc_setup.sql
databricks.yml                  Databricks Asset Bundle root
usecase-claims/                 second use case: health-claims adjudication
docs/ebcdic-comp3.md            deep dive: EBCDIC, COMP-3, fixed-length records
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

## Decoding the raw bytes (EBCDIC / COMP-3)

Real mainframe files are EBCDIC, fixed-length, with packed-decimal (COMP-3)
numbers — not readable by `spark.read.csv`. This repo ships a copybook-driven
codec and a round-trip demo:

```bash
python converter/mf_codec.py          # field layout, hex dump, round-trip self-test
python runner/ebcdic_comp3_demo.py    # encode CSV -> EBCDIC, decode back, verify
```

See **`docs/ebcdic-comp3.md`** for the full explanation (code pages, zoned vs
packed decimal, sign nibbles, offsets, REDEFINES/OCCURS, and gotchas).

## Deploy to Databricks (Asset Bundle)

The migrated notebooks live in `notebooks/` and are wired into a Workflow that
mirrors the JCL job stream:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run nightly_account_posting -t dev
```

- `databricks.yml` — bundle root (dev/prod targets, `catalog`/`schema` vars)
- `resources/mainframe_job.yml` — job with `post_transactions → backup_vsam →
  build_report → reconcile` tasks (the JCL STEP010–040 mapping)
- `resources/uc_setup.sql` — Unity Catalog catalog/schemas/volume/tables
- `notebooks/01..05` — ingest → post → backup → report → reconcile

## Second use case — health claims adjudication

`usecase-claims/` migrates an independent application (nightly claims
adjudication) to show the same pattern on a different workload, including
**stateful sequential logic** (a member deductible accumulator) mapped to a
distributed window function. See `usecase-claims/README.md`.

```bash
python usecase-claims/data/generate_claims_data.py
python usecase-claims/converter/convert_claims.py
python usecase-claims/runner/run_claims.py
```

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
