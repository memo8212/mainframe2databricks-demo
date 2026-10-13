# Use Case — Health Claims Adjudication (mainframe → Databricks)

A second, independent legacy application to show the migration pattern on a
different workload: **nightly claims adjudication**.

## The legacy application

```
JCL CLAIMS_BATCH
  STEP010  SORT      claims by CLAIM-ID
  STEP020  CLAIMADJ  coverage + stateful deductible + coinsurance (VSAM MEMBMAST)
  STEP030  CLAIMRPT  paid totals + denial rate
DB2 HLTHDB.MEMBER / CLAIM / CLAIM_ADJUDICATED / SERVICE_CODE
```

**Business rules migrated:**

1. **Coverage** — `MED`, `SUR`, `LAB` are covered; `COS`, `ALT` are denied.
2. **Deductible** — applied per member, **in claim order** (stateful accumulator
   stored in the VSAM member master).
3. **Coinsurance** — plan pays GOLD 90% / SILVER 80% / BRONZE 70% of the
   post-deductible amount.
4. **Status** — `APPROVED` / `NO-PAY` (deductible consumed it) / `DENIED`.
5. **Member accumulator** updated with the deductible consumed.

## Mapping

| Mainframe | Databricks |
|---|---|
| JCL 3-step job | Workflow (tasks + `depends_on`) |
| COBOL `CLAIMADJ` (READ/REWRITE loop) | PySpark notebook with a `Window` |
| VSAM **MEMBMAST** (KSDS) | Delta table + business key + `MERGE` |
| Sequential `CLAIM.DAILY` | Parquet → Bronze |
| DB2 tables | Delta tables |
| `SORT` | `orderBy` |
| stateful deductible (sequential) | cumulative `sum(...) OVER (PARTITION BY member ORDER BY claim)` |

## Layout

```
legacy/        JCL, COBOL (CLAIMADJ), COPYBOOKs (claim + member), DB2 DDL
converter/     convert_claims.py  -> workflow JSON, schemas, notebook, report
runner/        run_claims.py      -> legacy (pandas) vs Databricks (PySpark) parity
data/          generate_claims_data.py -> members.csv, claims.csv
artifacts/     generated/ (committed) + output/ (git-ignored)
```

## Run

```bash
python usecase-claims/data/generate_claims_data.py
python usecase-claims/converter/convert_claims.py
python usecase-claims/runner/run_claims.py
```

## The interesting part: migrating stateful logic

COBOL processes claims **sequentially** and mutates the member's
`DEDUCTIBLE-REM` as it goes — order matters. The distributed equivalent is a
windowed running total, which reproduces the same result without a loop:

```sql
-- deductible available for this claim =
--   remaining deductible − covered amount of earlier claims for the member
SELECT claim_id,
       greatest(0, ded_cents - sum(cov_cents)
                OVER (PARTITION BY member_id ORDER BY claim_id
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS available
```

Then `ded_applied = least(cov_cents, available)`, and coinsurance is applied to
the remainder. The runner proves the windowed version matches the sequential
legacy version **exactly** (max paid diff `0.0`).

## Result (sample run)

- 2,500 claims adjudicated, 300 members, ~39% denied (uncovered codes).
- Claim status, paid amount, and member deductibles all match: **PASS**.
