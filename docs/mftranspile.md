# `mftranspile` — the COBOL → PySpark Conversion App

`mftranspile` is the tool that automates what the hand-written converter in this
repo does: it **parses COBOL / COPYBOOK / JCL** and **generates runnable PySpark**
plus a workflow and a translation report — then **verifies** the generated code
against the legacy result.

```
 legacy/*.cbl  *.cpy  *.jcl
        │
        ▼  mftranspile convert
 ┌───────────────────────────────────────────────┐
 │ lexer + parser  (divisions, statements, AST)   │
 │ copybook parser (PIC -> offsets + types)       │
 │ rule engine     (recognize the batch archetype)│
 └───────────────────────────────────────────────┘
        │
        ▼
 build/post_transactions.py   ← runnable PySpark
 build/schema.py              ← StructTypes from COPYBOOKs
 build/workflow.json          ← JCL job stream -> Workflow DAG
 build/translation_report.md  ← per-construct mapping + confidence
        │
        ▼  mftranspile verify   (generated PySpark  vs  legacy pandas result)
      PASS / FAIL  (row + value parity)
```

## Install / run

```bash
# from the repo root
pip install -e .            # exposes the `mftranspile` command (optional)

# 1) convert
python -m mftranspile convert \
  --cobol legacy/cobol/ACCTPOST.cbl \
  --copybook ACCTREC=legacy/copybook/ACCTREC.cpy \
  --jcl legacy/jcl/NIGHTLY_BATCH.jcl \
  --out build/

# 2) produce the legacy result (mainframe stand-in)
python data/generate_mainframe_data.py
python runner/run_demo.py                 # writes artifacts/output/legacy/account_master.csv

# 3) verify the GENERATED code against the legacy result
python -m mftranspile verify \
  --module build/post_transactions.py \
  --data data \
  --legacy artifacts/output/legacy/account_master.csv
# -> verify: PASS   max_balance_diff=0.0
```

## What it recognizes (supported subset)

`mftranspile` targets the **sequential-master-update** archetype (a batch program
that reads a transaction file and updates a keyed master), which is the most
common mainframe ETL shape. Concretely:

| COBOL construct | Generated PySpark |
|---|---|
| `SELECT`/`FD`/`COPY` (files) | read Parquet bridge tables |
| `MOVE <seq-key> TO <master-key>` | `join` on the key |
| `IF <seq.type> = 'CR'` `ADD`/`SUBTRACT` `<amt>` | signed amount + `groupBy(key).agg(sum)` |
| `READ <master> … REWRITE` | join aggregated updates back to the master |
| `IF <bal> < 0 AND <type> = 'CH' MOVE 'O'/'A'` | `when(...).otherwise(...)` status |
| `COMPUTE <ws> = <bal> * <rate>` + `ADD` | `when(type='SA' & bal>0, round(bal*rate))` interest |
| packed decimal (`S9(11)V99 COMP-3`) | integer-cents arithmetic (exact) |
| `SORT` (JCL) | `orderBy` |
| `IDCAMS REPRO` (JCL) | Delta copy / time travel |

### Confidence levels (in the report)

- **AUTO** — translated directly from the statement.
- **PATTERN** — recognized a known archetype; the generated expression should be
  reviewed (e.g. the `READ…REWRITE` loop → aggregate+join, or the interest rule).
- **REVIEW** — needs a human (captured as `Unknown` rather than silently dropped).

Anything the parser doesn't understand becomes an `Unknown` statement and is
surfaced in the report — the tool never pretends to translate what it can't.

## The parser

- **Tokenizer** — fixed-format aware (strips sequence/indicator columns), handles
  `*>` comments, string literals, numbers, relational **and arithmetic** operators.
  COBOL names (including hyphens and names starting with digits, e.g.
  `1000-POST-TRANSACTION`) are one token.
- **Statements** — `IF/ELSE`, `EVALUATE`, `READ` (`AT END`/`INVALID KEY`/`NOT …`),
  `REWRITE`, `WRITE`, `MOVE`, `ADD`, `SUBTRACT`, `MULTIPLY`, `DIVIDE`, `COMPUTE`,
  `PERFORM` (`<para>` and `UNTIL … END-PERFORM`), `OPEN`, `CLOSE`, `START`,
  `DISPLAY`, `STOP RUN`, `EXIT PARAGRAPH/PERFORM`.
- **Conditions** — `AND`/`OR`/`NOT`, relational operators, and COBOL's `NOT =`
  (not-equal) form.
- **Data** — `FILE-CONTROL`, `FILE SECTION` (inline `PIC`s and `COPY`), and
  `WORKING-STORAGE` (`PIC` + `VALUE` constants, used for rates/limits).

## Tests

```bash
pytest -q tests/test_mftranspile.py
```

Covers the copybook layout (58-byte record, 7-byte COMP-3 balance), program
structure, rule recognition (rate `0.05`, keys, literals) and module generation.

## Extending it

Add a recognizer in `mftranspile/translate.py` for a new archetype (e.g. file
merge, control-break report, or a claims-style accumulator) and a row in the
report. The parser already produces the AST; translation is a matter of matching
statement patterns and emitting PySpark.
