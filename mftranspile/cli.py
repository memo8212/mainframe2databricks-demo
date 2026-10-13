"""CLI for mftranspile.

    python -m mftranspile convert \
        --cobol legacy/cobol/ACCTPOST.cbl \
        --copybook ACCTREC=legacy/copybook/ACCTREC.cpy \
        --jcl legacy/jcl/NIGHTLY_BATCH.jcl \
        --out build/

    python -m mftranspile verify \
        --module build/post_transactions.py \
        --data data \
        --legacy artifacts/output/legacy/account_master.csv
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from mftranspile.report import build_report
from mftranspile.translate import translate
from mftranspile.verify import verify


def parse_jcl(path: str | Path) -> tuple[str, list[dict]]:
    job, steps = "UNKNOWN", []
    for line in Path(path).read_text().splitlines():
        if line.startswith("//") and " JOB " in line:
            job = line[2:].split()[0]
        m = re.match(r"//([A-Z0-9]+)\s+EXEC\s+PGM=([A-Z0-9]+)", line)
        if m:
            steps.append({"step": m.group(1), "pgm": m.group(2)})
    return job, steps


def _build_workflow(job: str, steps: list[dict]) -> dict:
    tasks = []
    for i, s in enumerate(steps):
        key = s["pgm"].lower()
        tasks.append({
            "task_key": key,
            "description": f"{s['step']} EXEC PGM={s['pgm']} (migrated)",
            "job_cluster_key": "mf_cluster",
            "notebook_task": {"notebook_path": f"/Users/memo/migrated/{key}", "source": "WORKSPACE"},
            "depends_on": [{"task_key": steps[i - 1]["pgm"].lower()}] if i else [],
        })
    return {"name": job, "tasks": tasks,
            "job_cluster": {"job_cluster_key": "mf_cluster",
                            "new_cluster": {"spark_version": "15.4.x-scala2.12",
                                            "node_type_id": "i3.xlarge", "num_workers": 2}},
            "tags": {"generated_by": "mftranspile"}}


def cmd_convert(args) -> int:
    copybooks = {}
    for spec in args.copybook:
        name, _, path = spec.partition("=")
        copybooks[name] = path
    if not copybooks:
        print("error: at least one --copybook NAME=PATH is required", file=sys.stderr)
        return 2

    jcl_steps = []
    job_name = "MIGRATED_JOB"
    if args.jcl:
        job_name, jcl_steps = parse_jcl(args.jcl)

    result = translate(args.cobol, copybooks, jcl_steps,
                       bridge_accounts=args.bridge_accounts,
                       bridge_transactions=args.bridge_transactions)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    module_file = out / "post_transactions.py"
    module_file.write_text(result.module_source, encoding="utf-8")
    (out / "schema.py").write_text(result.schema_source, encoding="utf-8")
    if jcl_steps:
        (out / "workflow.json").write_text(
            json.dumps(_build_workflow(job_name, jcl_steps), indent=2), encoding="utf-8")
    (out / "translation_report.md").write_text(
        build_report(result, str(args.cobol), jcl_steps, job_name), encoding="utf-8")

    print(f"Converted {result.program_id}:")
    for f in sorted(out.iterdir()):
        print(f"  - {f}")
    auto = sum(1 for r in result.report_rows if r["confidence"] == "AUTO")
    pat = sum(1 for r in result.report_rows if r["confidence"] == "PATTERN")
    print(f"  {len(result.report_rows)} constructs mapped ({auto} AUTO, {pat} PATTERN)")
    return 0


def cmd_verify(args) -> int:
    colmap = None
    if args.map:
        colmap = {}
        for pair in args.map.split(","):
            k, _, v = pair.partition("=")
            colmap[k.strip()] = v.strip()
    res = verify(args.module, args.data, args.legacy, out_dir=args.out,
                 column_map=colmap, tol=args.tol)
    print("verify:", "PASS" if res["passed"] else "FAIL")
    print(f"  rows: legacy={res['rows']} generated={res['generated_rows']} "
          f"row_match={res['row_match']}")
    print(f"  checks: {res['checks']}  max_balance_diff={res['max_balance_diff']}")
    return 0 if res["passed"] else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="mftranspile",
                                 description="COBOL -> PySpark conversion app")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("convert", help="translate COBOL/COPYBOOK/JCL to PySpark")
    c.add_argument("--cobol", required=True)
    c.add_argument("--copybook", action="append", default=[],
                   help="NAME=PATH (repeatable)")
    c.add_argument("--jcl", default=None)
    c.add_argument("--out", default="build")
    c.add_argument("--bridge-accounts", default="accounts")
    c.add_argument("--bridge-transactions", default="transactions")
    c.set_defaults(func=cmd_convert)

    v = sub.add_parser("verify", help="run the generated module vs the legacy result")
    v.add_argument("--module", required=True)
    v.add_argument("--data", required=True, help="dir with accounts.csv + transactions.csv")
    v.add_argument("--legacy", required=True, help="legacy account_master.csv")
    v.add_argument("--out", default=None)
    v.add_argument("--map", default=None,
                   help="extract->copybook column map, e.g. ACCT_NO=TR_ACCT_NO,TRAN_TYPE=TR_TYPE")
    v.add_argument("--tol", type=float, default=0.01)
    v.set_defaults(func=cmd_verify)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
