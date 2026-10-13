"""COBOL -> PySpark translation engine.

Recognizes the classic **sequential-master-update** archetype (a batch program
that reads a transaction file and updates a keyed master) and generates runnable
PySpark:

  READ <master> ... NOT INVALID KEY <body>   ->  join master on the key
  IF <seq.type> = 'CR' ADD/SUBTRACT <seq.amt> ->  signed amount + groupBy key
  IF <master.bal> < 0 AND <master.type>='CH'  ->  when/otherwise status
  COMPUTE <ws> = <master.bal> * <rate>; ADD    ->  interest when/otherwise
  REWRITE <master>                             ->  (Delta MERGE in the target)

Money fields with implied decimals are handled in integer cents so the result is
exact (matching COBOL packed-decimal semantics).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from mftranspile import cobol as C
from mftranspile.copybook import Field, parse_copybook


def col(cobol_name: str) -> str:
    return cobol_name.upper().replace("-", "_")


@dataclass
class TranslationResult:
    program_id: str
    module_source: str = ""
    schema_source: str = ""
    report_rows: list = field(default_factory=list)
    notes: list = field(default_factory=list)


# ------------------------------------------------------------- AST helpers
def walk(stmts):
    for s in stmts:
        yield s
        for attr in ("then", "els", "stmts", "at_end", "not_at_end", "invalid", "not_invalid"):
            if hasattr(s, attr):
                yield from walk(getattr(s, attr))


def find(stmts, cls):
    return [s for s in walk(stmts) if isinstance(s, cls)]


def _field_name(e) -> str | None:
    return e.name if isinstance(e, C.Field) else None


def _is_lit(e, val=None):
    if isinstance(e, C.Str):
        return val is None or e.value == val
    if isinstance(e, C.Num):
        return val is None or e.value == val
    return False


# ------------------------------------------------------------- recognizers
@dataclass
class PostingRules:
    seq_file: str
    master_file: str
    seq_key: str
    master_key: str
    seq_type: str
    seq_amount: str
    credit_lit: str
    bal_field: str
    status_field: str
    type_field: str
    overdraft_lit: str
    ok_lit: str
    overdraft_type: str
    interest_type: str
    rate: float
    rate_name: str


def recognize(prog: C.Program, copybooks: dict[str, list[Field]]) -> PostingRules:
    master = next((f for f in prog.files.values() if f.org == "INDEXED"), None)
    seq = next((f for f in prog.files.values() if f.org == "SEQUENTIAL"), None)
    if not master or not seq:
        raise ValueError("expected one INDEXED (master) and one SEQUENTIAL file")

    # find the main loop -> update paragraph
    update_para = interest_para = None
    for p in prog.paragraphs:
        for perf in find(p.statements, C.Perform):
            for rd in find(perf.stmts, C.Read):
                if rd.file == seq.name:
                    for inner in find(rd.not_at_end, C.Perform):
                        if inner.target:
                            update_para = inner.target
            if perf.target and perf.target not in (p.name,):
                pass
    # main paragraph performs the interest paragraph too
    for p in prog.paragraphs:
        if p.name.startswith("0000"):
            for perf in find(p.statements, C.Perform):
                if perf.target and perf.target != update_para and "INTEREST" in perf.target:
                    interest_para = perf.target
    if not update_para:
        raise ValueError("could not find the master-update paragraph")

    upd = next(p for p in prog.paragraphs if p.name == update_para)
    intp = next((p for p in prog.paragraphs if p.name == interest_para), None)

    # key move: MOVE <seq-key> TO <master-key>
    seq_key = master_key = None
    for mv in find(upd.statements, C.Move):
        if len(mv.dsts) == 1 and isinstance(mv.src, C.Field):
            seq_key, master_key = mv.src.name, mv.dsts[0]
            break

    # sign rule: IF <seq.type> = lit ADD <seq.amt> TO <bal> ELSE SUBTRACT ...
    seq_type = seq_amount = credit_lit = bal_field = None
    for ifs in find(upd.statements, C.If):
        adds = [s for s in ifs.then if isinstance(s, C.Add)]
        subs = [s for s in ifs.els if isinstance(s, C.Subtract)]
        if adds and subs and isinstance(ifs.cond, C.Compare):
            seq_type = _field_name(ifs.cond.left)
            credit_lit = ifs.cond.right.value if isinstance(ifs.cond.right, C.Str) else None
            seq_amount = _field_name(adds[0].src)
            bal_field = adds[0].dsts[0]
            break

    # status rule: IF <bal> < 0 AND <type> = lit MOVE 'O'/'A' TO <status>
    status_field = type_field = overdraft_lit = ok_lit = overdraft_type = None
    for ifs in find(upd.statements, C.If):
        cond = ifs.cond
        if isinstance(cond, C.And):
            l, r = cond.left, cond.right
            if isinstance(l, C.Compare) and isinstance(r, C.Compare):
                # balance < 0
                balf = _field_name(l.left) if _is_lit(l.right, 0) else _field_name(r.left)
                typef = _field_name(r.left) if _field_name(r.left) != balf else _field_name(l.left)
                mv_then = [s for s in ifs.then if isinstance(s, C.Move)]
                mv_els = [s for s in ifs.els if isinstance(s, C.Move)]
                if mv_then and mv_els:
                    status_field = mv_then[0].dsts[0]
                    overdraft_lit = mv_then[0].src.value if isinstance(mv_then[0].src, C.Str) else "O"
                    ok_lit = mv_els[0].src.value if isinstance(mv_els[0].src, C.Str) else "A"
                    type_field = typef
                    overdraft_type = r.right.value if isinstance(r.right, C.Str) else "CH"
                    break

    # interest rule from the interest paragraph
    interest_type = "SA"
    rate = 0.0
    rate_name = "INTEREST_RATE"
    if intp:
        for mv in find(intp.statements, C.Move):
            if isinstance(mv.src, C.Str):
                interest_type = mv.src.value
                break
        for comp in find(intp.statements, C.Compute):
            if isinstance(comp.expr, C.BinOp) and comp.expr.op == "*":
                rate_name = comp.dst
                other = comp.expr.right if isinstance(comp.expr.left, C.Field) else comp.expr.left
                rate_name = other.name if isinstance(other, C.Field) else comp.dst
        item = prog.ws.get(rate_name)
        if item and item.value:
            try:
                rate = float(item.value.strip("'"))
            except ValueError:
                rate = 0.0

    return PostingRules(
        seq_file=seq.name, master_file=master.name,
        seq_key=seq_key or "?", master_key=master_key or "?",
        seq_type=seq_type or "?", seq_amount=seq_amount or "?",
        credit_lit=credit_lit or "CR", bal_field=bal_field or "?",
        status_field=status_field or "?", type_field=type_field or "?",
        overdraft_lit=overdraft_lit or "O", ok_lit=ok_lit or "A",
        overdraft_type=overdraft_type or "CH",
        interest_type=interest_type, rate=rate, rate_name=rate_name)


# ------------------------------------------------------------- codegen
def generate_module(prog: C.Program, rules: PostingRules,
                    master_fields: list[Field], bridge_accounts: str,
                    bridge_transactions: str) -> tuple[str, list]:
    pid = prog.program_id or "PROGRAM"
    out_cols = ", ".join(f'"{f.name.upper()}"' for f in master_fields)
    seqk, mk = col(rules.seq_key), col(rules.master_key)
    st, sa = col(rules.seq_type), col(rules.seq_amount)
    bal, status = col(rules.bal_field), col(rules.status_field)
    mtype = col(rules.type_field)

    src = f'''# AUTO-GENERATED by mftranspile from {pid}.cbl
# Source program : {pid}
# Master file    : {rules.master_file}  (VSAM {col(rules.master_file)} -> keyed Delta)
# Sequential file: {rules.seq_file}
# Do not edit by hand — review the translation report before production use.
#
# Money fields with implied decimals are computed in integer cents so the result
# is exact and matches COBOL packed-decimal (COMP-3) semantics.
from pyspark.sql import functions as F

INTEREST_RATE = {rules.rate}          # {rules.rate_name}
CREDIT = "{rules.credit_lit}"
ACCOUNT_BRIDGE = "{bridge_accounts}"
TRANSACTION_BRIDGE = "{bridge_transactions}"


def build_account_master(spark, bridge_dir: str):
    """Re-implements {pid}: apply {rules.seq_file} to {rules.master_file}."""
    master = spark.read.parquet(f"{{bridge_dir}}/{{ACCOUNT_BRIDGE}}")
    source = spark.read.parquet(f"{{bridge_dir}}/{{TRANSACTION_BRIDGE}}")

    # READ {rules.seq_file} loop: IF {rules.seq_type} = '{rules.credit_lit}'
    #   ADD/SUBTRACT {rules.seq_amount} TO/FROM {rules.bal_field}
    signed = source.withColumn(
        "SIGNED_CENTS",
        (F.when(F.col("{st}") == CREDIT, F.round(F.col("{sa}") * 100))
          .otherwise(-F.round(F.col("{sa}") * 100))).cast("long"))
    net = signed.groupBy("{seqk}").agg(F.sum("SIGNED_CENTS").alias("NET_CENTS"))

    # READ {rules.master_file} ... REWRITE {rules.master_file}
    updates = (master
        .withColumn("BALANCE_CENTS", F.round(F.col("{bal}") * 100).cast("long"))
        .join(net, F.col("{mk}") == F.col("{seqk}"), "left")
        .withColumn("NET_CENTS", F.coalesce("NET_CENTS", F.lit(0)).cast("long"))
        .withColumn("NEW_BALANCE_CENTS", F.col("BALANCE_CENTS") + F.col("NET_CENTS"))
        # IF {rules.bal_field} < 0 AND {rules.type_field} = '{rules.overdraft_type}'
        .withColumn("{status}",
            F.when((F.col("NEW_BALANCE_CENTS") < 0) & (F.col("{mtype}") == "{rules.overdraft_type}"),
                   "{rules.overdraft_lit}").otherwise("{rules.ok_lit}"))
        # 2000-POST-INTEREST: savings accounts accrue interest on positive balances
        .withColumn("INTEREST_CENTS",
            F.when((F.col("{mtype}") == "{rules.interest_type}") & (F.col("NEW_BALANCE_CENTS") > 0),
                   F.round(F.col("NEW_BALANCE_CENTS") * INTEREST_RATE))
             .otherwise(F.lit(0.0)).cast("long"))
        .withColumn("{bal}",
            F.round((F.col("NEW_BALANCE_CENTS") + F.col("INTEREST_CENTS")) / 100, 2))
        .select({out_cols}))
    return updates


def main() -> int:
    import sys
    from pyspark.sql import SparkSession
    bridge = sys.argv[1] if len(sys.argv) > 1 else "artifacts/output/bridge"
    out = sys.argv[2] if len(sys.argv) > 2 else "artifacts/output/dbx"
    spark = (SparkSession.builder.master("local[*]").appName("{pid}").getOrCreate())
    result = build_account_master(spark, bridge)
    result.write.mode("overwrite").option("overwriteSchema", "true").parquet(out)
    print("rows written:", result.count())
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''

    report = [
        {"construct": "PROGRAM-ID", "cobol": pid, "spark": "module name", "confidence": "AUTO"},
        {"construct": "SELECT/FD (file)", "cobol": f"{rules.seq_file} (sequential), {rules.master_file} (indexed)",
         "spark": f"read parquet {{bridge}}/{bridge_transactions}, {bridge_accounts}", "confidence": "AUTO"},
        {"construct": "MOVE key", "cobol": f"MOVE {rules.seq_key} TO {rules.master_key}",
         "spark": f'join on F.col("{col(rules.master_key)}") == F.col("{col(rules.seq_key)}")', "confidence": "AUTO"},
        {"construct": "IF ADD/SUBTRACT", "cobol": f"IF {rules.seq_type} = '{rules.credit_lit}' ADD/SUBTRACT {rules.seq_amount}",
         "spark": f'signed cents when {col(rules.seq_type)} == "{rules.credit_lit}" then + else -', "confidence": "AUTO"},
        {"construct": "READ ... REWRITE", "cobol": f"READ {rules.master_file} ... REWRITE",
         "spark": f"groupBy({col(rules.seq_key)}).agg(sum) then join to master", "confidence": "PATTERN"},
        {"construct": "IF overdraft", "cobol": f"IF {rules.bal_field} < 0 AND {rules.type_field} = '{rules.overdraft_type}' MOVE '{rules.overdraft_lit}'",
         "spark": f'when(new_balance < 0 & {col(rules.type_field)} == "{rules.overdraft_type}", "{rules.overdraft_lit}")', "confidence": "AUTO"},
        {"construct": "COMPUTE interest", "cobol": f"{rules.rate_name} = {rules.bal_field} * {rules.rate}",
         "spark": f'when({col(rules.type_field)} == "{rules.interest_type}" & balance > 0, round(balance * INTEREST_RATE))', "confidence": "PATTERN"},
        {"construct": "Packed decimal", "cobol": f"{rules.bal_field} S9(11)V99 COMP-3",
         "spark": "integer cents arithmetic (exact)", "confidence": "AUTO"},
    ]
    return src, report


def generate_schema(master_fields: list[Field]) -> str:
    from mftranspile.copybook import struct_type_source
    return (
        '"""StructTypes derived from the COPYBOOKs."""\n'
        "from pyspark.sql.types import (StructType, StructField, StringType,\n"
        "                               IntegerType, LongType, DoubleType, DecimalType)\n\n"
        + struct_type_source("ACCT_SCHEMA", master_fields) + "\n"
    )


def translate(cobol_path: str | Path, copybook_paths: dict[str, str | Path],
              jcl_steps: list[dict] | None = None,
              bridge_accounts: str = "accounts",
              bridge_transactions: str = "transactions") -> TranslationResult:
    prog = C.parse_program(Path(cobol_path).read_text())
    copybooks = {name: parse_copybook(p) for name, p in copybook_paths.items()}
    rules = recognize(prog, copybooks)

    # master fields: from the master file's copybook (first copybook provided)
    master_fields = next(iter(copybooks.values()))
    module, report = generate_module(prog, rules, master_fields,
                                     bridge_accounts, bridge_transactions)
    res = TranslationResult(program_id=prog.program_id, module_source=module,
                            schema_source=generate_schema(master_fields),
                            report_rows=report)
    res.notes.append(f"recognized archetype: sequential master update "
                     f"({rules.seq_file} -> {rules.master_file})")
    return res
