"""Unit tests for the mftranspile conversion app (no Spark required)."""
from pathlib import Path

from mftranspile import cobol as C
from mftranspile.copybook import parse_copybook, record_length
from mftranspile.translate import recognize, translate

ROOT = Path(__file__).resolve().parents[1]
COBOL = ROOT / "legacy" / "cobol" / "ACCTPOST.cbl"
CPY = ROOT / "legacy" / "copybook" / "ACCTREC.cpy"
JCL = ROOT / "legacy" / "jcl" / "NIGHTLY_BATCH.jcl"


def test_copybook_layout():
    fields = parse_copybook(CPY)
    assert len(fields) == 6
    assert record_length(fields) == 58
    bal = next(f for f in fields if f.name == "acct_balance")
    assert bal.kind == "packed" and bal.length == 7 and bal.decimals == 2


def test_parse_program_structure():
    prog = C.parse_program(COBOL.read_text())
    assert prog.program_id == "ACCTPOST"
    names = {p.name for p in prog.paragraphs}
    assert {"0000-MAIN", "1000-POST-TRANSACTION", "2000-POST-INTEREST"} <= names
    assert any(f.org == "INDEXED" for f in prog.files.values())
    assert any(f.org == "SEQUENTIAL" for f in prog.files.values())


def test_recognize_rules():
    prog = C.parse_program(COBOL.read_text())
    rules = recognize(prog, {"ACCTREC": parse_copybook(CPY)})
    assert rules.seq_key == "TR-ACCT-NO"
    assert rules.master_key == "ACCT-NO"
    assert rules.seq_type == "TR-TYPE"
    assert rules.credit_lit == "CR"
    assert rules.bal_field == "ACCT-BALANCE"
    assert rules.status_field == "ACCT-STATUS"
    assert rules.interest_type == "SA"
    assert abs(rules.rate - 0.05) < 1e-9


def test_translate_emits_module():
    res = translate(COBOL, {"ACCTREC": CPY})
    src = res.module_source
    assert "INTEREST_RATE = 0.05" in src
    assert 'F.col("TR_TYPE") == CREDIT' in src
    assert "ACCT_STATUS" in src
    assert "def build_account_master" in src
    constructs = {r["construct"] for r in res.report_rows}
    assert {"MOVE key", "IF ADD/SUBTRACT", "COMPUTE interest"} <= constructs
    assert all(r["confidence"] in {"AUTO", "PATTERN", "REVIEW"} for r in res.report_rows)


def test_schema_from_copybook():
    res = translate(COBOL, {"ACCTREC": CPY})
    assert "ACCT_SCHEMA" in res.schema_source
    assert "acct_balance" in res.schema_source
