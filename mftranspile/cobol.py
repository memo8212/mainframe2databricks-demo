"""A small COBOL parser (useful subset) used by the transpiler.

Supports the constructs found in batch ETL programs:
  IDENTIFICATION / PROGRAM-ID, FILE-CONTROL (SELECT), FILE SECTION (FD, COPY),
  WORKING-STORAGE (PIC, VALUE), PROCEDURE DIVISION paragraphs and the statements
  OPEN/CLOSE, READ (AT END / INVALID KEY), REWRITE, WRITE, MOVE, ADD, SUBTRACT,
  MULTIPLY, DIVIDE, COMPUTE, IF/ELSE, EVALUATE, PERFORM, START, DISPLAY, STOP RUN,
  EXIT PARAGRAPH/PERFORM.

It is intentionally a *subset* parser: unsupported statements are captured as
`Unknown` (and flagged in the translation report) rather than failing.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------- AST nodes
@dataclass
class Expr:
    pass


@dataclass
class Num(Expr):
    value: float


@dataclass
class Str(Expr):
    value: str


@dataclass
class Field(Expr):
    name: str


@dataclass
class BinOp(Expr):
    op: str
    left: Expr
    right: Expr


@dataclass
class Cond:
    pass


@dataclass
class Compare(Cond):
    left: Expr
    op: str
    right: Expr


@dataclass
class Truth(Cond):
    field: Expr


@dataclass
class And(Cond):
    left: Cond
    right: Cond


@dataclass
class Or(Cond):
    left: Cond
    right: Cond


@dataclass
class Not(Cond):
    inner: Cond


# statements
@dataclass
class Stmt:
    pass


@dataclass
class If(Stmt):
    cond: Cond
    then: list
    els: list = field(default_factory=list)


@dataclass
class Evaluate(Stmt):
    subject: Expr
    whens: list = field(default_factory=list)   # list[(values, stmts)]
    other: list = field(default_factory=list)


@dataclass
class Read(Stmt):
    file: str
    next: bool = False
    at_end: list = field(default_factory=list)
    not_at_end: list = field(default_factory=list)
    invalid: list = field(default_factory=list)
    not_invalid: list = field(default_factory=list)


@dataclass
class Rewrite(Stmt):
    record: str


@dataclass
class Write(Stmt):
    record: str


@dataclass
class Move(Stmt):
    src: Expr
    dsts: list


@dataclass
class Add(Stmt):
    src: Expr
    dsts: list


@dataclass
class Subtract(Stmt):
    src: Expr
    dsts: list


@dataclass
class Compute(Stmt):
    dst: str
    expr: Expr


@dataclass
class Perform(Stmt):
    target: str | None = None
    until: Cond | None = None
    stmts: list = field(default_factory=list)


@dataclass
class OpenClose(Stmt):
    mode: str
    files: list


@dataclass
class Start(Stmt):
    file: str
    rel: str
    field: str


@dataclass
class Display(Stmt):
    items: list


@dataclass
class StopRun(Stmt):
    pass


@dataclass
class Exit(Stmt):
    what: str


@dataclass
class Unknown(Stmt):
    text: str


@dataclass
class FileDef:
    name: str
    assign: str = ""
    org: str = ""
    access: str = ""
    key: str = ""
    record: str = ""
    copybook: str = ""
    fields: list = field(default_factory=list)   # list[(cobol_name, pic, packed)]


@dataclass
class DataItem:
    name: str
    level: int
    pic: str = ""
    value: str = ""


@dataclass
class Paragraph:
    name: str
    statements: list


@dataclass
class Program:
    program_id: str = ""
    files: dict = field(default_factory=dict)
    ws: dict = field(default_factory=dict)
    paragraphs: list = field(default_factory=list)


# --------------------------------------------------------------- tokenizer
TOKEN_RE = re.compile(r"""
    '(?:[^']*)'                       |   # string literal '
    "(?:[^"]*)"                       |   # string literal "
    \d+\.\d+                          |   # decimal number
    [A-Za-z0-9][A-Za-z0-9-]*          |   # word / paragraph name (may start with digit)
    <=|>=|<>|=|<|>                    |   # relational operators
    [+\-*/]                           |   # arithmetic operators (spaced in COBOL)
    [().,]                                # punctuation
""", re.VERBOSE)

_OPS = {"=", "<", ">", "<=", ">=", "<>", "+", "-", "*", "/"}

_NUM_RE = re.compile(r"\d+(\.\d+)?$")


def tokenize(source: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    for raw in source.splitlines():
        line = raw
        if "*>" in line:                       # inline comment
            line = line.split("*>", 1)[0]
        if len(line) > 6:
            if line[6] == "*":                 # fixed-format comment indicator
                continue
            line = line[6:]                    # strip sequence area
        for m in TOKEN_RE.finditer(line):
            tok = m.group(0)
            if tok[0] in "'\"":
                tokens.append(("STR", tok[1:-1]))
            elif _NUM_RE.match(tok):
                tokens.append(("NUM", tok))
            elif tok in ("(", ")", ",", "."):
                tokens.append(("PUNC", tok))
            elif tok in _OPS:
                tokens.append(("OP", tok))
            else:
                tokens.append(("WORD", tok.upper()))
    tokens.append(("EOF", ""))
    return tokens


# --------------------------------------------------------------- parser
STMT_KEYWORDS = {"MOVE", "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "COMPUTE", "IF",
                 "EVALUATE", "READ", "REWRITE", "WRITE", "PERFORM", "OPEN", "CLOSE",
                 "DISPLAY", "STOP", "EXIT", "START", "GOBACK", "CONTINUE", "SET"}

# Tokens that end the operand list of a statement (next statement / scope end).
SCOPE_WORDS = {"ELSE", "END-IF", "END-READ", "END-PERFORM", "END-EVALUATE", "WHEN",
               "AT", "NOT", "INVALID", "END-COMPUTE", "END-MOVE", "END-ADD",
               "END-SUBTRACT", "END-WRITE", "END-OPEN", "END-CLOSE", "END-START",
               "END-DISPLAY", "END-REWRITE"}
BOUNDARY = STMT_KEYWORDS | SCOPE_WORDS


class Parser:
    def __init__(self, tokens):
        self.t = tokens
        self.i = 0

    # -- helpers
    def peek(self, k=0):
        return self.t[min(self.i + k, len(self.t) - 1)]

    def at(self, kind, value=None):
        tk, tv = self.peek()
        return tk == kind and (value is None or tv == value)

    def word_is(self, value):
        tk, tv = self.peek()
        return tk == "WORD" and tv == value

    def next(self):
        tok = self.t[self.i]
        self.i += 1
        return tok

    def expect_period(self):
        if self.at("PUNC", "."):
            self.next()

    def at_boundary(self) -> bool:
        tk, tv = self.peek()
        return tk == "EOF" or (tk == "PUNC" and tv == ".") or (tk == "WORD" and tv in BOUNDARY)

    # -- statements
    def parse_statements(self, stops: set[str]) -> list:
        out = []
        while True:
            tk, tv = self.peek()
            if tk == "EOF":
                break
            if tv == ".":
                self.next()
                continue
            if tk == "WORD" and tv in stops:
                break
            before = self.i
            st = self.parse_statement()
            if st is not None:
                out.append(st)
            if self.i == before:          # no progress guard
                self.next()
        return out

    def parse_statement(self):
        tk, tv = self.peek()
        if tk != "WORD":
            self.next()
            return None
        if tv == "IF":
            return self.parse_if()
        if tv == "EVALUATE":
            return self.parse_evaluate()
        if tv == "READ":
            return self.parse_read()
        if tv in ("REWRITE", "WRITE"):
            return self.parse_write(tv)
        if tv == "MOVE":
            return self.parse_move()
        if tv == "ADD":
            return self.parse_addsub("ADD")
        if tv == "SUBTRACT":
            return self.parse_addsub("SUBTRACT")
        if tv in ("MULTIPLY", "DIVIDE"):
            return self.parse_arith(tv)
        if tv == "COMPUTE":
            return self.parse_compute()
        if tv == "PERFORM":
            return self.parse_perform()
        if tv in ("OPEN", "CLOSE"):
            return self.parse_openclose()
        if tv == "START":
            return self.parse_start()
        if tv == "DISPLAY":
            return self.parse_display()
        if tv == "STOP":
            self.next()
            if self.word_is("RUN"):
                self.next()
            self.expect_period()
            return StopRun()
        if tv == "EXIT":
            self.next()
            what = self.next()[1] if self.peek()[0] == "WORD" else ""
            self.expect_period()
            return Exit(what)
        if tv in ("GOBACK", "CONTINUE"):
            self.next()
            self.expect_period()
            return Exit(tv)
        # unknown: consume to end of sentence
        start = self.i
        while self.peek()[0] != "EOF" and not self.at("PUNC", "."):
            self.next()
        self.expect_period()
        return Unknown(" ".join(v for _, v in self.t[start:self.i]))

    # IF cond [THEN] stmts [ELSE stmts] END-IF | .
    def parse_if(self):
        self.next()  # IF
        cond = self.parse_condition()
        if self.word_is("THEN"):
            self.next()
        then = self.parse_statements({"ELSE", "END-IF"})
        els = []
        if self.word_is("ELSE"):
            self.next()
            els = self.parse_statements({"END-IF"})
        if self.word_is("END-IF"):
            self.next()
            self.expect_period()
        return If(cond, then, els)

    def parse_evaluate(self):
        self.next()  # EVALUATE
        subject = self.parse_expr()
        whens, other = [], []
        while self.word_is("WHEN"):
            self.next()
            if self.word_is("OTHER"):
                self.next()
                other = self.parse_statements({"WHEN", "END-EVALUATE"})
                continue
            values = []
            while not (self.word_is("WHEN") or self.word_is("END-EVALUATE")):
                # collect value list until statement keyword or next WHEN
                values.append(self.parse_expr())
                if self.peek()[0] == "WORD" and self.peek()[1] in STMT_KEYWORDS:
                    break
                if self.word_is("WHEN") or self.word_is("END-EVALUATE"):
                    break
                # 'OR' between values in COBOL uses 'OR' keyword or space
                if self.word_is("OR") or self.word_is("THRU") or self.word_is("THROUGH"):
                    self.next()
            stmts = self.parse_statements({"WHEN", "END-EVALUATE"})
            whens.append((values, stmts))
        if self.word_is("END-EVALUATE"):
            self.next()
            self.expect_period()
        return Evaluate(subject, whens, other)

    def parse_read(self):
        self.next()  # READ
        fname = self.next()[1]
        node = Read(file=fname)
        if self.word_is("NEXT"):
            self.next()
            node.next = True
        while True:
            if self.word_is("AT") and self.peek(1)[1] == "END":
                self.next(); self.next()
                node.at_end = self.parse_statements({"NOT", "END-READ"})
            elif self.word_is("NOT") and self.peek(1)[1] == "AT":
                self.next(); self.next()
                if self.word_is("END"):
                    self.next()
                node.not_at_end = self.parse_statements({"END-READ"})
            elif self.word_is("INVALID") and self.peek(1)[1] == "KEY":
                self.next(); self.next()
                node.invalid = self.parse_statements({"NOT", "END-READ"})
            elif self.word_is("NOT") and self.peek(1)[1] == "INVALID":
                self.next(); self.next()
                if self.word_is("KEY"):
                    self.next()
                node.not_invalid = self.parse_statements({"END-READ"})
            else:
                break
        if self.word_is("END-READ"):
            self.next()
        self.expect_period()
        return node

    def parse_write(self, kw):
        self.next()
        rec = self.next()[1]
        while not self.at_boundary():
            self.next()
        if self.word_is("END-WRITE") or self.word_is("END-REWRITE"):
            self.next()
        self.expect_period()
        return Rewrite(rec) if kw == "REWRITE" else Write(rec)

    def parse_move(self):
        self.next()  # MOVE
        src = self.parse_expr()
        if self.word_is("TO"):
            self.next()
        dsts = []
        while not self.at_boundary():
            dsts.append(self.next()[1])
        if self.word_is("END-MOVE"):
            self.next()
        self.expect_period()
        return Move(src, dsts)

    def parse_addsub(self, kw):
        self.next()
        src = self.parse_expr()
        keyword = "TO" if kw == "ADD" else "FROM"
        if self.word_is(keyword):
            self.next()
        dsts = []
        while not self.at_boundary():
            if self.word_is("GIVING"):
                self.next()
                continue
            tk, tv = self.peek()
            if tk == "WORD":
                dsts.append(tv)
            self.next()
        if self.word_is(f"END-{kw}"):
            self.next()
        self.expect_period()
        return Add(src, dsts) if kw == "ADD" else Subtract(src, dsts)

    def parse_arith(self, kw):
        self.next()
        a = self.parse_expr()
        kw2 = "BY" if kw == "MULTIPLY" else "INTO"
        if self.word_is(kw2):
            self.next()
        b = self.parse_expr()
        while not self.at_boundary():
            self.next()
        self.expect_period()
        op = "*" if kw == "MULTIPLY" else "/"
        return Compute(_as_field_name(b), BinOp(op, a, b))

    def parse_compute(self):
        self.next()  # COMPUTE
        dst = self.next()[1]
        if self.at("OP", "="):
            self.next()
        expr = self.parse_expr()
        while not self.at_boundary():
            self.next()
        if self.word_is("END-COMPUTE"):
            self.next()
        self.expect_period()
        return Compute(dst, expr)

    def parse_perform(self):
        self.next()  # PERFORM
        if self.word_is("UNTIL"):
            self.next()
            cond = self.parse_condition()
            stmts = self.parse_statements({"END-PERFORM"})
            if self.word_is("END-PERFORM"):
                self.next()
            self.expect_period()
            return Perform(until=cond, stmts=stmts)
        # PERFORM <para> [UNTIL cond]
        target = None
        if self.peek()[0] == "WORD" and self.peek()[1] not in ("UNTIL", "VARYING"):
            target = self.next()[1]
        until = None
        if self.word_is("UNTIL"):
            self.next()
            until = self.parse_condition()
        # consume optional VARYING ... clauses (stop at the next statement)
        while not self.at_boundary():
            self.next()
        self.expect_period()
        return Perform(target=target, until=until)

    def parse_openclose(self):
        mode = self.next()[1]  # OPEN / CLOSE
        if self.word_is("INPUT") or self.word_is("OUTPUT") or self.word_is("I-O") or self.word_is("EXTEND"):
            self.next()
        files = []
        while not self.at_boundary():
            files.append(self.next()[1])
        self.expect_period()
        return OpenClose(mode, files)

    def parse_start(self):
        self.next()
        fname = self.next()[1]
        rel = ""
        fld = ""
        if self.word_is("KEY"):
            self.next()
            rel = self.next()[1]  # >=, =, etc
            if self.peek()[0] == "WORD":
                fld = self.next()[1]
        while not self.at_boundary():
            self.next()
        self.expect_period()
        return Start(fname, rel, fld)

    def parse_display(self):
        self.next()
        items = []
        while not self.at_boundary():
            items.append(self.next()[1])
        self.expect_period()
        return Display(items)

    # -- expressions
    def parse_expr(self):
        node = self.parse_term()
        while self.at("OP", "+") or self.at("OP", "-"):
            op = self.next()[1]
            node = BinOp(op, node, self.parse_term())
        return node

    def parse_term(self):
        node = self.parse_factor()
        while self.at("OP", "*") or self.at("OP", "/"):
            op = self.next()[1]
            node = BinOp(op, node, self.parse_factor())
        return node

    def parse_factor(self):
        tk, tv = self.peek()
        if self.at("PUNC", "("):
            self.next()
            node = self.parse_expr()
            if self.at("PUNC", ")"):
                self.next()
            return node
        if tk == "NUM":
            self.next()
            return Num(float(tv))
        if tk == "STR":
            self.next()
            return Str(tv)
        if tk == "WORD":
            self.next()
            return Field(tv)
        self.next()
        return Field(tv)

    # -- conditions
    def parse_condition(self) -> Cond:
        node = self.parse_cond_and()
        while self.word_is("OR"):
            self.next()
            node = Or(node, self.parse_cond_and())
        return node

    def parse_cond_and(self) -> Cond:
        node = self.parse_cond_atom()
        while self.word_is("AND"):
            self.next()
            node = And(node, self.parse_cond_atom())
        return node

    def parse_cond_atom(self) -> Cond:
        if self.word_is("NOT"):
            self.next()
            return Not(self.parse_cond_atom())
        if self.at("PUNC", "("):
            self.next()
            node = self.parse_condition()
            if self.at("PUNC", ")"):
                self.next()
            return node
        left = self.parse_expr()
        # COBOL `NOT =` (i.e. !=): optional NOT before the relational operator
        negated = False
        if self.word_is("NOT") and self.peek(1)[0] == "OP":
            self.next()
            negated = True
        if self.peek()[0] == "OP":
            op = self.next()[1]
            right = self.parse_expr()
            cmp = Compare(left, op, right)
            return Not(cmp) if negated else cmp
        return Truth(left)


def _as_field_name(e: Expr) -> str:
    return e.name if isinstance(e, Field) else "?"


# --------------------------------------------------------------- top level
def _split_sentences(text: str) -> list[str]:
    """Split on sentence-terminating periods (not decimals)."""
    text = re.sub(r"\*>.*", "", text)
    parts = re.split(r"\.(?=\s|$)", text)
    return [p.strip() for p in parts if p.strip()]


def parse_program(source: str, copybook_loader=None) -> Program:
    prog = Program()

    m = re.search(r"PROGRAM-ID\.\s+([A-Za-z0-9-]+)", source)
    if m:
        prog.program_id = m.group(1)

    # FILE-CONTROL: SELECT ... ASSIGN TO ... [ORG] [KEY]
    for sent in _split_sentences(source):
        m = re.search(r"\bSELECT\s+([A-Za-z0-9-]+)\s+ASSIGN\s+TO\s+([A-Za-z0-9-]+)", sent, re.I)
        if m:
            name = m.group(1).upper()
            fd = prog.files.setdefault(name, FileDef(name=name))
            fd.assign = m.group(2).upper()
            om = re.search(r"ORGANIZATION\s+IS\s+([A-Za-z-]+)", sent, re.I)
            if om:
                fd.org = om.group(1).upper()
            am = re.search(r"ACCESS\s+MODE\s+IS\s+([A-Za-z-]+)", sent, re.I)
            if am:
                fd.access = am.group(1).upper()
            km = re.search(r"RECORD\s+KEY\s+IS\s+([A-Za-z0-9-]+)", sent, re.I)
            if km:
                fd.key = km.group(1).upper()

    # FILE SECTION: FD <name>. / COPY <cb>. / 01 <rec>.
    lines = source.splitlines()
    cur_fd = None
    in_file_section = False
    for raw in lines:
        line = raw.split("*>")[0]
        if len(line) > 6:
            line = line[6:]
        s = line.strip().upper()
        if s.startswith("FILE SECTION"):
            in_file_section = True
            continue
        if s.startswith(("WORKING-STORAGE", "LINKAGE", "PROCEDURE", "REPORT", "SCREEN")):
            in_file_section = False
            cur_fd = None
        m = re.match(r"FD\s+([A-Z0-9-]+)", s)
        if m:
            cur_fd = m.group(1)
            prog.files.setdefault(cur_fd, FileDef(name=cur_fd))
            in_file_section = True
            continue
        m = re.match(r"COPY\s+([A-Z0-9-]+)", s)
        if m and cur_fd:
            prog.files[cur_fd].copybook = m.group(1)
            continue
        m = re.match(r"01\s+([A-Z0-9-]+)", s)
        if m and cur_fd and in_file_section:
            prog.files[cur_fd].record = m.group(1)
            continue
        # inline field definitions (level NN name PIC ...)
        m = re.match(r"(\d{2})\s+([A-Z0-9-]+)\s+PIC\s+([A-Z0-9()V]+)", s, re.I)
        if m and cur_fd and in_file_section:
            prog.files[cur_fd].fields.append(
                (m.group(2).upper(), m.group(3).upper(), "COMP-3" in s.upper()))

    # WORKING-STORAGE: level NN name PIC ... [VALUE ...]
    in_ws = False
    for raw in lines:
        line = raw.split("*>")[0]
        if len(line) > 6:
            line = line[6:]
        s = line.strip()
        up = s.upper()
        if up.startswith("WORKING-STORAGE"):
            in_ws = True
            continue
        if in_ws and (up.startswith("LINKAGE") or up.startswith("PROCEDURE")):
            in_ws = False
        if not in_ws:
            continue
        m = re.match(r"(\d{2})\s+([A-Za-z0-9-]+)(?:\s+PIC\s+([A-Za-z0-9()V]+))?"
                     r"(?:\s+VALUE\s+([A-Za-z0-9'.\-]+))?", s, re.I)
        if m:
            name = m.group(2).upper()
            value = (m.group(4) or "").rstrip(".")
            prog.ws[name] = DataItem(
                name=name, level=int(m.group(1)),
                pic=(m.group(3) or "").upper(), value=value)

    # PROCEDURE DIVISION paragraphs
    proc = re.search(r"PROCEDURE\s+DIVISION", source, re.I)
    if proc:
        body = source[proc.end():]
        prog.paragraphs = _parse_paragraphs(body)

    # resolve copybooks into fields (optional)
    if copybook_loader:
        for fd in prog.files.values():
            if fd.copybook:
                fd.fields = copybook_loader(fd.copybook)  # type: ignore[attr-defined]
    return prog


def _parse_paragraphs(body: str) -> list[Paragraph]:
    paras: list[Paragraph] = []
    cur_name, cur_lines = None, []
    for raw in body.splitlines():
        line = raw.split("*>")[0]
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        # paragraph header: NAME. in area A (indent <= 8)
        if re.match(r"^[A-Za-z0-9-]+\.$", stripped) and indent <= 8:
            if cur_name is not None:
                paras.append(_parse_para(cur_name, "\n".join(cur_lines)))
            cur_name, cur_lines = stripped[:-1].upper(), []
            continue
        if cur_name is not None:
            cur_lines.append(line)
    if cur_name is not None:
        paras.append(_parse_para(cur_name, "\n".join(cur_lines)))
    return paras


def _parse_para(name: str, text: str) -> Paragraph:
    tokens = tokenize(text)
    p = Parser(tokens)
    stmts = p.parse_statements(set())
    return Paragraph(name=name, statements=stmts)
