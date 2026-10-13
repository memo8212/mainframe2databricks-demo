"""COPYBOOK parser: PIC clauses -> typed field layout (+ Spark schema source).

Standalone (no dependency on the rest of the repo) so `mftranspile` can be used
as a library or CLI.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_EBCDIC = "cp037"


@dataclass
class Field:
    name: str            # python-friendly (acct_no)
    cobol_name: str      # ACCT-NO
    pic: str
    kind: str            # char | zoned | packed
    offset: int
    length: int
    digits: int = 0
    decimals: int = 0
    signed: bool = False

    @property
    def spark_type(self) -> str:
        if self.kind == "char":
            return "StringType()"
        if self.kind == "packed":
            return "DecimalType(13, 2)" if self.decimals else "LongType()"
        # zoned / display
        if self.decimals:
            return "DecimalType(13, 2)"
        return "LongType()" if self.digits > 9 else "IntegerType()"


def _pic_parts(pic: str):
    p = pic.upper()
    decimals = 0
    if "V" in p:
        m = re.search(r"V(9+)", p)
        decimals = len(m.group(1)) if m else 0
    signed = p.startswith("S")
    int_digits = sum(int(n) for n in re.findall(r"9\((\d+)\)", p))
    if int_digits == 0:
        int_digits = p.count("9") - decimals
    return p, int_digits + decimals, decimals, signed


def parse_copybook(path: str | Path) -> list[Field]:
    fields: list[Field] = []
    offset = 0
    for line in Path(path).read_text().splitlines():
        if "*>" in line:
            line = line.split("*>")[0]
        m = re.match(r"\s*\d{2}\s+([A-Z0-9-]+)\s+PIC\s+([A-Z0-9()V]+)", line)
        if not m:
            continue
        cobol_name, pic = m.group(1), m.group(2)
        p, digits, decimals, signed = _pic_parts(pic)
        packed = "COMP-3" in line.upper()
        if p.startswith("X"):
            n = int(re.search(r"X\((\d+)\)", p).group(1))
            kind, length = "char", n
        elif packed:
            kind = "packed"
            length = (digits + 1 + 1) // 2
        else:
            kind, length = "zoned", digits
        fields.append(Field(
            name=cobol_name.lower().replace("-", "_"), cobol_name=cobol_name,
            pic=pic, kind=kind, offset=offset, length=length,
            digits=digits, decimals=decimals, signed=signed))
        offset += length
    return fields


def record_length(layout: list[Field]) -> int:
    return sum(f.length for f in layout)


def struct_type_source(var_name: str, layout: list[Field]) -> str:
    lines = [f"{var_name} = StructType(["]
    for f in layout:
        lines.append(f'    StructField("{f.name}", {f.spark_type}, True),')
    lines.append("])")
    return "\n".join(lines)
