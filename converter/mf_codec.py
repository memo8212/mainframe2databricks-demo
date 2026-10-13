"""Mainframe record codec: COPYBOOK-driven EBCDIC / COMP-3 encode & decode.

This is the piece that turns raw VSAM/DB2 byte records into rows a Spark job can
read. It supports the common copybook PIC clauses used by the demo:

  X(n)              EBCDIC character            -> string
  9(n)              zoned (display) decimal     -> integer/long
  S9(n)V99 COMP-3   packed decimal              -> decimal (double here)
  S9(n) COMP-3      packed integer
  9(n) COMP-3       packed unsigned

Key facts
  * EBCDIC is not ASCII: digits/letters use different byte values (code page 037).
  * COMP-3 (packed decimal) stores two decimal digits per byte; the final nibble
    is the sign: C=+, D=-, F=unsigned. `V` marks an *implied* decimal point
    (no byte is stored for it).
  * Record length = sum of field byte lengths; fields are fixed-offset.

Run:  python converter/mf_codec.py        # self-test / round-trip demo
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

EBCDIC = "cp037"


@dataclass
class Field:
    name: str          # python-friendly (acct_no)
    cobol_name: str    # ACCT-NO
    pic: str
    kind: str          # char | zoned | packed
    offset: int
    length: int
    digits: int = 0    # total digits (packed/zoned)
    decimals: int = 0  # implied decimals after V
    signed: bool = False


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
    # total digits include the implied decimal places (V99 adds 2 digits)
    return p, int_digits + decimals, decimals, signed


def parse_copybook(path: str | Path) -> list[Field]:
    """Parse a copybook into an ordered, offset-annotated field layout."""
    fields: list[Field] = []
    offset = 0
    for line in Path(path).read_text().splitlines():
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
            # packed: (digits + 1 sign nibble) / 2 bytes
            kind = "packed"
            length = (digits + 1 + 1) // 2
        else:  # zoned / display
            kind, length = "zoned", digits

        fields.append(Field(
            name=cobol_name.lower().replace("-", "_"), cobol_name=cobol_name,
            pic=pic, kind=kind, offset=offset, length=length,
            digits=digits, decimals=decimals, signed=signed))
        offset += length
    return fields


def record_length(layout: list[Field]) -> int:
    return sum(f.length for f in layout)


# ------------------------------------------------------------------ packed
def _pack(value: float, digits: int, decimals: int, signed: bool) -> bytes:
    scaled = int(round(abs(value) * 10 ** decimals))
    s = str(scaled).zfill(digits)[-digits:]
    nibbles = [int(c) for c in s]
    sign = 0xC if (value >= 0 or not signed) else 0xD
    if not signed:
        sign = 0xF
    nibbles.append(sign)
    if len(nibbles) % 2:               # pad leading zero nibble if needed
        nibbles.insert(0, 0)
    out = bytearray()
    for i in range(0, len(nibbles), 2):
        out.append((nibbles[i] << 4) | nibbles[i + 1])
    return bytes(out)


def _unpack(data: bytes, decimals: int) -> float:
    nibbles: list[int] = []
    for b in data:
        nibbles.append(b >> 4)
        nibbles.append(b & 0x0F)
    sign_nib = nibbles[-1]
    digits = "".join(str(n) for n in nibbles[:-1])
    val = int(digits) / (10 ** decimals)
    return -val if sign_nib == 0x0D else val


# ------------------------------------------------------------------ record
def encode_record(values: dict, layout: list[Field]) -> bytes:
    out = bytearray()
    for f in layout:
        v = values[f.name]
        if f.kind == "char":
            out += str(v).ljust(f.length)[:f.length].encode(EBCDIC)
        elif f.kind == "packed":
            out += _pack(float(v), f.digits, f.decimals, f.signed)
        else:  # zoned / display
            s = str(int(round(float(v) * 10 ** f.decimals))).zfill(f.length)[-f.length:]
            out += s.encode(EBCDIC)
    return bytes(out)


def decode_record(data: bytes, layout: list[Field]) -> dict:
    row = {}
    for f in layout:
        chunk = data[f.offset:f.offset + f.length]
        if f.kind == "char":
            row[f.name] = chunk.decode(EBCDIC).strip()
        elif f.kind == "packed":
            row[f.name] = round(_unpack(chunk, f.decimals), f.decimals)
        else:  # zoned
            digits = chunk.decode(EBCDIC)
            val = int(digits) if digits.isdigit() else 0
            row[f.name] = round(val / (10 ** f.decimals), f.decimals)
    return row


# ------------------------------------------------------------------- self-test
def _self_test() -> None:
    layout = parse_copybook(
        Path(__file__).resolve().parent.parent / "legacy" / "copybook" / "ACCTREC.cpy")
    print("layout:")
    for f in layout:
        print(f"  {f.cobol_name:14s} off={f.offset:3d} len={f.length:2d} "
              f"kind={f.kind:6s} digits={f.digits} dec={f.decimals}")
    print("record length:", record_length(layout), "bytes")

    sample = {
        "acct_no": "A000000042",
        "acct_name": "ACCOUNT HOLDER 42",
        "acct_type": "SA",
        "acct_balance": 12345.67,
        "acct_status": "A",
        "acct_last_date": 20240131,
    }
    raw = encode_record(sample, layout)
    print("\nencoded (hex):", raw.hex(" "))

    back = decode_record(raw, layout)
    print("decoded:", back)
    ok = (back["acct_no"] == sample["acct_no"]
          and back["acct_type"] == sample["acct_type"]
          and abs(back["acct_balance"] - sample["acct_balance"]) < 1e-9
          and back["acct_last_date"] == sample["acct_last_date"])
    print("\nround-trip:", "PASS" if ok else "FAIL")

    # Show the packed balance bytes explicitly
    bal = next(f for f in layout if f.name == "acct_balance")
    packed = raw[bal.offset:bal.offset + bal.length]
    print(f"\nacct_balance packed bytes: {packed.hex(' ')} "
          f"-> nibbles {' '.join(f'{b>>4:x}{b&0xf:x}' for b in packed)} "
          f"(C=+, D=- sign nibble last)")


if __name__ == "__main__":
    _self_test()
