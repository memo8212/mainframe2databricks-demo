"""EBCDIC / COMP-3 round-trip demo.

Simulates the 'VSAM unload' step of a migration:

  1. Take the account master (CSV extract) and ENCODE it into real fixed-length
     EBCDIC records with packed-decimal (COMP-3) balances.
  2. Read the raw bytes back and DECODE them using the COPYBOOK layout.
  3. Prove the decoded rows match the original (this is the decoder you'd run
     before writing the Parquet bridge that Spark ingests).

Run:  python runner/ebcdic_comp3_demo.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from converter.mf_codec import (  # noqa: E402
    parse_copybook, encode_record, decode_record, record_length,
)

DATA = ROOT / "data"
OUT = ROOT / "artifacts" / "output"
OUT.mkdir(parents=True, exist_ok=True)


def load_csv(path: Path) -> list[dict]:
    with open(path) as f:
        return list(csv.DictReader(f))


def main() -> int:
    layout = parse_copybook(ROOT / "legacy" / "copybook" / "ACCTREC.cpy")
    reclen = record_length(layout)
    print(f"copybook: {len(layout)} fields, record length {reclen} bytes")

    rows = load_csv(DATA / "accounts.csv")
    print(f"accounts to encode: {len(rows)}")

    # ---- 1. encode to EBCDIC/COMP-3 ----
    records = []
    for r in rows:
        values = {
            "acct_no": r["ACCT_NO"],
            "acct_name": r["ACCT_NAME"],
            "acct_type": r["ACCT_TYPE"],
            "acct_balance": float(r["ACCT_BALANCE"]),
            "acct_status": r["ACCT_STATUS"],
            "acct_last_date": int(r["ACCT_LAST_DATE"]),
        }
        records.append(encode_record(values, layout))

    raw_path = OUT / "ACCTMAST.ebc"
    raw_path.write_bytes(b"".join(records))
    print(f"wrote {len(records)} records -> {raw_path} "
          f"({raw_path.stat().st_size} bytes = {len(records)} x {reclen})")
    print("first record (hex):", records[0].hex(" "))

    # ---- 2. decode raw bytes back ----
    blob = raw_path.read_bytes()
    decoded = [decode_record(blob[i:i + reclen], layout) for i in range(0, len(blob), reclen)]

    # ---- 3. verify parity with the original CSV ----
    mismatches = 0
    for orig, dec in zip(rows, decoded):
        if (dec["acct_no"] != orig["ACCT_NO"]
                or dec["acct_type"] != orig["ACCT_TYPE"]
                or abs(dec["acct_balance"] - float(orig["ACCT_BALANCE"])) > 0.005):
            mismatches += 1
    print(f"\ndecoded {len(decoded)} records, mismatches: {mismatches}")
    print("sample decoded row:", decoded[0])

    # ---- 4. write the decoded rows as the Parquet bridge ----
    bridge = OUT / "bridge_from_ebcdic"
    bridge.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    df = pd.DataFrame(decoded).rename(columns=str.upper)
    df.to_parquet(bridge / "part-00000.parquet", index=False)
    print(f"parquet bridge written -> {bridge}")

    ok = mismatches == 0 and len(decoded) == len(rows)
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
