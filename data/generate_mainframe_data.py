"""Generate synthetic mainframe extracts (CSV) for the demo.

Simulates the output of unloading the VSAM ACCTMAST and the DB2/sequential
TRANSACTION file so the migration pipeline can run locally.

  data/accounts.csv      -> from VSAM ACCTMAST (KSDS)
  data/transactions.csv  -> from PROD.TRANS.DAILY (sequential)

Run:  python data/generate_mainframe_data.py
"""
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

DATA = Path(__file__).resolve().parent

ACCT_TYPES = ["CH", "SA"]           # Checking, Savings
TRAN_TYPES = ["CR", "DR"]           # Credit, Debit


def gen_accounts(path: Path, n: int = 400, seed: int = 11) -> None:
    rng = random.Random(seed)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ACCT_NO", "ACCT_NAME", "ACCT_TYPE",
                    "ACCT_BALANCE", "ACCT_STATUS", "ACCT_LAST_DATE"])
        for i in range(1, n + 1):
            w.writerow([
                f"A{i:09d}",
                f"ACCOUNT HOLDER {i}",
                rng.choice(ACCT_TYPES),
                round(rng.uniform(-500, 25000), 2),
                "A",
                "20240101",
            ])


def gen_transactions(path: Path, n_accounts: int = 400, n: int = 3000, seed: int = 12) -> None:
    rng = random.Random(seed)
    start = date(2024, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["TRAN_ID", "ACCT_NO", "TRAN_TYPE", "TRAN_AMOUNT", "TRAN_DATE"])
        for tid in range(1, n + 1):
            d = start + timedelta(days=rng.randrange(60))
            w.writerow([
                tid,
                f"A{rng.randint(1, n_accounts):09d}",
                rng.choice(TRAN_TYPES),
                round(rng.uniform(1, 2000), 2),
                d.strftime("%Y%m%d"),
            ])


if __name__ == "__main__":
    gen_accounts(DATA / "accounts.csv")
    gen_transactions(DATA / "transactions.csv")
    print("wrote", DATA / "accounts.csv")
    print("wrote", DATA / "transactions.csv")
