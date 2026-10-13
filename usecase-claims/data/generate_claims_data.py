"""Generate synthetic claims extracts (CSV) for the claims use case.

  data/members.csv  -> from VSAM MEMBMAST (KSDS)
  data/claims.csv   -> from HLTH.CLAIM.DAILY (sequential)

Run:  python usecase-claims/data/generate_claims_data.py
"""
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

DATA = Path(__file__).resolve().parent
PLANS = ["GOLD", "SILVER", "BRONZE"]
SERVICE_CODES = ["MED", "SUR", "LAB", "COS", "ALT"]   # COS/ALT are not covered


def gen_members(path: Path, n: int = 300, seed: int = 21) -> None:
    rng = random.Random(seed)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["MEMBER_ID", "MEMBER_NAME", "PLAN_TYPE", "DEDUCTIBLE_REM", "PLAN_YEAR"])
        for i in range(1, n + 1):
            w.writerow([
                f"M{i:07d}",
                f"MEMBER {i}",
                rng.choice(PLANS),
                round(rng.choice([0, 250, 500, 1000, 1500]) * rng.random(), 2),
                "2024",
            ])


def gen_claims(path: Path, n_members: int = 300, n: int = 2500, seed: int = 22) -> None:
    rng = random.Random(seed)
    start = date(2024, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["CLAIM_ID", "MEMBER_ID", "SERVICE_CODE", "CLAIM_AMOUNT", "SERVICE_DATE"])
        for cid in range(1, n + 1):
            d = start + timedelta(days=rng.randrange(90))
            w.writerow([
                f"C{cid:09d}",
                f"M{rng.randint(1, n_members):07d}",
                rng.choice(SERVICE_CODES),
                round(rng.uniform(20, 8000), 2),
                d.strftime("%Y%m%d"),
            ])


if __name__ == "__main__":
    gen_members(DATA / "members.csv")
    gen_claims(DATA / "claims.csv")
    print("wrote", DATA / "members.csv")
    print("wrote", DATA / "claims.csv")
