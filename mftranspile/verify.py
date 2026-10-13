"""Verify a generated module against the legacy (mainframe) result.

Builds the Parquet bridge from the sample extracts (renaming extract columns to
the COPYBOOK field names), runs the generated PySpark, and compares to the
legacy CSV produced by the mainframe stand-in.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

# extract column -> copybook field (normally defined by the unload spec)
DEFAULT_COLUMN_MAP = {
    "ACCT_NO": "TR_ACCT_NO",
    "TRAN_TYPE": "TR_TYPE",
    "TRAN_AMOUNT": "TR_AMOUNT",
}

COMPARE_COLS = ["ACCT_BALANCE", "ACCT_STATUS"]


def _load_module(path: str | Path):
    spec = importlib.util.spec_from_file_location("generated_module", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)          # type: ignore[union-attr]
    return mod


def _spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]")
            .appName("mftranspile-verify")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def verify(module_path: str | Path, data_dir: str | Path, legacy_csv: str | Path,
           out_dir: str | Path | None = None,
           column_map: dict | None = None, tol: float = 0.01) -> dict:
    column_map = column_map or DEFAULT_COLUMN_MAP
    data_dir = Path(data_dir)
    bridge = Path(out_dir) if out_dir else data_dir.parent / "verify_bridge"
    bridge.mkdir(parents=True, exist_ok=True)

    accounts = pd.read_csv(data_dir / "accounts.csv")
    transactions = pd.read_csv(data_dir / "transactions.csv").rename(columns=column_map)
    (bridge / "accounts").mkdir(parents=True, exist_ok=True)
    (bridge / "transactions").mkdir(parents=True, exist_ok=True)
    accounts.to_parquet(bridge / "accounts" / "part-00000.parquet", index=False)
    transactions.to_parquet(bridge / "transactions" / "part-00000.parquet", index=False)

    spark = _spark()
    mod = _load_module(module_path)
    result = mod.build_account_master(spark, str(bridge)).toPandas()
    spark.stop()

    legacy = pd.read_csv(legacy_csv)
    merged = legacy.merge(result, on="ACCT_NO", suffixes=("_MF", "_DBX"), how="outer",
                          indicator=True)
    row_match = bool((merged["_merge"] == "both").all())
    checks = {}
    for c in COMPARE_COLS:
        if c == "ACCT_BALANCE":
            checks[c] = bool(((merged["ACCT_BALANCE_MF"] - merged["ACCT_BALANCE_DBX"]).abs() <= tol).all())
        else:
            checks[c] = bool((merged["ACCT_STATUS_MF"] == merged["ACCT_STATUS_DBX"]).all())
    max_diff = float((merged["ACCT_BALANCE_MF"] - merged["ACCT_BALANCE_DBX"]).abs().max()) if row_match else float("nan")
    passed = row_match and all(checks.values())

    return {
        "passed": passed,
        "rows": int(len(legacy)),
        "row_match": row_match,
        "checks": checks,
        "max_balance_diff": max_diff,
        "generated_rows": int(len(result)),
    }
