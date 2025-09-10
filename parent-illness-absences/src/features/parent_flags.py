"""Construct parent vs non-parent flags from IPUMS CPS variables.

Docs:
- NCHILD:  https://cps.ipums.org/cps-action/variables/NCHILD
- NCHLT5:  https://cps.ipums.org/cps-action/variables/NCHLT5
- MOMLOC:  https://cps.ipums.org/cps-action/variables/MOMLOC
- POPLOC:  https://cps.ipums.org/cps-action/variables/POPLOC
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def make_parent_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # co-resident child presence
    nch = pd.to_numeric(df.get("NCHILD"), errors="coerce").fillna(0)
    momloc = pd.to_numeric(df.get("MOMLOC"), errors="coerce").fillna(0)
    poploc = pd.to_numeric(df.get("POPLOC"), errors="coerce").fillna(0)
    nclt5 = pd.to_numeric(df.get("NCHLT5"), errors="coerce").fillna(0)

    df["is_parent"] = ((nch > 0) | (momloc > 0) | (poploc > 0)).astype(int)
    df["has_child_u5"] = (nclt5 > 0).astype(int)
    return df


def _aggregate_monthly_rates(df: pd.DataFrame) -> pd.DataFrame:
    # Restrict to civilians 25–49 who are employed
    emp = pd.to_numeric(df.get("EMPSTAT"), errors="coerce").fillna(0)
    age = pd.to_numeric(df.get("AGE"), errors="coerce").fillna(0)
    keep = (emp.isin([10, 12])) & (age.between(25, 49))
    g = df.loc[keep].groupby(["YEAR", "MONTH", "is_parent"], as_index=False)
    out = g["own_ill_absent"].mean().rename(columns={"own_ill_absent": "rate"})
    return out.astype({"YEAR": int, "MONTH": int, "is_parent": int})


def main() -> None:
    raw = Path("data/raw/cps.parquet")
    if not raw.exists():
        print("data/raw/cps.parquet not found; run `make ipums` first.")
        return
    df = pd.read_parquet(raw)
    df = make_parent_flags(df)
    rates = _aggregate_monthly_rates(df)
    out = Path("data/processed")
    out.mkdir(parents=True, exist_ok=True)
    rates.to_parquet(out / "cps_absence_rates.parquet")
    print("Wrote data/processed/cps_absence_rates.parquet")


if __name__ == "__main__":
    main()

