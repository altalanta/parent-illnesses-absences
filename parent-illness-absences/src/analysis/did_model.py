"""Difference-in-Differences models on own-illness absences.

Models:
- (a) Person-month micro GLM (binomial) [requires micro input].
- (b) Month-level OLS on rates by group (parents vs non-parents).

Spec: own_ill_absent_it ~ is_parent_i * (P2_t + P3_t) + C(state) + C(month)
       + covariates [sex, educ, classwkr, industry]

Cluster SEs at state or month level for robustness.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf


def _make_periods(df: pd.DataFrame, year_col: str = "YEAR") -> pd.DataFrame:
    df = df.copy()
    y = pd.to_numeric(df[year_col], errors="coerce")
    df["P1"] = ((y >= 1994) & (y <= 2007)).astype(int)
    df["P2"] = ((y >= 2008) & (y <= 2019)).astype(int)
    df["P3"] = (y >= 2020).astype(int)
    return df


def run_monthlevel_ols(rates: pd.DataFrame) -> str:
    """Run OLS on monthly rates by is_parent, interacting with P2/P3.

    Input columns: YEAR, MONTH, is_parent, rate
    """
    df = _make_periods(rates)
    # Reshape to have parent/non-parent columns to construct gap if needed
    # But DiD spec can be done directly with is_parent * periods
    df["month_id"] = (df["YEAR"].astype(int) * 12 + df["MONTH"].astype(int))
    model = smf.ols(
        formula="rate ~ is_parent * (P2 + P3) + C(MONTH)",
        data=df,
    ).fit(cov_type="cluster", cov_kwds={"groups": df["month_id"]})
    return model.summary().as_text()


def run_person_glm(micro: pd.DataFrame) -> str:
    """Run GLM Binomial on person-month microdata (if available).

    Requires columns: own_ill_absent, is_parent, YEAR, MONTH, STATEFIP
    """
    df = _make_periods(micro)
    df["month_id"] = (df["YEAR"].astype(int) * 12 + df["MONTH"].astype(int))
    model = smf.glm(
        formula="own_ill_absent ~ is_parent * (P2 + P3) + C(STATEFIP) + C(MONTH)",
        data=df,
        family=sm.families.Binomial(),
    ).fit(cov_type="cluster", cov_kwds={"groups": df["STATEFIP"]})
    return model.summary().as_text()


def main() -> None:
    # Prefer month-level OLS using processed file
    proc = Path("data/processed/cps_absence_rates.parquet")
    out_dir = Path("results")
    out_dir.mkdir(parents=True, exist_ok=True)
    if proc.exists():
        rates = pd.read_parquet(proc)
        summary = run_monthlevel_ols(rates)
        (out_dir / "did_summary.txt").write_text(summary)
        print("Wrote results/did_summary.txt")
    else:
        print("Processed rates not found; skipping DiD. Run `make build` after IPUMS.")


if __name__ == "__main__":
    main()

