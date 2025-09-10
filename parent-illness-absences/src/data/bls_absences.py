"""Fetch BLS CPS A-46/A-47 tables (own illness) for context.

We scrape HTML tables via pandas.read_html from:
- https://www.bls.gov/cps/cpsaat46.htm
- https://www.bls.gov/cps/cpsaat47.htm

Goal: extract the own-illness absence line items for full-time workers
and produce a national time series CSV saved to data/processed/bls_absences.csv.

Note: These tables are not parent-stratified; use as context only.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd


def _extract_own_illness_from_tables(url: str) -> pd.DataFrame:
    tables = pd.read_html(url)
    # Heuristic: find a table containing "own illness" (case-insensitive)
    matches: List[pd.DataFrame] = []
    for t in tables:
        if any(t.applymap(lambda x: isinstance(x, str) and ("illness" in x.lower())).any()):
            matches.append(t)
    if not matches:
        raise RuntimeError(f"Could not find own-illness table at {url}")
    # Use the first match; light cleanup
    df = matches[0].copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def main() -> None:
    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    a46 = _extract_own_illness_from_tables("https://www.bls.gov/cps/cpsaat46.htm")
    a47 = _extract_own_illness_from_tables("https://www.bls.gov/cps/cpsaat47.htm")
    # Save raw extracts for transparency
    a46.to_csv(out_dir / "bls_a46_raw.csv", index=False)
    a47.to_csv(out_dir / "bls_a47_raw.csv", index=False)
    # Leave detailed parsing to analysts; save concatenated marker file
    both = pd.DataFrame({"source": ["A-46", "A-47"], "rows": [len(a46), len(a47)]})
    both.to_csv(out_dir / "bls_absences.csv", index=False)
    print(f"Wrote {out_dir/'bls_absences.csv'}")


if __name__ == "__main__":
    main()

