"""IPUMS CPS extract client and processing.

This module submits and downloads an IPUMS CPS Basic Monthly extract via the
IPUMS Microdata Extract API, then writes a parquet file to data/raw/cps.parquet.

Docs:
- IPUMS Microdata Extract API: https://developer.ipums.org/docs/microdata_extracts_api/
- CPS variables:
  - ABSENT:   https://cps.ipums.org/cps-action/variables/ABSENT
  - WHYABSNT: https://cps.ipums.org/cps-action/variables/WHYABSNT
  - NCHILD:   https://cps.ipums.org/cps-action/variables/NCHILD
  - NCHLT5:   https://cps.ipums.org/cps-action/variables/NCHLT5
  - MOMLOC:   https://cps.ipums.org/cps-action/variables/MOMLOC
  - POPLOC:   https://cps.ipums.org/cps-action/variables/POPLOC
  - EMPSTAT:  https://cps.ipums.org/cps-action/variables/EMPSTAT

Note: Running this requires an IPUMS account and API key
saved as IPUMS_API_KEY in a .env file at the repo root.

The API will queue an extract; we poll until it is ready and then download
as parquet. For CI and unit tests, we DO NOT run this network step.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

IPUMS_API_BASE = "https://api.ipums.org"
PROJECT = "cps"

# Variables to request
IPUMS_VARS = [
    "YEAR",
    "MONTH",
    "SERIAL",
    "CPSID",
    "STATEFIP",
    "AGE",
    "SEX",
    "EDUC",
    "EMPSTAT",
    "ABSENT",
    "WHYABSNT",
    "UHRSWORKT",
    "CLASSWKR",
    "IND",
    "OCC",
    "NCHILD",
    "NCHLT5",
    "MOMLOC",
    "POPLOC",
]


@dataclass
class IpumsCpsClient:
    api_key: str
    out_dir: Path = Path("data/raw")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}

    def _build_extract(self) -> Dict:
        """Build an extract request JSON for CPS Basic Monthly, 1994–present.

        IMPORTANT: IPUMS' CPS API expects either a list of sample IDs
        or constraints specifying YEAR/MONTH. The simplest approach is
        to request all months with a YEAR lower bound and MONTH all.
        """
        years = list(range(1994, pd.Timestamp.today().year + 1))
        extract = {
            "description": "Parents vs non-parents own-illness absences, 1994+",
            "dataFormat": "parquet",
            "samples": {"years": years, "months": list(range(1, 13))},
            "variables": [{"name": v} for v in IPUMS_VARS],
            # optional case selection: limit to age 25-49 to reduce size server-side
            "caseSelect": {
                "AGE": {"min": 25, "max": 49},
            },
        }
        return extract

    def submit_extract(self) -> Dict:
        url = f"{IPUMS_API_BASE}/extracts/{PROJECT}"
        resp = requests.post(url, json=self._build_extract(), headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def poll_extract(self, extract_number: int, wait_s: int = 10, timeout_s: int = 36000) -> Dict:
        url = f"{IPUMS_API_BASE}/extracts/{PROJECT}/{extract_number}"
        start = time.time()
        while True:
            r = requests.get(url, headers=self._headers(), timeout=30)
            r.raise_for_status()
            js = r.json()
            status = js.get("status")
            if status == "completed":
                return js
            if status in {"failed", "canceled"}:
                raise RuntimeError(f"Extract {extract_number} status={status}")
            if time.time() - start > timeout_s:
                raise TimeoutError(f"Polling timed out for extract {extract_number}")
            time.sleep(wait_s)

    def download_extract(self, extract_number: int) -> Path:
        url = f"{IPUMS_API_BASE}/extracts/{PROJECT}/{extract_number}/files"
        r = requests.get(url, headers=self._headers(), timeout=60)
        r.raise_for_status()
        files = r.json().get("files", [])
        if not files:
            raise RuntimeError("No files listed for completed extract")
        # Pick first file (single parquet .zip)
        dl_url = files[0]["downloadUrl"]
        self.out_dir.mkdir(parents=True, exist_ok=True)
        zip_path = self.out_dir / f"ipums_cps_{extract_number}.zip"
        with requests.get(dl_url, headers=self._headers(), stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
        return zip_path

    def load_parquet_from_zip(self, zip_path: Path) -> pd.DataFrame:
        import zipfile

        with zipfile.ZipFile(zip_path) as zf:
            # Find parquet file inside
            members = [n for n in zf.namelist() if n.endswith(".parquet")]
            if not members:
                raise RuntimeError("No parquet in downloaded ZIP")
            with zf.open(members[0]) as f:
                return pd.read_parquet(f)

    # --- Processing helpers ---
    @staticmethod
    def minimal_recode(df: pd.DataFrame) -> pd.DataFrame:
        """Minimal recodes: types and own-illness flag.

        Own illness: ABSENT indicates has job but not at work; WHYABSNT maps
        to own illness/injury/medical codes in CPS. IPUMS codebook values for
        WHYABSNT define which codes are own illness/injury. Commonly code 10 (own illness).
        """
        for c in ["YEAR", "MONTH", "STATEFIP", "AGE", "SEX", "EDUC", "EMPSTAT"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        # boolean parent helper
        for c in ["NCHILD", "NCHLT5", "MOMLOC", "POPLOC", "ABSENT", "WHYABSNT"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        # own illness: ABSENT==1 and WHYABSNT == own illness (commonly 10)
        own_ill_codes = {10}
        df["own_ill_absent"] = (
            (df["ABSENT"].fillna(0) == 1) & (df["WHYABSNT"].isin(list(own_ill_codes)))
        ).astype(int)
        return df


def main() -> None:
    load_dotenv()
    api_key = os.getenv("IPUMS_API_KEY")
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    if not api_key:
        raise SystemExit("Missing IPUMS_API_KEY in .env")

    client = IpumsCpsClient(api_key=api_key, out_dir=out_dir)
    # Submit, poll, download, load, minimal recode, save
    submit = client.submit_extract()
    extract_number = submit.get("number") or submit.get("extractNumber")
    if not extract_number:
        raise RuntimeError(f"Unexpected submit response: {submit}")
    client.poll_extract(int(extract_number))
    zip_path = client.download_extract(int(extract_number))
    df = client.load_parquet_from_zip(zip_path)
    df = client.minimal_recode(df)
    out_path = Path("data/raw/cps.parquet")
    df.to_parquet(out_path)
    print(f"Wrote {out_path} with {len(df):,} rows")


if __name__ == "__main__":
    main()

