from pathlib import Path

import pandas as pd
import pytest


def test_processed_dataset_shape():
    path = Path("data/processed/cps_absence_rates.parquet")
    if not path.exists():
        pytest.skip("Processed CPS dataset not present; run make build after IPUMS")
    df = pd.read_parquet(path)
    # Expect reasonable coverage (300+ months since 1994)
    months = df[["YEAR", "MONTH"]].drop_duplicates().shape[0]
    assert months >= 300
    # Both groups present
    assert set(df["is_parent"].unique()) == {0, 1}
    # Non-null rates
    assert df["rate"].notna().all()

