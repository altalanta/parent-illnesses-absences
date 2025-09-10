"""Matplotlib plots for absence rates and parent–non-parent gap.

Figures:
- parents vs non-parents monthly own-illness absence rates
- gap (parent minus non-parent) with period bands P1/P2/P3

Saves PNGs and a simple HTML report embedding the images.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def _load_rates() -> pd.DataFrame:
    path = Path("data/processed/cps_absence_rates.parquet")
    if not path.exists():
        raise FileNotFoundError("data/processed/cps_absence_rates.parquet not found")
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["YEAR"].astype(int).astype(str) + "-" + df["MONTH"].astype(int).astype(str) + "-01")
    return df


def _period_bands(ax):
    ax.axvspan(pd.Timestamp("1994-01-01"), pd.Timestamp("2007-12-31"), color="#999999", alpha=0.08)
    ax.axvspan(pd.Timestamp("2008-01-01"), pd.Timestamp("2019-12-31"), color="#999999", alpha=0.05)
    ax.axvspan(pd.Timestamp("2020-01-01"), pd.Timestamp("2035-01-01"), color="#999999", alpha=0.08)


def plot_timeseries(df: pd.DataFrame, out_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(10, 5))
    p = df[df["is_parent"] == 1].sort_values("date")
    np_ = df[df["is_parent"] == 0].sort_values("date")
    ax.plot(p["date"], p["rate"], label="Parents", lw=1.5)
    ax.plot(np_["date"], np_["rate"], label="Non-parents", lw=1.5)
    _period_bands(ax)
    ax.set_title("Own-illness absence rates (25–49, employed)")
    ax.set_ylabel("Rate")
    ax.legend()
    out = out_dir / "parents_vs_nonparents.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def plot_gap(df: pd.DataFrame, out_dir: Path) -> Path:
    wide = df.pivot_table(index="date", columns="is_parent", values="rate")
    wide.columns = ["non_parent" if c == 0 else "parent" for c in wide.columns]
    wide["gap"] = wide["parent"] - wide["non_parent"]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(wide.index, wide["gap"], color="#b0413e", lw=1.5)
    _period_bands(ax)
    ax.set_title("Gap: Parents minus Non-parents (own-illness absence)")
    ax.set_ylabel("Percentage points")
    out = out_dir / "gap_timeseries.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def _write_index_html(out_dir: Path, images: list[Path]) -> None:
    html = ["<html><body><h1>Absence Figures</h1>"]
    for img in images:
        html.append(f"<h3>{img.name}</h3>")
        html.append(f"<img src='{img.name}' style='max-width:100%;' />")
    html.append("</body></html>")
    (out_dir / "index.html").write_text("\n".join(html), encoding="utf-8")


def main() -> None:
    df = _load_rates()
    out = Path("figures")
    out.mkdir(parents=True, exist_ok=True)
    imgs = [plot_timeseries(df, out), plot_gap(df, out)]
    _write_index_html(out, imgs)
    print("Wrote figures/ and figures/index.html")


if __name__ == "__main__":
    main()

