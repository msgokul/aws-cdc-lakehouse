"""Profile the downloaded seed datasets.

Day 1 verification step: proves the data landed intact and gives us the
row counts, null rates and duplicate counts we will later use to design
Glue Data Quality rules (Day 5) and to seed *deliberate* defects (Day 3).

Usage:
    python generators/profile_data.py            # profiles data/olist/*.csv
    python generators/profile_data.py data/tlc   # profiles Parquet instead
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]


def summarize(df: pd.DataFrame, name: str) -> dict:
    """Return a profile dict for one table. Pure function -> unit-testable."""
    null_pct = (df.isna().sum() / max(len(df), 1) * 100).round(2)
    worst_null_col = null_pct.idxmax() if len(df.columns) else None
    return {
        "table": name,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "duplicate_rows": int(df.duplicated().sum()),
        "worst_null_col": worst_null_col,
        "worst_null_pct": float(null_pct.max()) if worst_null_col else 0.0,
    }


def profile_dir(data_dir: Path) -> list[dict]:
    profiles = []
    csvs = sorted(data_dir.glob("*.csv"))
    parquets = sorted(data_dir.glob("*.parquet"))

    for path in csvs:
        df = pd.read_csv(path, low_memory=False)
        profiles.append(summarize(df, path.stem))
    for path in parquets:
        df = pd.read_parquet(path)
        profiles.append(summarize(df, path.stem))
    return profiles


def main() -> int:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "data" / "olist"
    if not target.is_absolute():
        target = REPO_ROOT / target
    if not target.exists():
        print(f"ERROR: {target} does not exist. Run scripts/get-data.ps1 first.")
        return 1

    profiles = profile_dir(target)
    if not profiles:
        print(f"ERROR: no .csv or .parquet files found in {target}")
        return 1

    header = f"{'table':<45} {'rows':>10} {'cols':>5} {'dups':>8} {'worst-null col':<30} {'null%':>6}"
    print(header)
    print("-" * len(header))
    for p in profiles:
        print(
            f"{p['table']:<45} {p['rows']:>10,} {p['columns']:>5} "
            f"{p['duplicate_rows']:>8,} {str(p['worst_null_col']):<30} {p['worst_null_pct']:>6.2f}"
        )
    print(
        "\nKeep this output - Day 5's Glue Data Quality rules assert against "
        "these baseline expectations."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
