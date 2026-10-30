"""Silver layer: cleanse bronze data and enforce data-quality rules.

Steps: cast types, normalise campaign names, de-duplicate on event_id,
then run quality checks. Rows that fail a check are quarantined (written
to a separate parquet) instead of silently dropped, and every check
prints a clear PASS/FAIL line.
"""
from pathlib import Path

import pandas as pd

NUMERIC = ["impressions", "clicks", "spend", "conversions", "revenue"]


def cast_types(bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze.copy()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    for col in NUMERIC:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["campaign_name"] = (
        df["campaign_name"].astype(str).str.strip().str.title()
    )
    return df


def quality_report(df: pd.DataFrame) -> dict[str, int]:
    """Return {check_name: number of violating rows} for a cast frame."""
    return {
        "missing_event_id": int(df["event_id"].isna().sum()),
        "invalid_event_date": int(df["event_date"].isna().sum()),
        "null_spend": int(df["spend"].isna().sum()),
        "null_conversions": int(df["conversions"].isna().sum()),
        "negative_impressions": int((df["impressions"] < 0).sum()),
        "negative_spend": int((df["spend"] < 0).sum()),
        "conversions_gt_clicks": int((df["conversions"] > df["clicks"]).sum()),
    }


def bad_row_mask(df: pd.DataFrame) -> pd.Series:
    return (
        df["event_id"].isna()
        | df["event_date"].isna()
        | df["spend"].isna()
        | df["conversions"].isna()
        | (df["impressions"] < 0)
        | (df["spend"] < 0)
        | (df["conversions"] > df["clicks"])
    )


def clean(bronze: pd.DataFrame, silver_path: str | Path,
          quarantine_path: str | Path) -> tuple[pd.DataFrame, dict[str, int]]:
    df = cast_types(bronze)
    before = len(df)
    # De-duplicate: same event delivered twice -> keep first occurrence.
    df = df.drop_duplicates(subset="event_id", keep="first")
    dupes_removed = before - len(df)

    report = quality_report(df)
    bad = bad_row_mask(df)
    quarantine, silver = df[bad], df[~bad].copy()
    # Remaining nulls in non-critical measures are safe to zero-fill.
    silver[["impressions", "clicks", "revenue"]] = (
        silver[["impressions", "clicks", "revenue"]].fillna(0)
    )

    for name, count in report.items():
        status = "PASS" if count == 0 else "FAIL"
        print(f"[silver][dq] {status} {name}: {count} violating row(s)")
    print(f"[silver] duplicates removed: {dupes_removed}, "
          f"quarantined: {len(quarantine)}, clean rows: {len(silver):,}")

    silver_path, quarantine_path = Path(silver_path), Path(quarantine_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    silver.to_parquet(silver_path, index=False)
    quarantine.to_parquet(quarantine_path, index=False)
    return silver, report
