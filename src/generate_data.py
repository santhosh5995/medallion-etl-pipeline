"""Generate synthetic advertiser performance data (with realistic dirt).

Each row is one campaign / channel / day performance record:
impressions, clicks, spend, conversions and revenue. A small amount of
messy data (duplicate rows, nulls, negative values, inconsistent campaign
name casing) is injected on purpose so the silver layer has real
cleansing and data-quality work to do.
"""
from pathlib import Path

import numpy as np
import pandas as pd

CAMPAIGNS = {
    "C001": "Spring Sale",
    "C002": "Brand Awareness",
    "C003": "App Installs",
    "C004": "Retargeting Q4",
    "C005": "Product Launch",
}
CHANNELS = ["search", "social", "display", "video"]


def generate_events(days: int = 30, seed: int = 42,
                    end_date: str = "2026-10-29") -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range(end=end_date, periods=days)
    rows, eid = [], 0
    for day in dates:
        for cid, cname in CAMPAIGNS.items():
            for channel in CHANNELS:
                impressions = int(rng.integers(5_000, 120_000))
                clicks = int(impressions * rng.uniform(0.01, 0.08))
                spend = round(clicks * rng.uniform(0.40, 2.50), 2)
                conversions = int(clicks * rng.uniform(0.02, 0.12))
                revenue = round(conversions * rng.uniform(25, 90), 2)
                eid += 1
                rows.append({
                    "event_id": f"E{eid:06d}",
                    "event_date": day.strftime("%Y-%m-%d"),
                    "campaign_id": cid,
                    "campaign_name": cname,
                    "channel": channel,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend": spend,
                    "conversions": conversions,
                    "revenue": revenue,
                })
    df = pd.DataFrame(rows)
    return _inject_dirt(df, rng)


def _inject_dirt(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Add the kinds of defects real source feeds contain."""
    df = df.copy()
    # ~1% exact duplicate deliveries
    dupes = df.sample(frac=0.01, random_state=7)
    df = pd.concat([df, dupes], ignore_index=True)
    # nulls in measures
    idx = rng.choice(df.index, size=12, replace=False)
    df.loc[idx[:6], "spend"] = np.nan
    df.loc[idx[6:], "conversions"] = np.nan
    # a few invalid negative values
    neg = rng.choice(df.index, size=5, replace=False)
    df.loc[neg, "impressions"] = -100
    # inconsistent campaign-name casing / whitespace
    messy = df["campaign_name"] == "Spring Sale"
    df.loc[messy, "campaign_name"] = " spring sale "
    return df.sample(frac=1.0, random_state=3).reset_index(drop=True)


def write_raw(df: pd.DataFrame, raw_dir: str | Path) -> list[Path]:
    """Write one raw CSV per event_date, like a daily source drop."""
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for day, group in df.groupby("event_date"):
        path = raw_dir / f"events_{day}.csv"
        group.to_csv(path, index=False)
        written.append(path)
    return written


if __name__ == "__main__":
    out = Path(__file__).resolve().parent.parent / "data" / "raw"
    files = write_raw(generate_events(), out)
    print(f"Wrote {len(files)} raw files to {out}")
