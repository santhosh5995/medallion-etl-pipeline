"""Bronze layer: land raw source files untouched, plus lineage metadata.

Bronze is append-only and stores everything as-is (all columns read as
strings) with two added columns: the source file name and the ingestion
timestamp. No cleansing happens here - that is the silver layer's job.
"""
from pathlib import Path

import pandas as pd


def ingest(raw_dir: str | Path, bronze_path: str | Path) -> pd.DataFrame:
    raw_dir, bronze_path = Path(raw_dir), Path(bronze_path)
    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No raw CSV files found in {raw_dir}")
    frames = []
    for f in files:
        df = pd.read_csv(f, dtype=str)  # keep raw values exactly as delivered
        df["source_file"] = f.name
        df["ingested_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        frames.append(df)
    bronze = pd.concat(frames, ignore_index=True)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze.to_parquet(bronze_path, index=False)
    print(f"[bronze] {len(files)} files -> {len(bronze):,} rows -> {bronze_path}")
    return bronze
