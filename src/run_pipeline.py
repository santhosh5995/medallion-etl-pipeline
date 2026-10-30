"""Run the full medallion pipeline end to end.

    python -m src.run_pipeline

Generates synthetic source data, then builds bronze -> silver -> gold.
"""
from pathlib import Path

import pandas as pd

from src import bronze, generate_data, gold, silver

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def main() -> None:
    print("== 1/4 Generate synthetic source data ==")
    raw_files = generate_data.write_raw(generate_data.generate_events(),
                                        DATA / "raw")
    print(f"Generated {len(raw_files)} daily raw files")

    print("== 2/4 Bronze: raw ingestion ==")
    bronze_df = bronze.ingest(DATA / "raw", DATA / "bronze" / "events.parquet")

    print("== 3/4 Silver: cleanse + data quality ==")
    silver_df, _ = silver.clean(bronze_df,
                                DATA / "silver" / "events.parquet",
                                DATA / "silver" / "quarantine.parquet")

    print("== 4/4 Gold: campaign KPI aggregates ==")
    gold_df = gold.build_gold(silver_df, DATA / "gold" / "daily_campaign_kpis.parquet")

    pd.set_option("display.width", 160)
    print("\nSample gold output (first 8 rows):")
    print(gold_df.head(8).to_string(index=False))
    print(f"\nTotal spend (gold): ${gold_df['spend'].sum():,.2f} | "
          f"Total revenue (gold): ${gold_df['revenue'].sum():,.2f} | "
          f"Blended ROAS: {gold_df['revenue'].sum() / gold_df['spend'].sum():.2f}")


if __name__ == "__main__":
    main()
