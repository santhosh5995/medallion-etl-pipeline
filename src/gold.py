"""Gold layer: curated, analytics-ready KPI aggregates.

Loads the silver table into SQLite (stdlib) and executes the SQL model
in sql/gold_daily_campaign_kpis.sql - the same pattern as a dbt model,
without needing a warehouse or extra setup.
"""
import sqlite3
from pathlib import Path

import pandas as pd

SQL_FILE = Path(__file__).resolve().parent.parent / "sql" / "gold_daily_campaign_kpis.sql"


def build_gold(silver: pd.DataFrame, gold_path: str | Path,
               sql_path: str | Path = SQL_FILE) -> pd.DataFrame:
    con = sqlite3.connect(":memory:")
    try:
        silver.to_sql("silver_events", con, index=False)
        gold = pd.read_sql_query(Path(sql_path).read_text(), con)
    finally:
        con.close()
    gold_path = Path(gold_path)
    gold_path.parent.mkdir(parents=True, exist_ok=True)
    gold.to_parquet(gold_path, index=False)
    print(f"[gold] {len(gold):,} campaign-day rows -> {gold_path}")
    return gold
