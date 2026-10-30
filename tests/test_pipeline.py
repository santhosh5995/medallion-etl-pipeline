"""End-to-end tests for the medallion pipeline (small synthetic dataset)."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import bronze, generate_data, gold, silver  # noqa: E402


@pytest.fixture(scope="module")
def layers(tmp_path_factory):
    """Run generate -> bronze -> silver -> gold once into a temp dir."""
    tmp = tmp_path_factory.mktemp("pipeline")
    generate_data.write_raw(generate_data.generate_events(days=5), tmp / "raw")
    bronze_df = bronze.ingest(tmp / "raw", tmp / "bronze" / "events.parquet")
    silver_df, report = silver.clean(bronze_df,
                                     tmp / "silver" / "events.parquet",
                                     tmp / "silver" / "quarantine.parquet")
    gold_df = gold.build_gold(silver_df, tmp / "gold" / "kpis.parquet")
    return bronze_df, silver_df, report, gold_df


def test_bronze_keeps_raw_values_and_lineage(layers):
    bronze_df, *_ = layers
    assert {"source_file", "ingested_at"} <= set(bronze_df.columns)
    # Untouched in bronze: measures are still raw strings, not yet cast.
    assert not pd.api.types.is_numeric_dtype(bronze_df["spend"])


def test_silver_dedupes(layers):
    bronze_df, silver_df, _, _ = layers
    assert silver_df["event_id"].is_unique
    assert len(silver_df) < len(bronze_df)  # duplicates + bad rows removed


def test_quality_checks_catch_bad_rows(layers):
    _, _, report, _ = layers
    # The generator deliberately injects these defects.
    assert report["null_spend"] > 0
    assert report["negative_impressions"] > 0
    # And none of them survive into silver.
    _, silver_df, _, _ = layers
    assert silver_df["spend"].notna().all()
    assert (silver_df["impressions"] >= 0).all()


def test_gold_reconciles_with_silver(layers):
    _, silver_df, _, gold_df = layers
    assert gold_df["spend"].sum() == pytest.approx(silver_df["spend"].sum(), abs=0.05)
    assert gold_df["conversions"].sum() == silver_df["conversions"].sum()
    assert gold_df["impressions"].sum() == silver_df["impressions"].sum()


def test_gold_grain_and_metrics(layers):
    *_, gold_df = layers
    assert gold_df.duplicated(subset=["event_date", "campaign_id"]).sum() == 0
    assert (gold_df["roas"] >= 0).all()
    assert gold_df["ctr_pct"].between(0, 100).all()
