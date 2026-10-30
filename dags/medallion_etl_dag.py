"""Airflow DAG orchestrating the same bronze -> silver -> gold steps.

The tasks call the exact same modules as `python -m src.run_pipeline`,
so the scheduled pipeline and the local run can never drift apart.
(Definition only - a running Airflow server is not required for the
local demo; drop this file into a DAGs folder to schedule it.)
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
DATA = PROJECT_ROOT / "data"

from src import bronze, generate_data, gold, silver  # noqa: E402


def _generate():
    generate_data.write_raw(generate_data.generate_events(), DATA / "raw")


def _bronze():
    bronze.ingest(DATA / "raw", DATA / "bronze" / "events.parquet")


def _silver():
    import pandas as pd
    df = pd.read_parquet(DATA / "bronze" / "events.parquet")
    silver.clean(df, DATA / "silver" / "events.parquet",
                 DATA / "silver" / "quarantine.parquet")


def _gold():
    import pandas as pd
    df = pd.read_parquet(DATA / "silver" / "events.parquet")
    gold.build_gold(df, DATA / "gold" / "daily_campaign_kpis.parquet")


with DAG(
    dag_id="medallion_etl_pipeline",
    description="Bronze -> Silver -> Gold medallion ETL for advertiser data",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 1,
                  "retry_delay": timedelta(minutes=5)},
    tags=["medallion", "etl", "data-quality"],
) as dag:
    t_generate = PythonOperator(task_id="generate_source_data", python_callable=_generate)
    t_bronze = PythonOperator(task_id="bronze_ingest", python_callable=_bronze)
    t_silver = PythonOperator(task_id="silver_cleanse_and_dq", python_callable=_silver)
    t_gold = PythonOperator(task_id="gold_kpi_aggregates", python_callable=_gold)

    t_generate >> t_bronze >> t_silver >> t_gold
