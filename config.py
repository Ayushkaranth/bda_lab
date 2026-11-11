"""Central paths for the whole project."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW = DATA / "raw"            # landing zone: CSV + JSON exactly as 'source systems' produce them
LAKE = DATA / "lake"
BRONZE = LAKE / "bronze"      # raw copy in Parquet (+ ingestion metadata)
SILVER = LAKE / "silver"      # cleaned, typed, partitioned Parquet
WAREHOUSE = DATA / "warehouse.duckdb"   # Gold tables + views used by the dashboard
LOG = DATA / "pipeline_log.json"
