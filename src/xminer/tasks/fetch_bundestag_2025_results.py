#!/usr/bin/env env python3
"""
Fetch and load Bundestag 2025 election results into the database.

Data source: https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata/btw25/csv/

This script downloads CSV files containing:
- kerg.csv: Overall results from all areas (absolute values)
- kerg2.csv: Overall results in flat form (absolute + relative values + differences)

Usage:
    python -m xminer.tasks.fetch_bundestag_2025_results
"""

import logging
import sys
import json
from pathlib import Path
from typing import Optional
import requests
import pandas as pd
from sqlalchemy import text, bindparam

# Add project root to path
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root / "src"))

from xminer.io.db import engine
from xminer.config.params import _get

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Data source URLs
BASE_URL = "https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata/btw25/csv/"
KERG_URL = f"{BASE_URL}kerg.csv"
KERG2_URL = f"{BASE_URL}kerg2.csv"


def download_csv(url: str, output_path: Path) -> bool:
    """
    Download CSV file from URL.

    Args:
        url: URL to download from
        output_path: Path to save the file

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)

        logger.info(f"✅ Downloaded to {output_path}")
        return True

    except requests.RequestException as e:
        logger.error(f"❌ Failed to download {url}: {e}")
        return False


def load_kerg_to_db(csv_path: Path, engine_conn) -> int:
    """
    Load kerg.csv data into database.

    Args:
        csv_path: Path to kerg.csv file
        engine_conn: Database engine connection

    Returns:
        Number of rows inserted
    """
    logger.info(f"Loading kerg.csv from {csv_path}...")

    # Read CSV with proper encoding, skipping metadata rows
    # The actual data starts at row 8 (0-indexed, so skiprows=8)
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', sep=';', skiprows=8, low_memory=False)
    except UnicodeDecodeError:
        logger.info("UTF-8 failed, trying ISO-8859-1 encoding...")
        df = pd.read_csv(csv_path, encoding='iso-8859-1', sep=';', skiprows=8, low_memory=False)

    logger.info(f"Loaded {len(df)} rows with columns: {list(df.columns)[:10]}... (showing first 10)")

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bundestag_2025_kerg (
        id BIGSERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT unique_kerg_row UNIQUE (data)
    );

    CREATE INDEX IF NOT EXISTS idx_bundestag_2025_kerg_data
        ON bundestag_2025_kerg USING GIN (data);
    """

    with engine_conn.connect() as conn:
        conn.execute(text(create_table_query))
        conn.commit()
        logger.info("✅ Table bundestag_2025_kerg created/verified")

    # Insert data as JSONB (allows flexible schema)
    rows_inserted = 0
    with engine_conn.connect() as conn:
        for idx, row in df.iterrows():
            row_dict = row.to_dict()

            # Convert to JSON-serializable format
            for key, value in row_dict.items():
                if pd.isna(value):
                    row_dict[key] = None
                elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                    row_dict[key] = str(value)

            insert_query = text("""
                INSERT INTO bundestag_2025_kerg (data)
                VALUES (CAST(:data AS jsonb))
                ON CONFLICT (data) DO NOTHING
            """)

            result = conn.execute(insert_query, {"data": json.dumps(row_dict)})
            if result.rowcount > 0:
                rows_inserted += 1

        conn.commit()

    logger.info(f"✅ Inserted {rows_inserted} new rows into bundestag_2025_kerg")
    return rows_inserted


def load_kerg2_to_db(csv_path: Path, engine_conn) -> int:
    """
    Load kerg2.csv data into database.

    Args:
        csv_path: Path to kerg2.csv file
        engine_conn: Database engine connection

    Returns:
        Number of rows inserted
    """
    logger.info(f"Loading kerg2.csv from {csv_path}...")

    # Read CSV with proper encoding, skipping metadata rows
    # The actual data starts at row 9 (0-indexed, so skiprows=9)
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', sep=';', skiprows=9, low_memory=False)
    except UnicodeDecodeError:
        logger.info("UTF-8 failed, trying ISO-8859-1 encoding...")
        df = pd.read_csv(csv_path, encoding='iso-8859-1', sep=';', skiprows=9, low_memory=False)

    logger.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bundestag_2025_kerg2 (
        id BIGSERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT unique_kerg2_row UNIQUE (data)
    );

    CREATE INDEX IF NOT EXISTS idx_bundestag_2025_kerg2_data
        ON bundestag_2025_kerg2 USING GIN (data);
    """

    with engine_conn.connect() as conn:
        conn.execute(text(create_table_query))
        conn.commit()
        logger.info("✅ Table bundestag_2025_kerg2 created/verified")

    # Insert data as JSONB
    rows_inserted = 0
    with engine_conn.connect() as conn:
        for idx, row in df.iterrows():
            row_dict = row.to_dict()

            # Convert to JSON-serializable format
            for key, value in row_dict.items():
                if pd.isna(value):
                    row_dict[key] = None
                elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                    row_dict[key] = str(value)

            insert_query = text("""
                INSERT INTO bundestag_2025_kerg2 (data)
                VALUES (CAST(:data AS jsonb))
                ON CONFLICT (data) DO NOTHING
            """)

            result = conn.execute(insert_query, {"data": json.dumps(row_dict)})
            if result.rowcount > 0:
                rows_inserted += 1

        conn.commit()

    logger.info(f"✅ Inserted {rows_inserted} new rows into bundestag_2025_kerg2")
    return rows_inserted


def main():
    """Main execution function."""
    logger.info("="*80)
    logger.info("BUNDESTAG 2025 ELECTION RESULTS FETCHER")
    logger.info("="*80)

    # Get data directory from config or use default
    data_dir = Path(_get('bundestag_2025_results.data_dir', default='data/bundestag_2025'))
    data_dir.mkdir(parents=True, exist_ok=True)

    kerg_path = data_dir / "kerg.csv"
    kerg2_path = data_dir / "kerg2.csv"

    # Download files
    logger.info("\n📥 Downloading CSV files...")
    kerg_success = download_csv(KERG_URL, kerg_path)
    kerg2_success = download_csv(KERG2_URL, kerg2_path)

    if not kerg_success and not kerg2_success:
        logger.error("❌ Failed to download any files. Exiting.")
        return 1

    # Load to database
    logger.info("\n💾 Loading data to database...")

    total_rows = 0
    if kerg_success and kerg_path.exists():
        try:
            rows = load_kerg_to_db(kerg_path, engine)
            total_rows += rows
        except Exception as e:
            logger.error(f"❌ Failed to load kerg.csv: {e}", exc_info=True)

    if kerg2_success and kerg2_path.exists():
        try:
            rows = load_kerg2_to_db(kerg2_path, engine)
            total_rows += rows
        except Exception as e:
            logger.error(f"❌ Failed to load kerg2.csv: {e}", exc_info=True)

    # Summary
    logger.info("\n" + "="*80)
    logger.info("SUMMARY")
    logger.info("="*80)
    logger.info(f"✅ Total rows inserted: {total_rows}")
    logger.info(f"📁 CSV files saved to: {data_dir}")
    logger.info(f"🗄️  Database tables: bundestag_2025_kerg, bundestag_2025_kerg2")
    logger.info("="*80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
