"""
Fetch Bundestag member votes from Excel files and import to database.

This script processes Excel files containing voting records from Bundestag
plenary sessions. Each Excel file contains individual member votes for a
specific voting session.

Excel file structure expected:
- Columns: Wahlperiode, Sitzungnr, Abstimmnr, Fraktion/Gruppe, Name, Vorname,
  Titel, ja, nein, Enthaltung, ungültig, nichtabgegeben, Bezeichnung, Bemerkung
- Each row represents one member's vote
"""

import os
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
import glob

import pandas as pd
import requests
from sqlalchemy import text

from ..config.params import Params
from ..io.db import engine

# ---------- Logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=getattr(logging, Params.logging_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", Params.logging_file), mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------- Database Setup ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.bundestag_votes (
    vote_id BIGSERIAL PRIMARY KEY,
    wahlperiode INTEGER NOT NULL,
    sitzungnr INTEGER NOT NULL,
    abstimmnr INTEGER NOT NULL,
    fraktion_gruppe VARCHAR(50),
    name VARCHAR(100) NOT NULL,
    vorname VARCHAR(100),
    titel VARCHAR(50),
    bezeichnung TEXT,
    ja INTEGER NOT NULL DEFAULT 0,
    nein INTEGER NOT NULL DEFAULT 0,
    enthaltung INTEGER NOT NULL DEFAULT 0,
    ungueltig INTEGER NOT NULL DEFAULT 0,
    nichtabgegeben INTEGER NOT NULL DEFAULT 0,
    bemerkung TEXT,
    vote_title TEXT,
    vote_date DATE,
    vote_source_url TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_vote_member UNIQUE (wahlperiode, sitzungnr, abstimmnr, name, vorname)
);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_vote_session
    ON public.bundestag_votes (wahlperiode, sitzungnr, abstimmnr);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_member
    ON public.bundestag_votes (name, vorname);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_fraktion
    ON public.bundestag_votes (fraktion_gruppe);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_retrieved
    ON public.bundestag_votes (retrieved_at);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_title
    ON public.bundestag_votes (vote_title);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_date
    ON public.bundestag_votes (vote_date);
"""

UPSERT_SQL = """
INSERT INTO public.bundestag_votes (
    wahlperiode, sitzungnr, abstimmnr, fraktion_gruppe, name, vorname, titel,
    bezeichnung, ja, nein, enthaltung, ungueltig, nichtabgegeben, bemerkung,
    vote_title, vote_date, vote_source_url, retrieved_at
)
VALUES (
    :wahlperiode, :sitzungnr, :abstimmnr, :fraktion_gruppe, :name, :vorname, :titel,
    :bezeichnung, :ja, :nein, :enthaltung, :ungueltig, :nichtabgegeben, :bemerkung,
    :vote_title, :vote_date, :vote_source_url, :retrieved_at
)
ON CONFLICT (wahlperiode, sitzungnr, abstimmnr, name, vorname)
DO UPDATE SET
    fraktion_gruppe = EXCLUDED.fraktion_gruppe,
    titel = EXCLUDED.titel,
    bezeichnung = EXCLUDED.bezeichnung,
    ja = EXCLUDED.ja,
    nein = EXCLUDED.nein,
    enthaltung = EXCLUDED.enthaltung,
    ungueltig = EXCLUDED.ungueltig,
    nichtabgegeben = EXCLUDED.nichtabgegeben,
    bemerkung = EXCLUDED.bemerkung,
    vote_title = EXCLUDED.vote_title,
    vote_date = EXCLUDED.vote_date,
    vote_source_url = EXCLUDED.vote_source_url,
    retrieved_at = EXCLUDED.retrieved_at;
"""


# ---------- Helper Functions ----------
def ensure_table_exists():
    """Create the bundestag_votes table if it doesn't exist."""
    logger.info("Ensuring bundestag_votes table exists...")
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
    logger.info("Table ready.")


def fetch_vote_metadata_from_bundestag(limit: int = 1000) -> Dict[str, Dict]:
    """
    Fetch vote metadata from Bundestag website listing page.

    Returns a dictionary mapping Excel filenames to metadata:
    {
        "20251219_2_xls.xlsx": {
            "title": "Gesetzentwurf zum Verbrauchervertrags...",
            "date": "2025-12-19",
            "url": "https://www.bundestag.de/resource/blob/..."
        },
        ...
    }
    """
    logger.info("Fetching vote metadata from Bundestag website...")

    base_url = "https://www.bundestag.de"
    ajax_url = f"{base_url}/ajax/filterlist/de/parlament/plenum/abstimmung/liste/462112-462112"

    metadata = {}
    offset = 0
    page_limit = 30  # Bundestag returns 30 items per page

    try:
        while offset < limit:
            # Fetch page
            url = f"{ajax_url}?limit={page_limit}&offset={offset}"
            logger.info(f"Fetching metadata page (offset={offset})...")

            response = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30
            )
            response.raise_for_status()
            html = response.text

            # Parse HTML to extract vote entries
            # Pattern: date, title, and Excel URL
            pattern = r'<p><strong>\s*(\d{2}\.\d{2}\.\d{4}):\s*<span>([^<]+)</span>\s*</strong></p>.*?href="([^"]+\.xlsx)"'
            matches = re.findall(pattern, html, re.DOTALL)

            if not matches:
                logger.info("No more vote entries found")
                break

            # Process matches
            for date_str, title, url in matches:
                filename = url.split('/')[-1]

                # Parse date (DD.MM.YYYY -> YYYY-MM-DD)
                day, month, year = date_str.split('.')
                date_iso = f"{year}-{month}-{day}"

                metadata[filename] = {
                    "title": title.strip(),
                    "date": date_iso,
                    "url": base_url + url if url.startswith('/') else url
                }

            logger.info(f"Extracted {len(matches)} vote entries from this page")

            # Check if there are more pages
            # The meta-slider div contains data-nextoffset
            next_offset_match = re.search(r'data-nextoffset="(\d+)"', html)
            if next_offset_match:
                next_offset = int(next_offset_match.group(1))
                if next_offset <= offset:
                    # No more pages
                    break
                offset = next_offset
            else:
                break

        logger.info(f"Fetched metadata for {len(metadata)} votes from Bundestag website")
        return metadata

    except Exception as e:
        logger.error(f"Error fetching vote metadata: {e}")
        logger.warning("Continuing without metadata...")
        return {}


def find_excel_files(directory: str) -> List[Path]:
    """
    Find all Excel files in the specified directory.

    Args:
        directory: Path to directory containing Excel vote files

    Returns:
        List of Path objects for Excel files
    """
    path = Path(directory)
    if not path.exists():
        logger.error(f"Directory does not exist: {directory}")
        return []

    # Find all .xlsx and .xls files
    excel_files = list(path.glob("*.xlsx")) + list(path.glob("*.xls"))
    logger.info(f"Found {len(excel_files)} Excel files in {directory}")

    return sorted(excel_files)


def parse_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Parse a Bundestag vote Excel file.

    Args:
        file_path: Path to Excel file

    Returns:
        DataFrame with vote data
    """
    logger.info(f"Parsing Excel file: {file_path.name}")

    try:
        # Read the Excel file (usually first sheet)
        df = pd.read_excel(file_path, sheet_name=0)

        # Verify expected columns exist
        expected_cols = [
            'Wahlperiode', 'Sitzungnr', 'Abstimmnr', 'Fraktion/Gruppe',
            'Name', 'Vorname', 'Titel', 'ja', 'nein', 'Enthaltung',
            'ungültig', 'nichtabgegeben', 'Bezeichnung', 'Bemerkung'
        ]

        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in {file_path.name}: {missing_cols}")

        logger.info(f"Loaded {len(df)} vote records from {file_path.name}")
        return df

    except Exception as e:
        logger.error(f"Error parsing {file_path.name}: {e}")
        raise


def transform_to_db_format(
    df: pd.DataFrame,
    vote_metadata: Optional[Dict[str, str]] = None
) -> List[Dict]:
    """
    Transform DataFrame to list of dicts ready for database insertion.

    Args:
        df: DataFrame with vote data
        vote_metadata: Optional dict with keys 'title', 'date', 'url'

    Returns:
        List of dictionaries with database-ready data
    """
    logger.info(f"Transforming {len(df)} records to database format...")

    # Rename columns to match database schema
    df_clean = df.copy()

    # Map Excel columns to database columns
    column_mapping = {
        'Wahlperiode': 'wahlperiode',
        'Sitzungnr': 'sitzungnr',
        'Abstimmnr': 'abstimmnr',
        'Fraktion/Gruppe': 'fraktion_gruppe',
        'Name': 'name',
        'Vorname': 'vorname',
        'Titel': 'titel',
        'ja': 'ja',
        'nein': 'nein',
        'Enthaltung': 'enthaltung',
        'ungültig': 'ungueltig',
        'nichtabgegeben': 'nichtabgegeben',
        'Bezeichnung': 'bezeichnung',
        'Bemerkung': 'bemerkung',
    }

    df_clean = df_clean.rename(columns=column_mapping)

    # Add vote metadata from Bundestag website
    if vote_metadata:
        df_clean['vote_title'] = vote_metadata.get('title')
        df_clean['vote_date'] = vote_metadata.get('date')
        df_clean['vote_source_url'] = vote_metadata.get('url')
    else:
        df_clean['vote_title'] = None
        df_clean['vote_date'] = None
        df_clean['vote_source_url'] = None

    # Add retrieved_at timestamp
    df_clean['retrieved_at'] = datetime.now(timezone.utc)

    # Handle NaN values
    df_clean = df_clean.fillna({
        'titel': None,
        'bemerkung': None,
        'vorname': None,
        'fraktion_gruppe': None,
        'vote_title': None,
        'vote_source_url': None,
    })

    # Convert to list of dicts
    records = df_clean.to_dict('records')

    logger.info(f"Transformed {len(records)} records")
    return records


def upsert_votes(records: List[Dict]) -> int:
    """
    Insert or update vote records in the database.

    Args:
        records: List of vote record dictionaries

    Returns:
        Number of records processed
    """
    if not records:
        logger.warning("No records to insert")
        return 0

    logger.info(f"Upserting {len(records)} vote records to database...")

    try:
        with engine.begin() as conn:
            conn.execute(text(UPSERT_SQL), records)

        logger.info(f"Successfully upserted {len(records)} records")
        return len(records)

    except Exception as e:
        logger.error(f"Error upserting records: {e}")
        raise


def save_to_csv(df: pd.DataFrame, output_path: str):
    """
    Optionally save DataFrame to CSV for inspection.

    Args:
        df: DataFrame to save
        output_path: Path to output CSV file
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved data to {output_path}")


# ---------- Main Execution ----------
def main():
    """Main execution function."""
    logger.info("="*80)
    logger.info("Starting Bundestag votes fetch")
    logger.info("="*80)

    # Configuration from parameters.yml
    excel_dir = Params.bundestag_votes_excel_dir
    load_to_db = Params.bundestag_votes_load_to_db
    store_csv = Params.bundestag_votes_store_csv

    logger.info(f"Configuration:")
    logger.info(f"  Excel directory: {excel_dir}")
    logger.info(f"  Load to database: {load_to_db}")
    logger.info(f"  Store CSV: {store_csv}")

    # Ensure table exists
    if load_to_db:
        ensure_table_exists()

    # Fetch vote metadata from Bundestag website
    logger.info("Fetching vote metadata from Bundestag website...")
    vote_metadata_map = fetch_vote_metadata_from_bundestag(limit=1000)

    # Find Excel files
    excel_files = find_excel_files(excel_dir)

    if not excel_files:
        logger.warning("No Excel files found. Exiting.")
        return

    # Process each Excel file
    total_records = 0
    all_dfs = []

    for file_path in excel_files:
        try:
            # Parse Excel
            df = parse_excel_file(file_path)

            # Get metadata for this file
            file_metadata = vote_metadata_map.get(file_path.name)
            if file_metadata:
                logger.info(f"Found metadata: {file_metadata['title']}")
            else:
                logger.warning(f"No metadata found for {file_path.name}")

            # Transform to database format with metadata
            records = transform_to_db_format(df, file_metadata)

            # Upsert to database
            if load_to_db:
                count = upsert_votes(records)
                total_records += count

            # Collect for CSV export
            if store_csv:
                all_dfs.append(df)

        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")
            continue

    # Save to CSV if requested
    if store_csv and all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        output_path = os.path.join(
            Params.outdir,
            f"bundestag_votes_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        )
        save_to_csv(combined_df, output_path)

    logger.info("="*80)
    logger.info(f"Bundestag votes fetch complete!")
    logger.info(f"  Files processed: {len(excel_files)}")
    logger.info(f"  Total records: {total_records}")
    logger.info("="*80)


if __name__ == "__main__":
    main()
