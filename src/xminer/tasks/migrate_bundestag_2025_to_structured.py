#!/usr/bin/env python3
"""
Migrate Bundestag 2025 election results from JSONB to structured tables.

This script:
1. Reads data from bundestag_2025_kerg and bundestag_2025_kerg2
2. Creates new properly structured tables
3. Migrates all data to the new tables
4. Creates indexes for fast queries

Usage:
    python -m xminer.tasks.migrate_bundestag_2025_to_structured
"""

import logging
import sys
from pathlib import Path
from decimal import Decimal
import pandas as pd
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root / "src"))

from xminer.io.db import engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_german_decimal(value):
    """Convert German decimal format (comma separator) to Python Decimal."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    # Handle string with comma as decimal separator
    value_str = str(value).strip()
    if not value_str:
        return None

    # Replace comma with dot for decimal parsing
    value_str = value_str.replace(',', '.')
    try:
        return Decimal(value_str)
    except:
        return None


def create_structured_tables():
    """Create new structured tables for Bundestag 2025 election results."""

    logger.info("Creating structured tables...")

    create_sql = """
    -- Drop old tables if they exist
    DROP TABLE IF EXISTS bundestag_2025_results CASCADE;

    -- Create main results table with proper columns
    CREATE TABLE bundestag_2025_results (
        id BIGSERIAL PRIMARY KEY,

        -- Election metadata
        wahlart VARCHAR(10) NOT NULL,           -- BT = Bundestagswahl
        wahltag DATE NOT NULL,                  -- Election date

        -- Geographic area
        gebietsart VARCHAR(20) NOT NULL,        -- Bund, Land, Wahlkreis
        gebietsnummer INTEGER NOT NULL,         -- Area number
        gebietsname TEXT NOT NULL,              -- Area name
        ueg_gebietsart VARCHAR(20),             -- Parent area type
        ueg_gebietsnummer INTEGER,              -- Parent area number

        -- Group/Party information
        gruppenart VARCHAR(50) NOT NULL,        -- Partei, System-Gruppe, Einzelbewerber
        gruppenname TEXT NOT NULL,              -- Party name or group name
        gruppenreihenfolge INTEGER,             -- Sort order

        -- Vote type
        stimme SMALLINT,                        -- 1=Erststimme, 2=Zweitstimme, NULL=system data

        -- Current election results
        anzahl BIGINT,                          -- Absolute count
        prozent NUMERIC(10,6),                  -- Percentage

        -- Previous election (2021) for comparison
        vorp_anzahl BIGINT,                     -- Previous absolute count
        vorp_prozent NUMERIC(10,6),             -- Previous percentage
        diff_prozent NUMERIC(10,6),             -- Percentage change
        diff_prozent_pkt NUMERIC(10,6),         -- Percentage point change

        -- Additional info
        bemerkung TEXT,                         -- Remarks
        gewahlt VARCHAR(200),                   -- Elected candidate(s)

        -- Metadata
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        CONSTRAINT unique_election_result UNIQUE (
            gebietsart, gebietsnummer, gruppenart, gruppenname, stimme
        )
    );

    -- Create indexes for common queries
    CREATE INDEX idx_btw2025_gebietsart ON bundestag_2025_results(gebietsart);
    CREATE INDEX idx_btw2025_gebietsnummer ON bundestag_2025_results(gebietsnummer);
    CREATE INDEX idx_btw2025_gruppenart ON bundestag_2025_results(gruppenart);
    CREATE INDEX idx_btw2025_gruppenname ON bundestag_2025_results(gruppenname);
    CREATE INDEX idx_btw2025_stimme ON bundestag_2025_results(stimme);
    CREATE INDEX idx_btw2025_wahltag ON bundestag_2025_results(wahltag);
    CREATE INDEX idx_btw2025_party_lookup ON bundestag_2025_results(gruppenart, gruppenname)
        WHERE gruppenart = 'Partei';

    -- Add comments
    COMMENT ON TABLE bundestag_2025_results IS 'Bundestag 2025 election results - structured table with proper columns';
    COMMENT ON COLUMN bundestag_2025_results.stimme IS '1=Erststimme (direct mandate), 2=Zweitstimme (party list), NULL=system data (turnout, invalid votes, etc.)';
    COMMENT ON COLUMN bundestag_2025_results.gruppenart IS 'Partei=political party, System-Gruppe=system data (turnout, valid votes, etc.), Einzelbewerber=independent candidate';
    """

    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()

    logger.info("✅ Structured tables created")


def migrate_kerg2_data():
    """Migrate data from bundestag_2025_kerg2 JSONB to structured table."""

    logger.info("Migrating kerg2 data...")

    # Read all data from JSONB table
    with engine.connect() as conn:
        result = conn.execute(text("SELECT data FROM bundestag_2025_kerg2"))
        rows = [row[0] for row in result]

    logger.info(f"Found {len(rows)} rows to migrate")

    # Convert to DataFrame
    df = pd.DataFrame(rows)

    # Parse date
    df['wahltag'] = pd.to_datetime(df['Wahltag'], format='%d.%m.%Y')

    # Convert numeric fields with German decimal format
    df['prozent_clean'] = df['Prozent'].apply(parse_german_decimal)
    df['vorp_prozent_clean'] = df['VorpProzent'].apply(parse_german_decimal)
    df['diff_prozent_clean'] = df['DiffProzent'].apply(parse_german_decimal)
    df['diff_prozent_pkt_clean'] = df['DiffProzentPkt'].apply(parse_german_decimal)

    # Prepare data for insertion
    rows_inserted = 0
    batch_size = 1000

    with engine.connect() as conn:
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                try:
                    insert_query = text("""
                        INSERT INTO bundestag_2025_results (
                            wahlart, wahltag, gebietsart, gebietsnummer, gebietsname,
                            ueg_gebietsart, ueg_gebietsnummer,
                            gruppenart, gruppenname, gruppenreihenfolge,
                            stimme, anzahl, prozent,
                            vorp_anzahl, vorp_prozent, diff_prozent, diff_prozent_pkt,
                            bemerkung, gewahlt
                        ) VALUES (
                            :wahlart, :wahltag, :gebietsart, :gebietsnummer, :gebietsname,
                            :ueg_gebietsart, :ueg_gebietsnummer,
                            :gruppenart, :gruppenname, :gruppenreihenfolge,
                            :stimme, :anzahl, :prozent,
                            :vorp_anzahl, :vorp_prozent, :diff_prozent, :diff_prozent_pkt,
                            :bemerkung, :gewahlt
                        )
                        ON CONFLICT (gebietsart, gebietsnummer, gruppenart, gruppenname, stimme)
                        DO NOTHING
                    """)

                    result = conn.execute(insert_query, {
                        'wahlart': row['Wahlart'],
                        'wahltag': row['wahltag'],
                        'gebietsart': row['Gebietsart'],
                        'gebietsnummer': int(row['Gebietsnummer']) if pd.notna(row['Gebietsnummer']) else None,
                        'gebietsname': row['Gebietsname'],
                        'ueg_gebietsart': row['UegGebietsart'] if pd.notna(row['UegGebietsart']) else None,
                        'ueg_gebietsnummer': int(row['UegGebietsnummer']) if pd.notna(row['UegGebietsnummer']) else None,
                        'gruppenart': row['Gruppenart'],
                        'gruppenname': row['Gruppenname'],
                        'gruppenreihenfolge': int(row['Gruppenreihenfolge']) if pd.notna(row['Gruppenreihenfolge']) else None,
                        'stimme': int(row['Stimme']) if pd.notna(row['Stimme']) else None,
                        'anzahl': int(row['Anzahl']) if pd.notna(row['Anzahl']) else None,
                        'prozent': row['prozent_clean'],
                        'vorp_anzahl': int(row['VorpAnzahl']) if pd.notna(row['VorpAnzahl']) else None,
                        'vorp_prozent': row['vorp_prozent_clean'],
                        'diff_prozent': row['diff_prozent_clean'],
                        'diff_prozent_pkt': row['diff_prozent_pkt_clean'],
                        'bemerkung': row['Bemerkung'] if pd.notna(row['Bemerkung']) else None,
                        'gewahlt': row['Gewählt'] if pd.notna(row['Gewählt']) else None,
                    })

                    if result.rowcount > 0:
                        rows_inserted += 1

                except Exception as e:
                    logger.warning(f"Failed to insert row: {e}")
                    continue

            conn.commit()
            logger.info(f"Progress: {min(i+batch_size, len(df))}/{len(df)} rows processed, {rows_inserted} inserted")

    logger.info(f"✅ Migrated {rows_inserted} rows from kerg2")
    return rows_inserted


def create_summary_views():
    """Create convenient views for common queries."""

    logger.info("Creating summary views...")

    views_sql = """
    -- View: Party results at federal level (Zweitstimme)
    CREATE OR REPLACE VIEW btw2025_bundesergebnis AS
    SELECT
        gruppenname as partei,
        anzahl as stimmen,
        prozent as prozent,
        vorp_anzahl as stimmen_2021,
        vorp_prozent as prozent_2021,
        diff_prozent_pkt as veraenderung_pkt
    FROM bundestag_2025_results
    WHERE gebietsart = 'Bund'
      AND gruppenart = 'Partei'
      AND stimme = 2
    ORDER BY anzahl DESC;

    -- View: Voter turnout by state
    CREATE OR REPLACE VIEW btw2025_wahlbeteiligung AS
    SELECT
        gebietsname as bundesland,
        MAX(CASE WHEN gruppenname = 'Wahlberechtigte' THEN anzahl END) as wahlberechtigte,
        MAX(CASE WHEN gruppenname = 'Wählende' THEN anzahl END) as waehlende,
        MAX(CASE WHEN gruppenname = 'Wählende' THEN prozent END) as wahlbeteiligung_prozent
    FROM bundestag_2025_results
    WHERE gebietsart IN ('Bund', 'Land')
      AND gruppenart = 'System-Gruppe'
      AND gruppenname IN ('Wahlberechtigte', 'Wählende')
    GROUP BY gebietsname
    ORDER BY gebietsname;

    -- View: Party results by state (Zweitstimme)
    CREATE OR REPLACE VIEW btw2025_laenderergebnisse AS
    SELECT
        gebietsname as bundesland,
        gruppenname as partei,
        anzahl as stimmen,
        prozent as prozent,
        diff_prozent_pkt as veraenderung_pkt
    FROM bundestag_2025_results
    WHERE gebietsart = 'Land'
      AND gruppenart = 'Partei'
      AND stimme = 2
    ORDER BY gebietsname, anzahl DESC;

    COMMENT ON VIEW btw2025_bundesergebnis IS 'Federal election results (Zweitstimme) with 2021 comparison';
    COMMENT ON VIEW btw2025_wahlbeteiligung IS 'Voter turnout by federal state';
    COMMENT ON VIEW btw2025_laenderergebnisse IS 'Party results by federal state (Zweitstimme)';
    """

    with engine.connect() as conn:
        conn.execute(text(views_sql))
        conn.commit()

    logger.info("✅ Created summary views")


def show_sample_queries():
    """Show sample data from the new structured table."""

    logger.info("\n" + "="*80)
    logger.info("SAMPLE QUERIES")
    logger.info("="*80)

    with engine.connect() as conn:
        # Federal results
        logger.info("\n1. Federal results (Zweitstimme):")
        result = conn.execute(text("SELECT * FROM btw2025_bundesergebnis LIMIT 10"))
        for row in result:
            logger.info(f"   {row.partei}: {row.stimmen:,} votes ({row.prozent}%), Change: {row.veraenderung_pkt} pts")

        # Voter turnout
        logger.info("\n2. Voter turnout:")
        result = conn.execute(text("SELECT * FROM btw2025_wahlbeteiligung WHERE bundesland = 'Bundesgebiet'"))
        for row in result:
            logger.info(f"   Eligible: {row.wahlberechtigte:,}, Voted: {row.waehlende:,} ({row.wahlbeteiligung_prozent}%)")

        # Table stats
        result = conn.execute(text("SELECT COUNT(*) FROM bundestag_2025_results"))
        total_rows = result.scalar()
        logger.info(f"\n3. Total rows in bundestag_2025_results: {total_rows:,}")


def main():
    """Main execution function."""
    logger.info("="*80)
    logger.info("BUNDESTAG 2025 DATA MIGRATION TO STRUCTURED TABLES")
    logger.info("="*80)

    try:
        # Create new structured tables
        create_structured_tables()

        # Migrate data
        rows_inserted = migrate_kerg2_data()

        # Create views
        create_summary_views()

        # Show samples
        show_sample_queries()

        # Summary
        logger.info("\n" + "="*80)
        logger.info("MIGRATION COMPLETE")
        logger.info("="*80)
        logger.info(f"✅ Migrated {rows_inserted:,} rows")
        logger.info(f"📊 New table: bundestag_2025_results")
        logger.info(f"📈 Created 3 views: btw2025_bundesergebnis, btw2025_wahlbeteiligung, btw2025_laenderergebnisse")
        logger.info("="*80)

        return 0

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
