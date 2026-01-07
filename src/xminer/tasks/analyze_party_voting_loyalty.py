#!/usr/bin/env python3
"""
Analyze party voting loyalty/cohesion in Bundestag votes.

This script calculates:
- Party majority position on each vote
- Individual dissent rates (voting against party majority)
- Party cohesion scores
- Most loyal and rebellious politicians per party

Usage:
    python -m xminer.tasks.analyze_party_voting_loyalty
"""

import logging
import sys
from pathlib import Path
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


def create_voting_loyalty_views():
    """Create views for party voting loyalty analysis."""

    logger.info("Creating party voting loyalty views...")

    sql = """
    -- Drop existing views
    DROP VIEW IF EXISTS party_rebellious_politicians CASCADE;
    DROP VIEW IF EXISTS politician_voting_loyalty CASCADE;
    DROP VIEW IF EXISTS party_vote_positions CASCADE;
    DROP VIEW IF EXISTS party_voting_cohesion CASCADE;

    -- =============================================================================
    -- VIEW 1: Party majority position on each vote
    -- =============================================================================
    CREATE OR REPLACE VIEW party_vote_positions AS
    SELECT
        vote_title,
        vote_date,
        fraktion_gruppe as party,

        -- Count votes by type
        SUM(ja) as party_yes_votes,
        SUM(nein) as party_no_votes,
        SUM(enthaltung) as party_abstain_votes,
        SUM(nichtabgegeben) as party_absent_votes,

        -- Total participating votes (yes + no + abstain)
        SUM(ja) + SUM(nein) + SUM(enthaltung) as party_participating_votes,

        -- Determine party majority position
        CASE
            WHEN SUM(ja) > SUM(nein) AND SUM(ja) > SUM(enthaltung) THEN 'Ja'
            WHEN SUM(nein) > SUM(ja) AND SUM(nein) > SUM(enthaltung) THEN 'Nein'
            WHEN SUM(enthaltung) > SUM(ja) AND SUM(enthaltung) > SUM(nein) THEN 'Enthaltung'
            ELSE 'Tied'
        END as party_majority_position,

        -- Calculate cohesion percentage (% voting with majority)
        CASE
            WHEN SUM(ja) >= SUM(nein) AND SUM(ja) >= SUM(enthaltung)
                THEN ROUND(100.0 * SUM(ja) / NULLIF(SUM(ja) + SUM(nein) + SUM(enthaltung), 0), 1)
            WHEN SUM(nein) >= SUM(ja) AND SUM(nein) >= SUM(enthaltung)
                THEN ROUND(100.0 * SUM(nein) / NULLIF(SUM(ja) + SUM(nein) + SUM(enthaltung), 0), 1)
            WHEN SUM(enthaltung) >= SUM(ja) AND SUM(enthaltung) >= SUM(nein)
                THEN ROUND(100.0 * SUM(enthaltung) / NULLIF(SUM(ja) + SUM(nein) + SUM(enthaltung), 0), 1)
            ELSE NULL
        END as cohesion_percent

    FROM bundestag_votes
    WHERE fraktion_gruppe IS NOT NULL
        AND fraktion_gruppe != 'Fraktionslos'
    GROUP BY vote_title, vote_date, fraktion_gruppe;

    COMMENT ON VIEW party_vote_positions IS 'Party majority position and cohesion on each Bundestag vote';


    -- =============================================================================
    -- VIEW 2: Individual politician voting loyalty
    -- =============================================================================
    CREATE OR REPLACE VIEW politician_voting_loyalty AS
    SELECT
        bv.vorname,
        bv.name,
        bv.fraktion_gruppe as party,

        -- Count total votes participated
        COUNT(DISTINCT bv.vote_title) as total_votes_participated,

        -- Count votes with/against party majority
        COUNT(DISTINCT CASE
            WHEN (pvp.party_majority_position = 'Ja' AND bv.ja = 1)
                OR (pvp.party_majority_position = 'Nein' AND bv.nein = 1)
                OR (pvp.party_majority_position = 'Enthaltung' AND bv.enthaltung = 1)
            THEN bv.vote_title
        END) as voted_with_party,

        COUNT(DISTINCT CASE
            WHEN (pvp.party_majority_position = 'Ja' AND (bv.nein = 1 OR bv.enthaltung = 1))
                OR (pvp.party_majority_position = 'Nein' AND (bv.ja = 1 OR bv.enthaltung = 1))
                OR (pvp.party_majority_position = 'Enthaltung' AND (bv.ja = 1 OR bv.nein = 1))
            THEN bv.vote_title
        END) as voted_against_party,

        -- Calculate loyalty percentage
        ROUND(100.0 * COUNT(DISTINCT CASE
            WHEN (pvp.party_majority_position = 'Ja' AND bv.ja = 1)
                OR (pvp.party_majority_position = 'Nein' AND bv.nein = 1)
                OR (pvp.party_majority_position = 'Enthaltung' AND bv.enthaltung = 1)
            THEN bv.vote_title
        END) / NULLIF(COUNT(DISTINCT bv.vote_title), 0), 1) as loyalty_percent,

        -- Calculate dissent rate
        ROUND(100.0 * COUNT(DISTINCT CASE
            WHEN (pvp.party_majority_position = 'Ja' AND (bv.nein = 1 OR bv.enthaltung = 1))
                OR (pvp.party_majority_position = 'Nein' AND (bv.ja = 1 OR bv.enthaltung = 1))
                OR (pvp.party_majority_position = 'Enthaltung' AND (bv.ja = 1 OR bv.nein = 1))
            THEN bv.vote_title
        END) / NULLIF(COUNT(DISTINCT bv.vote_title), 0), 1) as dissent_rate

    FROM bundestag_votes bv
    JOIN party_vote_positions pvp
        ON pvp.vote_title = bv.vote_title
        AND pvp.party = bv.fraktion_gruppe
    WHERE bv.fraktion_gruppe IS NOT NULL
        AND bv.fraktion_gruppe != 'Fraktionslos'
        AND (bv.ja = 1 OR bv.nein = 1 OR bv.enthaltung = 1)  -- Only count actual votes
    GROUP BY bv.vorname, bv.name, bv.fraktion_gruppe
    HAVING COUNT(DISTINCT bv.vote_title) >= 5;  -- At least 5 votes to be meaningful

    COMMENT ON VIEW politician_voting_loyalty IS 'Individual politician loyalty to party voting positions';


    -- =============================================================================
    -- VIEW 3: Party overall voting cohesion
    -- =============================================================================
    CREATE OR REPLACE VIEW party_voting_cohesion AS
    SELECT
        party,

        -- Overall statistics
        COUNT(DISTINCT vote_title) as total_votes,
        ROUND(AVG(cohesion_percent), 1) as avg_cohesion_percent,
        ROUND(MIN(cohesion_percent), 1) as min_cohesion_percent,
        ROUND(MAX(cohesion_percent), 1) as max_cohesion_percent,

        -- Count highly cohesive votes (>90%)
        COUNT(DISTINCT CASE WHEN cohesion_percent >= 90 THEN vote_title END) as votes_high_cohesion,

        -- Count split votes (<60% cohesion)
        COUNT(DISTINCT CASE WHEN cohesion_percent < 60 THEN vote_title END) as votes_split,

        -- Percentage of votes with high cohesion
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN cohesion_percent >= 90 THEN vote_title END)
            / NULLIF(COUNT(DISTINCT vote_title), 0), 1) as high_cohesion_rate

    FROM party_vote_positions
    GROUP BY party
    ORDER BY avg_cohesion_percent DESC;

    COMMENT ON VIEW party_voting_cohesion IS 'Overall party voting cohesion statistics';


    -- =============================================================================
    -- VIEW 4: Most rebellious politicians by party
    -- =============================================================================
    CREATE OR REPLACE VIEW party_rebellious_politicians AS
    SELECT
        party,
        vorname,
        name,
        total_votes_participated,
        voted_against_party,
        dissent_rate,
        loyalty_percent,
        ROW_NUMBER() OVER (PARTITION BY party ORDER BY dissent_rate DESC) as dissent_rank
    FROM politician_voting_loyalty
    WHERE total_votes_participated >= 10  -- At least 10 votes
    ORDER BY party, dissent_rate DESC;

    COMMENT ON VIEW party_rebellious_politicians IS 'Politicians ranked by dissent rate within their party';
    """

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    logger.info("✅ Created 4 voting loyalty views")


def generate_insights():
    """Generate insights from voting loyalty analysis."""

    logger.info("\n" + "="*80)
    logger.info("PARTY VOTING LOYALTY ANALYSIS")
    logger.info("="*80)

    with engine.connect() as conn:

        # Insight 1: Party cohesion comparison
        logger.info("\n📊 INSIGHT 1: Party Voting Cohesion")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                party,
                total_votes,
                avg_cohesion_percent,
                high_cohesion_rate,
                votes_split
            FROM party_voting_cohesion
            ORDER BY avg_cohesion_percent DESC
        """))

        print("\nPartei    |Votes|Avg Cohesion|High Cohes Rate|Split Votes")
        print("-"*70)
        for row in result:
            print(f"{row.party:10}|{row.total_votes:>5}|{row.avg_cohesion_percent:>11.1f}%|{row.high_cohesion_rate:>14.1f}%|{row.votes_split:>11}")

        # Insight 2: Most rebellious politicians overall
        logger.info("\n\n📊 INSIGHT 2: Most Rebellious Politicians (All Parties)")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                vorname || ' ' || name as full_name,
                party,
                total_votes_participated,
                voted_against_party,
                dissent_rate
            FROM politician_voting_loyalty
            WHERE total_votes_participated >= 10
            ORDER BY dissent_rate DESC
            LIMIT 15
        """))

        for row in result:
            print(f"{row.full_name:35} ({row.party:10}) Dissent: {row.dissent_rate:>5.1f}% ({row.voted_against_party}/{row.total_votes_participated} votes)")

        # Insight 3: Top 3 most rebellious per party
        logger.info("\n\n📊 INSIGHT 3: Most Rebellious Politicians by Party (Top 3)")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                party,
                vorname || ' ' || name as full_name,
                total_votes_participated,
                voted_against_party,
                dissent_rate,
                dissent_rank
            FROM party_rebellious_politicians
            WHERE dissent_rank <= 3
            ORDER BY party, dissent_rank
        """))

        current_party = None
        for row in result:
            if current_party != row.party:
                current_party = row.party
                print(f"\n{row.party}:")
            print(f"  {row.dissent_rank}. {row.full_name:35} Dissent: {row.dissent_rate:>5.1f}% ({row.voted_against_party}/{row.total_votes_participated})")

        # Insight 4: Most loyal politicians
        logger.info("\n\n📊 INSIGHT 4: Most Loyal Politicians (100% Loyalty)")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                vorname || ' ' || name as full_name,
                party,
                total_votes_participated,
                loyalty_percent
            FROM politician_voting_loyalty
            WHERE loyalty_percent = 100
                AND total_votes_participated >= 10
            ORDER BY total_votes_participated DESC
            LIMIT 20
        """))

        count = result.rowcount
        for row in result:
            print(f"{row.full_name:35} ({row.party:10}) - {row.total_votes_participated:>2} votes, 100% loyalty")

        if count == 0:
            print("No politicians with 100% loyalty found (minimum 10 votes)")

        # Insight 5: Most divided votes
        logger.info("\n\n📊 INSIGHT 5: Most Divided Party Votes (Lowest Cohesion)")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                party,
                vote_title,
                vote_date,
                cohesion_percent,
                party_yes_votes,
                party_no_votes,
                party_abstain_votes
            FROM party_vote_positions
            WHERE cohesion_percent IS NOT NULL
            ORDER BY cohesion_percent ASC
            LIMIT 15
        """))

        for row in result:
            print(f"\n{row.party:10} - Cohesion: {row.cohesion_percent:>5.1f}%")
            print(f"  {row.vote_title[:70]}...")
            print(f"  Ja: {row.party_yes_votes}, Nein: {row.party_no_votes}, Enthaltung: {row.party_abstain_votes}")


def main():
    """Main execution function."""
    logger.info("="*80)
    logger.info("PARTY VOTING LOYALTY ANALYSIS")
    logger.info("="*80)

    try:
        # Create views
        create_voting_loyalty_views()

        # Generate insights
        generate_insights()

        # Summary
        logger.info("\n" + "="*80)
        logger.info("ANALYSIS COMPLETE")
        logger.info("="*80)
        logger.info("✅ Created 4 voting loyalty views:")
        logger.info("   1. party_vote_positions - Party majority on each vote")
        logger.info("   2. politician_voting_loyalty - Individual loyalty rates")
        logger.info("   3. party_voting_cohesion - Overall party cohesion")
        logger.info("   4. party_rebellious_politicians - Most rebellious by party")
        logger.info("="*80)

        return 0

    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
