#!/usr/bin/env python3
"""
Analyze how politicians' social media activity relates to their party's 2025 election performance.

This script creates views and tables linking:
- Politicians (politicians_12_2025)
- Their tweets
- Their party's election results
- Their voting records in Bundestag

Usage:
    python -m xminer.tasks.analyze_politicians_vs_results
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


def create_politician_election_views():
    """Create views linking politicians with election results."""

    logger.info("Creating politician-election analysis views...")

    sql = """
    -- Drop existing views to allow column name changes
    DROP VIEW IF EXISTS politicians_by_state_results CASCADE;
    DROP VIEW IF EXISTS top_politicians_by_performance CASCADE;
    DROP VIEW IF EXISTS politician_activity_summary CASCADE;
    DROP VIEW IF EXISTS politicians_tweets_vs_results CASCADE;
    DROP VIEW IF EXISTS politicians_with_results CASCADE;

    -- =============================================================================
    -- VIEW 1: Politicians with their party's election performance
    -- =============================================================================
    CREATE OR REPLACE VIEW politicians_with_results AS
    SELECT
        p.id,
        p.vorname,
        p.nachname,
        p.username,
        p.partei_kurz,
        p.wp_wkr_land as bundesland,

        -- Party's federal results
        r.anzahl as party_votes_federal,
        r.prozent as party_percent_federal,
        r.vorp_prozent as party_percent_2021,
        r.diff_prozent_pkt as party_change_pkt,

        -- Categorize performance
        CASE
            WHEN r.diff_prozent_pkt > 5 THEN 'Big Winner (>5%)'
            WHEN r.diff_prozent_pkt > 0 THEN 'Winner'
            WHEN r.diff_prozent_pkt < -5 THEN 'Big Loser (<-5%)'
            WHEN r.diff_prozent_pkt < 0 THEN 'Loser'
            ELSE 'New Party'
        END as performance_category

    FROM politicians_12_2025 p
    LEFT JOIN bundestag_2025_results r
        ON (
            -- Normalize party names for matching
            CASE
                WHEN p.partei_kurz = 'BÜNDNIS 90/DIE GRÜNEN' THEN 'GRÜNE'
                WHEN p.partei_kurz = 'DIE LINKE.' THEN 'Die Linke'
                ELSE p.partei_kurz
            END = r.gruppenname
        )
        AND r.gebietsart = 'Bund'
        AND r.gruppenart = 'Partei'
        AND r.stimme = 2;

    COMMENT ON VIEW politicians_with_results IS 'Politicians linked with their party federal election performance';


    -- =============================================================================
    -- VIEW 2: Tweet activity vs party performance
    -- =============================================================================
    CREATE OR REPLACE VIEW politicians_tweets_vs_results AS
    SELECT
        pr.partei_kurz,
        pr.performance_category,
        pr.party_percent_federal,
        pr.party_change_pkt,

        COUNT(DISTINCT pr.username) as politician_count,
        COUNT(t.tweet_id) as total_tweets,
        ROUND(COUNT(t.tweet_id)::NUMERIC / NULLIF(COUNT(DISTINCT pr.username), 0), 1) as avg_tweets_per_politician,

        SUM(t.like_count) as total_likes,
        ROUND(AVG(t.like_count), 1) as avg_likes_per_tweet,

        SUM(t.retweet_count) as total_retweets,
        ROUND(AVG(t.retweet_count), 1) as avg_retweets_per_tweet

    FROM politicians_with_results pr
    LEFT JOIN tweets t ON t.username = pr.username
        AND t.retrieved_at >= '2025-09-01'  -- Exclude test data before Sept 1, 2025
    WHERE pr.partei_kurz IS NOT NULL
    GROUP BY pr.partei_kurz, pr.performance_category, pr.party_percent_federal, pr.party_change_pkt
    ORDER BY pr.party_percent_federal DESC NULLS LAST;

    COMMENT ON VIEW politicians_tweets_vs_results IS 'Party social media activity vs election performance';


    -- =============================================================================
    -- VIEW 3: Individual politician performance
    -- =============================================================================
    CREATE OR REPLACE VIEW politician_activity_summary AS
    SELECT
        pr.vorname,
        pr.nachname,
        pr.username,
        pr.partei_kurz,
        pr.bundesland,
        pr.party_percent_federal,
        pr.party_change_pkt,
        pr.performance_category,

        -- Tweet metrics (2025)
        COUNT(DISTINCT CASE WHEN t.retrieved_at >= '2025-09-01' THEN t.tweet_id END) as tweets_2025,
        COALESCE(SUM(CASE WHEN t.retrieved_at >= '2025-09-01' THEN t.like_count END), 0) as total_likes_2025,
        ROUND(AVG(CASE WHEN t.retrieved_at >= '2025-09-01' THEN t.like_count END), 1) as avg_likes_2025,

        -- Bundestag voting participation
        COUNT(DISTINCT bv.vote_title) as bundestag_votes_participated,
        SUM(bv.ja) as votes_yes,
        SUM(bv.nein) as votes_no,
        SUM(bv.enthaltung) as votes_abstain

    FROM politicians_with_results pr
    LEFT JOIN tweets t ON t.username = pr.username
    LEFT JOIN bundestag_votes bv
        ON CONCAT(bv.vorname, ' ', bv.name) = CONCAT(pr.vorname, ' ', pr.nachname)
        AND bv.fraktion_gruppe = pr.partei_kurz
    WHERE pr.partei_kurz IS NOT NULL
    GROUP BY
        pr.vorname, pr.nachname, pr.username, pr.partei_kurz, pr.bundesland,
        pr.party_percent_federal, pr.party_change_pkt, pr.performance_category
    ORDER BY pr.party_percent_federal DESC NULLS LAST, tweets_2025 DESC;

    COMMENT ON VIEW politician_activity_summary IS 'Individual politician activity: tweets, votes, and party performance';


    -- =============================================================================
    -- VIEW 4: Most active politicians by winning/losing parties
    -- =============================================================================
    CREATE OR REPLACE VIEW top_politicians_by_performance AS
    SELECT
        partei_kurz,
        performance_category,
        party_change_pkt,
        vorname || ' ' || nachname as full_name,
        username,
        tweets_2025,
        avg_likes_2025,
        bundestag_votes_participated,
        ROW_NUMBER() OVER (PARTITION BY partei_kurz ORDER BY tweets_2025 DESC) as rank_in_party
    FROM politician_activity_summary
    WHERE tweets_2025 > 0
    ORDER BY party_change_pkt DESC NULLS LAST, tweets_2025 DESC;

    COMMENT ON VIEW top_politicians_by_performance IS 'Most active politicians ranked within their party';


    -- =============================================================================
    -- VIEW 5: State-level analysis
    -- =============================================================================
    CREATE OR REPLACE VIEW politicians_by_state_results AS
    SELECT
        p.wp_wkr_land as bundesland,
        p.partei_kurz,

        -- Federal results
        pr.party_percent_federal as federal_percent,

        -- State-level results
        sr.prozent as state_percent,
        sr.diff_prozent_pkt as state_change_pkt,

        -- Politician metrics
        COUNT(DISTINCT p.username) as politicians_in_state,
        COUNT(t.tweet_id) as tweets_from_state,
        ROUND(AVG(t.like_count), 1) as avg_likes_per_tweet

    FROM politicians_12_2025 p
    LEFT JOIN politicians_with_results pr
        ON pr.username = p.username
    LEFT JOIN bundestag_2025_results sr
        ON sr.gruppenname = p.partei_kurz
        AND sr.gebietsart = 'Land'
        AND sr.gebietsname = p.wp_wkr_land
        AND sr.gruppenart = 'Partei'
        AND sr.stimme = 2
    LEFT JOIN tweets t
        ON t.username = p.username
        AND t.retrieved_at >= '2025-09-01'  -- Exclude test data before Sept 1, 2025
    WHERE p.wp_wkr_land IS NOT NULL
        AND p.partei_kurz IS NOT NULL
    GROUP BY p.wp_wkr_land, p.partei_kurz, pr.party_percent_federal, sr.prozent, sr.diff_prozent_pkt
    ORDER BY p.wp_wkr_land, sr.prozent DESC NULLS LAST;

    COMMENT ON VIEW politicians_by_state_results IS 'Politician activity and party performance by state';
    """

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    logger.info("✅ Created 5 analysis views")


def generate_insights():
    """Generate insights from the analysis."""

    logger.info("\n" + "="*80)
    logger.info("POLITICIAN-ELECTION ANALYSIS INSIGHTS")
    logger.info("="*80)

    with engine.connect() as conn:

        # Insight 1: Party tweet activity vs performance
        logger.info("\n📊 INSIGHT 1: Social Media Activity vs Election Performance")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                partei_kurz,
                party_change_pkt,
                politician_count,
                total_tweets,
                avg_tweets_per_politician,
                avg_likes_per_tweet
            FROM politicians_tweets_vs_results
            WHERE party_change_pkt IS NOT NULL
            ORDER BY party_change_pkt DESC
        """))

        print("\nParty  |Change|Politicians|Tweets|Avg/Pol|Avg Likes")
        print("-"*65)
        for row in result:
            avg_tweets = row.avg_tweets_per_politician if row.avg_tweets_per_politician else 0
            avg_likes = row.avg_likes_per_tweet if row.avg_likes_per_tweet else 0
            print(f"{row.partei_kurz:7}|{row.party_change_pkt:+5.1f}%|{row.politician_count:>11}|{row.total_tweets:>6}|{avg_tweets:>7.1f}|{avg_likes:>9.1f}")

        # Insight 2: Winners vs Losers - who tweeted more?
        logger.info("\n\n📊 INSIGHT 2: Winners vs Losers - Tweet Activity")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                performance_category,
                COUNT(DISTINCT partei_kurz) as parties,
                SUM(total_tweets) as total_tweets,
                ROUND(AVG(avg_tweets_per_politician), 1) as avg_tweets_per_pol,
                ROUND(AVG(avg_likes_per_tweet), 1) as avg_likes
            FROM politicians_tweets_vs_results
            WHERE performance_category IN ('Big Winner (>5%)', 'Winner', 'Loser', 'Big Loser (<-5%)')
            GROUP BY performance_category
            ORDER BY
                CASE performance_category
                    WHEN 'Big Winner (>5%)' THEN 1
                    WHEN 'Winner' THEN 2
                    WHEN 'Loser' THEN 3
                    WHEN 'Big Loser (<-5%)' THEN 4
                END
        """))

        for row in result:
            print(f"\n{row.performance_category:20}: {row.parties} parties")
            print(f"  Total tweets: {row.total_tweets:,}")
            print(f"  Avg tweets/politician: {row.avg_tweets_per_pol}")
            print(f"  Avg likes/tweet: {row.avg_likes}")

        # Insight 3: Most active politicians in winning parties
        logger.info("\n\n📊 INSIGHT 3: Top 5 Most Active Politicians (Winning Parties)")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                full_name,
                partei_kurz,
                party_change_pkt,
                tweets_2025,
                avg_likes_2025,
                bundestag_votes_participated
            FROM top_politicians_by_performance
            WHERE party_change_pkt > 0
                AND rank_in_party <= 3
            ORDER BY party_change_pkt DESC, tweets_2025 DESC
            LIMIT 15
        """))

        for row in result:
            print(f"{row.full_name:30} ({row.partei_kurz:7}) Party: {row.party_change_pkt:+5.1f}%")
            print(f"  Tweets: {row.tweets_2025:>6}, Avg Likes: {row.avg_likes_2025:>8.1f}, Bundestag Votes: {row.bundestag_votes_participated:>4}")

        # Insight 4: Most active politicians in losing parties
        logger.info("\n\n📊 INSIGHT 4: Top 5 Most Active Politicians (Losing Parties)")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                full_name,
                partei_kurz,
                party_change_pkt,
                tweets_2025,
                avg_likes_2025,
                bundestag_votes_participated
            FROM top_politicians_by_performance
            WHERE party_change_pkt < 0
                AND rank_in_party <= 3
            ORDER BY party_change_pkt ASC, tweets_2025 DESC
            LIMIT 15
        """))

        for row in result:
            print(f"{row.full_name:30} ({row.partei_kurz:7}) Party: {row.party_change_pkt:+5.1f}%")
            print(f"  Tweets: {row.tweets_2025:>6}, Avg Likes: {row.avg_likes_2025:>8.1f}, Bundestag Votes: {row.bundestag_votes_participated:>4}")

        # Insight 5: State-level performance
        logger.info("\n\n📊 INSIGHT 5: State with Most Active Politicians")
        logger.info("-"*80)
        result = conn.execute(text("""
            SELECT
                bundesland,
                SUM(politicians_in_state) as total_politicians,
                SUM(tweets_from_state) as total_tweets,
                ROUND(AVG(avg_likes_per_tweet), 1) as avg_likes
            FROM politicians_by_state_results
            WHERE bundesland IS NOT NULL
            GROUP BY bundesland
            ORDER BY total_tweets DESC
            LIMIT 10
        """))

        for row in result:
            print(f"{row.bundesland:25}: {row.total_politicians:>3} pols, {row.total_tweets:>6} tweets, {row.avg_likes:>7.1f} avg likes")


def main():
    """Main execution function."""
    logger.info("="*80)
    logger.info("POLITICIAN vs ELECTION RESULTS ANALYSIS")
    logger.info("="*80)

    try:
        # Create views
        create_politician_election_views()

        # Generate insights
        generate_insights()

        # Summary
        logger.info("\n" + "="*80)
        logger.info("ANALYSIS COMPLETE")
        logger.info("="*80)
        logger.info("✅ Created 5 analysis views:")
        logger.info("   1. politicians_with_results - Basic linking")
        logger.info("   2. politicians_tweets_vs_results - Party-level aggregation")
        logger.info("   3. politician_activity_summary - Individual metrics")
        logger.info("   4. top_politicians_by_performance - Rankings")
        logger.info("   5. politicians_by_state_results - State-level analysis")
        logger.info("="*80)

        return 0

    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
