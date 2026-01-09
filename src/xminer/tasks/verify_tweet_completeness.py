# src/xminer/tasks/verify_tweet_completeness.py
"""
Script to verify tweet data completeness after merge.
Checks for:
1. Any gaps in tweet coverage
2. Authors who might have missing tweets
3. Date range coverage
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import text
from ..io.db import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_main_table_stats():
    """Get comprehensive stats about the main tweets table."""
    logger.info("=" * 80)
    logger.info("MAIN TWEETS TABLE STATISTICS")
    logger.info("=" * 80)

    with engine.connect() as conn:
        # Overall stats
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_tweets,
                COUNT(DISTINCT author_id) as unique_authors,
                MIN(created_at) as earliest_tweet,
                MAX(created_at) as latest_tweet,
                MIN(retrieved_at) as earliest_retrieval,
                MAX(retrieved_at) as latest_retrieval
            FROM tweets
        """))
        stats = result.fetchone()

        logger.info("Total tweets: %d", stats[0])
        logger.info("Unique authors: %d", stats[1])
        logger.info("Tweet date range: %s to %s", stats[2], stats[3])
        logger.info("Retrieval date range: %s to %s", stats[4], stats[5])

        # Tweets by month
        logger.info("\n--- Tweets by Month ---")
        result = conn.execute(text("""
            SELECT
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as tweet_count,
                COUNT(DISTINCT author_id) as author_count
            FROM tweets
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month DESC
            LIMIT 12
        """))
        for row in result:
            logger.info("  %s: %d tweets from %d authors",
                       row[0].strftime("%Y-%m") if row[0] else "Unknown",
                       row[1], row[2])

        return stats


def check_authors_with_gaps():
    """Check for authors who might have gaps in their tweet history."""
    logger.info("\n" + "=" * 80)
    logger.info("CHECKING FOR AUTHORS WITH POTENTIAL GAPS")
    logger.info("=" * 80)

    with engine.connect() as conn:
        # Find authors where we have old tweets but no recent ones
        result = conn.execute(text("""
            WITH author_stats AS (
                SELECT
                    author_id,
                    username,
                    COUNT(*) as tweet_count,
                    MIN(created_at) as first_tweet,
                    MAX(created_at) as last_tweet,
                    MAX(retrieved_at) as last_retrieval
                FROM tweets
                GROUP BY author_id, username
            )
            SELECT
                author_id,
                username,
                tweet_count,
                first_tweet,
                last_tweet,
                last_retrieval,
                EXTRACT(DAY FROM (NOW() - last_tweet)) as days_since_last_tweet
            FROM author_stats
            WHERE last_tweet < NOW() - INTERVAL '7 days'
            ORDER BY days_since_last_tweet DESC
            LIMIT 20
        """))

        rows = result.fetchall()
        if rows:
            logger.info("Authors with no tweets in last 7 days (might be inactive or gaps):")
            for row in rows:
                logger.info("  @%s (ID: %s): %d tweets, last tweet: %s (%.0f days ago)",
                           row[1], row[0], row[2], row[4], row[6] or 0)
        else:
            logger.info("All tracked authors have recent tweets (within 7 days)")

        return rows


def check_retrieval_coverage():
    """Check retrieval coverage - when were tweets last fetched for each author."""
    logger.info("\n" + "=" * 80)
    logger.info("RETRIEVAL COVERAGE ANALYSIS")
    logger.info("=" * 80)

    with engine.connect() as conn:
        # Check retrieval dates
        result = conn.execute(text("""
            SELECT
                DATE(retrieved_at) as retrieval_date,
                COUNT(*) as tweets_fetched,
                COUNT(DISTINCT author_id) as authors_fetched
            FROM tweets
            WHERE retrieved_at >= NOW() - INTERVAL '14 days'
            GROUP BY DATE(retrieved_at)
            ORDER BY retrieval_date DESC
        """))

        logger.info("Retrieval activity (last 14 days):")
        rows = result.fetchall()
        for row in rows:
            logger.info("  %s: %d tweets from %d authors", row[0], row[1], row[2])

        return rows


def compare_with_x_profiles():
    """Compare tweets table authors with x_profiles to find missing authors."""
    logger.info("\n" + "=" * 80)
    logger.info("COMPARING WITH X_PROFILES")
    logger.info("=" * 80)

    with engine.connect() as conn:
        # Find profiles that exist in x_profiles but have no tweets
        result = conn.execute(text("""
            SELECT
                xp.x_user_id,
                xp.username
            FROM x_profiles xp
            WHERE xp.x_user_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM tweets t
                  WHERE t.author_id = xp.x_user_id
              )
            ORDER BY xp.username
        """))

        rows = result.fetchall()
        if rows:
            logger.info("Profiles with NO tweets in database (%d total):", len(rows))
            for row in rows[:20]:  # Show first 20
                logger.info("  @%s (ID: %s)", row[1], row[0])
            if len(rows) > 20:
                logger.info("  ... and %d more", len(rows) - 20)
        else:
            logger.info("✅ All profiles in x_profiles have at least one tweet")

        return rows


def check_recent_tweets_coverage(since_date: str = "2026-01-04"):
    """Check coverage of tweets after a specific date (useful after API switch)."""
    logger.info("\n" + "=" * 80)
    logger.info("RECENT TWEETS COVERAGE (since %s)", since_date)
    logger.info("=" * 80)

    with engine.connect() as conn:
        # Count tweets after the date
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_tweets,
                COUNT(DISTINCT author_id) as unique_authors,
                MIN(created_at) as earliest,
                MAX(created_at) as latest
            FROM tweets
            WHERE created_at >= :since_date
        """), {"since_date": since_date})

        stats = result.fetchone()
        logger.info("Tweets after %s:", since_date)
        logger.info("  Total tweets: %d", stats[0])
        logger.info("  Unique authors: %d", stats[1])
        logger.info("  Date range: %s to %s", stats[2], stats[3])

        # Check if there are authors who tweeted before but not after
        result = conn.execute(text("""
            WITH before_authors AS (
                SELECT DISTINCT author_id, username
                FROM tweets
                WHERE created_at < :since_date
            ),
            after_authors AS (
                SELECT DISTINCT author_id
                FROM tweets
                WHERE created_at >= :since_date
            )
            SELECT
                b.author_id,
                b.username,
                COUNT(DISTINCT t.tweet_id) as old_tweet_count,
                MAX(t.created_at) as last_tweet_before
            FROM before_authors b
            LEFT JOIN after_authors a ON b.author_id = a.author_id
            LEFT JOIN tweets t ON t.author_id = b.author_id AND t.created_at < :since_date
            WHERE a.author_id IS NULL
            GROUP BY b.author_id, b.username
            HAVING COUNT(DISTINCT t.tweet_id) > 5
            ORDER BY old_tweet_count DESC
            LIMIT 20
        """), {"since_date": since_date})

        rows = result.fetchall()
        if rows:
            logger.info("\nActive authors (>5 old tweets) with NO new tweets since %s:", since_date)
            for row in rows:
                logger.info("  @%s: %d old tweets, last: %s", row[1], row[2], row[3])
        else:
            logger.info("\n✅ All previously active authors have tweets after %s", since_date)

        return stats


def check_test_table_status():
    """Check if test table still exists and its status."""
    logger.info("\n" + "=" * 80)
    logger.info("TEST TABLE STATUS")
    logger.info("=" * 80)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'tweets_test_jan2026'
            )
        """))

        exists = result.scalar()
        if exists:
            logger.info("⚠️ Test table 'tweets_test_jan2026' still exists")

            # Get its stats
            result = conn.execute(text("""
                SELECT COUNT(*), MIN(created_at), MAX(created_at)
                FROM tweets_test_jan2026
            """))
            stats = result.fetchone()
            logger.info("  Contains: %d tweets (%s to %s)", stats[0], stats[1], stats[2])
            logger.info("  Run 'python -m xminer.tasks.merge_test_tweets_to_main cleanup' to drop it")
        else:
            logger.info("✅ Test table has been cleaned up (does not exist)")

        return exists


def main():
    """Run all verification checks."""
    logger.info("=" * 80)
    logger.info("TWEET DATA COMPLETENESS VERIFICATION")
    logger.info("Started at: %s", datetime.now(timezone.utc))
    logger.info("=" * 80)

    get_main_table_stats()
    check_authors_with_gaps()
    check_retrieval_coverage()
    compare_with_x_profiles()
    check_recent_tweets_coverage()
    check_test_table_status()

    logger.info("\n" + "=" * 80)
    logger.info("VERIFICATION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
