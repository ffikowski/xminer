# src/xminer/tasks/merge_test_tweets_to_main.py
"""
Script to verify test table and merge tweets into main tweets table.
Run this AFTER fetch_tweets_jan2026_test.py to move data from test to production.
"""
import logging
from sqlalchemy import text
from ..io.db import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TEST_TABLE = "tweets_test_jan2026"

def verify_test_table():
    """Show summary of test table data."""
    logger.info("=" * 80)
    logger.info("VERIFYING TEST TABLE: %s", TEST_TABLE)
    logger.info("=" * 80)

    with engine.connect() as conn:
        # Check if table exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :tbl
            )
        """), {"tbl": TEST_TABLE})

        if not result.scalar():
            logger.error("❌ Test table %s does not exist!", TEST_TABLE)
            return False

        # Get summary stats
        result = conn.execute(text(f"""
            SELECT
                COUNT(*) as total,
                MIN(created_at) as earliest,
                MAX(created_at) as latest,
                COUNT(DISTINCT author_id) as unique_authors
            FROM {TEST_TABLE}
        """))
        stats = result.fetchone()

        logger.info("Total tweets in test table: %d", stats[0])
        logger.info("Date range: %s to %s", stats[1], stats[2])
        logger.info("Unique authors: %d", stats[3])

        # Show breakdown by author
        result = conn.execute(text(f"""
            SELECT
                username,
                COUNT(*) as tweet_count
            FROM {TEST_TABLE}
            GROUP BY username
            ORDER BY tweet_count DESC
            LIMIT 10
        """))
        logger.info("\nTop 10 authors by tweet count:")
        for row in result:
            logger.info("  %s: %d tweets", row[0], row[1])

        # Check for duplicates with main table
        result = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM {TEST_TABLE} t
            WHERE EXISTS (
                SELECT 1 FROM tweets m
                WHERE m.tweet_id = t.tweet_id
            )
        """))
        duplicates = result.scalar()
        logger.info("\nTweets already in main table: %d", duplicates)

        # Show new tweets count
        result = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM {TEST_TABLE} t
            WHERE NOT EXISTS (
                SELECT 1 FROM tweets m
                WHERE m.tweet_id = t.tweet_id
            )
        """))
        new_count = result.scalar()
        logger.info("New tweets to be added: %d", new_count)

        logger.info("=" * 80)
        return True

def merge_to_main(dry_run=True):
    """
    Merge test table data into main tweets table.

    Args:
        dry_run: If True, only show what would be done. If False, actually merge.
    """
    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
    else:
        logger.warning("=" * 80)
        logger.warning("LIVE MODE - Data will be merged into main tweets table!")
        logger.warning("=" * 80)

    with engine.begin() as conn:
        # Count tweets to merge
        result = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM {TEST_TABLE}
        """))
        total_tweets = result.scalar()

        logger.info("Tweets to merge: %d", total_tweets)

        if not dry_run:
            # Perform the merge
            result = conn.execute(text(f"""
                INSERT INTO tweets (
                    tweet_id, author_id, username, created_at, text, lang,
                    conversation_id, in_reply_to_user_id, possibly_sensitive,
                    like_count, reply_count, retweet_count, quote_count,
                    bookmark_count, impression_count,
                    source, entities, referenced_tweets, retrieved_at
                )
                SELECT
                    tweet_id, author_id, username, created_at, text, lang,
                    conversation_id, in_reply_to_user_id, possibly_sensitive,
                    like_count, reply_count, retweet_count, quote_count,
                    bookmark_count, impression_count,
                    source, entities, referenced_tweets, retrieved_at
                FROM {TEST_TABLE}
                ON CONFLICT (tweet_id)
                DO UPDATE SET
                    like_count = EXCLUDED.like_count,
                    reply_count = EXCLUDED.reply_count,
                    retweet_count = EXCLUDED.retweet_count,
                    quote_count = EXCLUDED.quote_count,
                    bookmark_count = EXCLUDED.bookmark_count,
                    impression_count = EXCLUDED.impression_count,
                    retrieved_at = EXCLUDED.retrieved_at
            """))

            logger.info("✅ Successfully merged %d tweets into main table", total_tweets)

            # Verify main table
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN created_at >= '2026-01-04' THEN 1 END) as after_jan4
                FROM tweets
            """))
            stats = result.fetchone()
            logger.info("Main tweets table now has:")
            logger.info("  Total tweets: %d", stats[0])
            logger.info("  Tweets after 2026-01-04: %d", stats[1])
        else:
            logger.info("(Dry run - no changes made)")

        logger.info("=" * 80)

def drop_test_table():
    """Drop the test table after successful merge."""
    logger.warning("Dropping test table: %s", TEST_TABLE)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TEST_TABLE}"))
    logger.info("✅ Test table dropped")

def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m xminer.tasks.merge_test_tweets_to_main verify")
        print("  python -m xminer.tasks.merge_test_tweets_to_main merge --dry-run")
        print("  python -m xminer.tasks.merge_test_tweets_to_main merge --live")
        print("  python -m xminer.tasks.merge_test_tweets_to_main cleanup")
        return

    command = sys.argv[1]

    if command == "verify":
        verify_test_table()
    elif command == "merge":
        if len(sys.argv) < 3:
            logger.error("Specify --dry-run or --live")
            return
        dry_run = sys.argv[2] == "--dry-run"
        verify_test_table()
        merge_to_main(dry_run=dry_run)
    elif command == "cleanup":
        drop_test_table()
    else:
        logger.error("Unknown command: %s", command)

if __name__ == "__main__":
    main()
