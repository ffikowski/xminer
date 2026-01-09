# src/xminer/tasks/fetch_missing_authors.py
"""
Fetch tweets for authors who might have been missed in recent fetches.
Targets active authors (>5 old tweets) with no new tweets since a cutoff date.
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import text
from ..io.db import engine
from ..io.x_api_dual import client  # Use the global client instance
from ..utils.global_helpers import sanitize_rows, INSERT_TWEETS_STMT
from ..config.params import Params

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_missing_authors(since_date: str = "2026-01-04", min_old_tweets: int = 5):
    """Get authors who were active before but have no tweets since cutoff."""
    with engine.connect() as conn:
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
            ),
            author_stats AS (
                SELECT
                    b.author_id,
                    b.username,
                    COUNT(DISTINCT t.tweet_id) as old_tweet_count,
                    MAX(t.created_at) as last_tweet_before
                FROM before_authors b
                LEFT JOIN tweets t ON t.author_id = b.author_id AND t.created_at < :since_date
                GROUP BY b.author_id, b.username
            )
            SELECT
                a.author_id,
                a.username,
                a.old_tweet_count,
                a.last_tweet_before
            FROM author_stats a
            LEFT JOIN after_authors af ON a.author_id = af.author_id
            WHERE af.author_id IS NULL
              AND a.old_tweet_count >= :min_tweets
            ORDER BY a.old_tweet_count DESC
        """), {"since_date": since_date, "min_tweets": min_old_tweets})

        return [
            {"author_id": row[0], "username": row[1], "old_count": row[2], "last_tweet": row[3]}
            for row in result.fetchall()
        ]


def normalize_tweet(t, author_id: int, username: str):
    """Normalize tweet from API response to database format."""
    pm = getattr(t, "public_metrics", {}) or {}
    entities = getattr(t, "entities", None)
    refs = getattr(t, "referenced_tweets", None)

    return {
        "tweet_id": str(t.id),
        "author_id": int(author_id),
        "username": username,
        "created_at": t.created_at,
        "text": getattr(t, "text", None),
        "lang": getattr(t, "lang", None),
        "conversation_id": str(getattr(t, "conversation_id")) if getattr(t, "conversation_id", None) else None,
        "in_reply_to_user_id": getattr(t, "in_reply_to_user_id", None),
        "possibly_sensitive": getattr(t, "possibly_sensitive", None),
        "like_count": pm.get("like_count"),
        "reply_count": pm.get("reply_count"),
        "retweet_count": pm.get("retweet_count"),
        "quote_count": pm.get("quote_count"),
        "bookmark_count": pm.get("bookmark_count"),
        "impression_count": pm.get("impression_count"),
        "source": getattr(t, "source", None),
        "entities": entities,
        "referenced_tweets": refs,
        "retrieved_at": datetime.now(timezone.utc),
    }


def fetch_and_save_tweets(author_id: int, username: str, since_date: str = "2026-01-04"):
    """Fetch tweets for a single author since cutoff date."""
    since_dt = datetime.fromisoformat(since_date + "T00:00:00+00:00")

    logger.info("Fetching tweets for @%s (ID: %s) since %s", username, author_id, since_date)

    try:
        # Use the correct API method with start_time filter
        response = client.get_users_tweets(
            id=author_id,
            max_results=100,
            start_time=since_dt,
            tweet_fields=Params.tweet_fields
        )

        tweets = response.data or []

        if not tweets:
            logger.info("  No tweets returned for @%s", username)
            return 0

        # Normalize tweets
        rows = [normalize_tweet(t, author_id, username) for t in tweets]

        # Save to database
        records = sanitize_rows(rows)
        with engine.begin() as conn:
            conn.execute(INSERT_TWEETS_STMT, records)

        logger.info("  ✅ @%s: Saved %d new tweets", username, len(records))
        return len(records)

    except Exception as e:
        logger.error("  ❌ Error fetching @%s: %s", username, str(e))
        return 0


def main():
    import sys

    since_date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-04"
    min_tweets = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    logger.info("=" * 80)
    logger.info("FETCHING TWEETS FOR POTENTIALLY MISSING AUTHORS")
    logger.info("Since date: %s, Min old tweets: %d", since_date, min_tweets)
    logger.info("=" * 80)

    missing = get_missing_authors(since_date, min_tweets)
    logger.info("Found %d authors to check", len(missing))

    total_fetched = 0
    for i, author in enumerate(missing, 1):
        logger.info("\n[%d/%d] @%s - %d old tweets, last: %s",
                   i, len(missing), author["username"], author["old_count"], author["last_tweet"])
        fetched = fetch_and_save_tweets(author["author_id"], author["username"], since_date)
        total_fetched += fetched

    logger.info("\n" + "=" * 80)
    logger.info("COMPLETE: Fetched %d new tweets from %d authors", total_fetched, len(missing))
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
