# src/xminer/tasks/fetch_tweets.py
"""
Fetch new tweets for all politicians directly to the main tweets table.

Usage:
    python -m xminer.tasks.fetch_tweets [--limit N] [--dry-run] [--author USERNAME] [--buffer-hours N]

Examples:
    # Fetch tweets for all politicians
    python -m xminer.tasks.fetch_tweets

    # Dry run (see what would be fetched)
    python -m xminer.tasks.fetch_tweets --dry-run

    # Fetch only for specific author
    python -m xminer.tasks.fetch_tweets --author hubertus_heil

    # Limit to 10 profiles
    python -m xminer.tasks.fetch_tweets --limit 10

    # Fetch tweets up to 24 hours ago (buffer for engagement metrics)
    python -m xminer.tasks.fetch_tweets --buffer-hours 24
"""
import os
import logging
import argparse
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from sqlalchemy import text

from ..config.params import Params
from dateutil import parser as dateparser
from ..io.db import engine
from ..io.x_api_dual import client  # Use dual API client
from ..utils.global_helpers import sanitize_rows, INSERT_TWEETS_STMT, politicians_table_name

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/fetch_tweets.log", mode="a"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


def get_all_profiles() -> List[Dict]:
    """Get all politician profiles with X accounts."""
    tbl = politicians_table_name(Params.month, Params.year)
    logger.info("Using politicians table: %s", tbl)

    sql = text(f"""
        SELECT DISTINCT ON (xp.x_user_id)
               xp.x_user_id,
               xp.username
        FROM public.x_profiles AS xp
        JOIN public."{tbl}" AS p
          ON p.username = xp.username
        WHERE xp.x_user_id IS NOT NULL
        ORDER BY xp.x_user_id, xp.retrieved_at DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [{"author_id": int(r[0]), "username": r[1]} for r in rows]


def get_latest_tweet_id(author_id: int) -> Optional[str]:
    """Get the latest tweet ID for an author from the main table."""
    sql = text("""
        SELECT tweet_id FROM tweets
        WHERE author_id = :aid
        ORDER BY created_at DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"aid": author_id}).fetchone()
        return str(row[0]) if row else None


def _refs_to_dict_list(refs):
    """Convert referenced tweets to dict list."""
    if not refs:
        return None
    out = []
    for r in refs:
        rid = getattr(r, "id", None) if not isinstance(r, dict) else r.get("id")
        rtype = getattr(r, "type", None) if not isinstance(r, dict) else r.get("type")
        out.append({"id": int(rid) if rid is not None else None, "type": rtype})
    return out


def normalize_tweet(t, author_id: int, username: Optional[str]) -> Dict:
    """Normalize tweet from API response to database format."""
    pm = getattr(t, "public_metrics", {}) or {}
    entities = getattr(t, "entities", None)
    refs = _refs_to_dict_list(getattr(t, "referenced_tweets", None))

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


def fetch_tweets_for_author(
    author_id: int,
    username: str,
    since_id: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    max_pages: int = 5
) -> List[Dict]:
    """
    Fetch tweets for an author.

    If since_id is provided, fetches only tweets newer than that ID.
    If start_time is provided (and no since_id), fetches only tweets after that time.
    If end_time is provided, fetches only tweets before that time (for buffer).
    """
    all_tweets = []
    cursor = None
    pages_fetched = 0

    try:
        while pages_fetched < max_pages:
            pages_fetched += 1

            kwargs = {
                "id": author_id,
                "max_results": 100,
                "tweet_fields": Params.tweet_fields
            }

            if since_id:
                kwargs["since_id"] = since_id
            elif start_time:
                kwargs["start_time"] = start_time
            if end_time:
                kwargs["end_time"] = end_time
            if cursor:
                kwargs["pagination_token"] = cursor

            response = client.get_users_tweets(**kwargs)
            tweets = response.data or []

            if not tweets:
                break

            for t in tweets:
                # Filter by end_time client-side if API doesn't support it
                tweet_data = normalize_tweet(t, author_id, username)
                if end_time and tweet_data["created_at"] and tweet_data["created_at"] > end_time:
                    continue
                all_tweets.append(tweet_data)

            # Get next page cursor
            meta = getattr(response, 'meta', {}) or {}
            cursor = meta.get('next_token')
            if not cursor:
                break

        return all_tweets

    except Exception as e:
        logger.error("Error fetching tweets for @%s: %s", username, str(e))
        return []


def upsert_tweets(rows: List[Dict]) -> int:
    """Insert tweets to main table with upsert."""
    if not rows:
        return 0
    records = sanitize_rows(rows)
    with engine.begin() as conn:
        conn.execute(INSERT_TWEETS_STMT, records)
    return len(records)


def main():
    parser = argparse.ArgumentParser(description="Fetch tweets for all politicians")
    parser.add_argument("--limit", type=int, help="Limit number of profiles to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--author", type=str, help="Only fetch for specific username")
    parser.add_argument("--buffer-hours", type=int, default=0,
                        help="Buffer hours - only fetch tweets older than N hours (for engagement metrics to settle)")
    args = parser.parse_args()

    profiles = get_all_profiles()
    total_available = len(profiles)

    # Filter by author if specified
    if args.author:
        profiles = [p for p in profiles if p["username"].lower() == args.author.lower()]
        if not profiles:
            logger.error("Author @%s not found", args.author)
            return

    # Apply sampling if configured
    n = int(Params.tweets_sample_limit)
    if n >= 0 and n < len(profiles):
        if Params.sample_seed is not None:
            random.seed(int(Params.sample_seed))
        profiles = random.sample(profiles, n)

    # Apply limit
    if args.limit and args.limit < len(profiles):
        profiles = profiles[:args.limit]

    # Parse last_fetch_date as fallback start_time for authors with no tweets
    fallback_start_time = None
    if Params.last_fetch_date:
        try:
            fallback_start_time = dateparser.parse(Params.last_fetch_date)
            if fallback_start_time.tzinfo is None:
                fallback_start_time = fallback_start_time.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # Calculate end_time based on buffer hours
    end_time = None
    if args.buffer_hours > 0:
        end_time = datetime.now(timezone.utc) - timedelta(hours=args.buffer_hours)

    logger.info("=" * 80)
    logger.info("TWEET FETCH")
    logger.info("=" * 80)
    logger.info("Processing %d profiles (out of %d available)", len(profiles), total_available)
    if fallback_start_time:
        logger.info("Fallback start_time for new authors: %s", fallback_start_time.date())
    if end_time:
        logger.info("Buffer: fetching tweets up to %s (%d hours ago)", end_time.strftime("%Y-%m-%d %H:%M UTC"), args.buffer_hours)
    if args.dry_run:
        logger.info("DRY RUN - no data will be saved")

    total_fetched = 0
    total_new = 0

    for i, p in enumerate(profiles, start=1):
        aid = p["author_id"]
        uname = p["username"]

        # Get latest tweet ID to fetch only new tweets
        since_id = get_latest_tweet_id(aid)

        if since_id:
            logger.info("[%d/%d] @%s (since_id: %s)", i, len(profiles), uname, since_id[:20] + "...")
        else:
            logger.info("[%d/%d] @%s (start_time: %s)", i, len(profiles), uname, fallback_start_time.date() if fallback_start_time else "none")

        tweets = fetch_tweets_for_author(
            aid, uname,
            since_id=since_id,
            start_time=fallback_start_time if not since_id else None,
            end_time=end_time
        )
        total_fetched += len(tweets)

        if tweets:
            if args.dry_run:
                logger.info("  [DRY RUN] Would save %d tweets", len(tweets))
                total_new += len(tweets)
            else:
                saved = upsert_tweets(tweets)
                logger.info("  Saved %d tweets", saved)
                total_new += saved
        else:
            logger.info("  No new tweets")

    # Summary
    logger.info("=" * 80)
    logger.info("FETCH COMPLETE")
    logger.info("=" * 80)
    logger.info("Profiles processed: %d", len(profiles))
    logger.info("Tweets fetched: %d", total_fetched)
    logger.info("Tweets saved: %d", total_new)

    if not args.dry_run and total_new > 0:
        # Show database stats
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*), MIN(created_at), MAX(created_at)
                FROM tweets
            """))
            stats = result.fetchone()
            logger.info("Total tweets in database: %d", stats[0])
            logger.info("Date range: %s to %s", stats[1], stats[2])

        logger.info("")
        logger.info("Remember to update last_fetch_date in parameters.yml!")


if __name__ == "__main__":
    main()
