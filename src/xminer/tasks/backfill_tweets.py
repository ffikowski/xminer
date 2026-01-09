# src/xminer/tasks/backfill_tweets.py
"""
Backfill historical tweets for authors.

This script ensures tweet continuity by:
1. Finding the oldest tweet for each author in the database
2. Fetching tweets before that oldest tweet to fill any gaps
3. Can be run periodically to backfill missing historical data

Usage:
    python -m xminer.tasks.backfill_tweets [--limit N] [--min-gap-days N] [--dry-run]

Options:
    --limit N        Max number of authors to process (default: all)
    --min-gap-days N Only backfill if oldest tweet is newer than N days (default: 0)
    --dry-run        Only show what would be fetched, don't save
    --author USERNAME  Only backfill for specific author
"""
import os
import logging
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from sqlalchemy import text

from ..config.params import Params
from ..io.db import engine
from ..io.x_api_dual import client
from ..utils.global_helpers import sanitize_rows, INSERT_TWEETS_STMT, politicians_table_name

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/backfill_tweets.log", mode="a"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


def get_authors_with_oldest_tweets() -> List[Dict]:
    """
    Get all authors with their oldest tweet date.
    Returns list of dicts with author_id, username, oldest_tweet_id, oldest_tweet_date, tweet_count.
    """
    sql = text("""
        SELECT
            t.author_id,
            t.username,
            t.tweet_id as oldest_tweet_id,
            t.created_at as oldest_tweet_date,
            counts.tweet_count
        FROM tweets t
        JOIN (
            SELECT author_id, MIN(created_at) as min_created_at, COUNT(*) as tweet_count
            FROM tweets
            GROUP BY author_id
        ) counts ON t.author_id = counts.author_id AND t.created_at = counts.min_created_at
        ORDER BY counts.tweet_count DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return [
        {
            "author_id": row[0],
            "username": row[1],
            "oldest_tweet_id": row[2],
            "oldest_tweet_date": row[3],
            "tweet_count": row[4]
        }
        for row in rows
    ]


def get_authors_with_latest_tweets() -> List[Dict]:
    """
    Get all authors with their latest tweet date.
    Used for checking gaps since the last fetch.
    Returns list of dicts with author_id, username, latest_tweet_id, latest_tweet_date, tweet_count.
    """
    sql = text("""
        SELECT
            t.author_id,
            t.username,
            t.tweet_id as latest_tweet_id,
            t.created_at as latest_tweet_date,
            counts.tweet_count
        FROM tweets t
        JOIN (
            SELECT author_id, MAX(created_at) as max_created_at, COUNT(*) as tweet_count
            FROM tweets
            GROUP BY author_id
        ) counts ON t.author_id = counts.author_id AND t.created_at = counts.max_created_at
        ORDER BY counts.tweet_count DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return [
        {
            "author_id": row[0],
            "username": row[1],
            "latest_tweet_id": row[2],
            "latest_tweet_date": row[3],
            "tweet_count": row[4]
        }
        for row in rows
    ]


def get_active_politicians() -> set:
    """Get set of author_ids for current politicians."""
    tbl = politicians_table_name(Params.month, Params.year)

    sql = text(f"""
        SELECT DISTINCT xp.x_user_id
        FROM public.x_profiles AS xp
        JOIN public."{tbl}" AS p ON p.username = xp.username
        WHERE xp.x_user_id IS NOT NULL
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    return {int(row[0]) for row in rows}


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


def normalize_tweet(t, author_id: int, username: str) -> Dict:
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


def fetch_tweets_since(
    author_id: int,
    username: str,
    since_tweet_id: str,
    max_pages: int = 5
) -> List[Dict]:
    """
    Fetch tweets for an author since a specific tweet ID.

    This fetches tweets newer than the given tweet ID, ensuring we don't
    miss any tweets that were posted since our last fetch.

    Args:
        author_id: Twitter user ID
        username: Twitter username
        since_tweet_id: Fetch tweets newer than this tweet ID
        max_pages: Maximum number of pages to fetch (safety limit)

    Returns:
        List of normalized tweet dicts
    """
    all_tweets = []
    cursor = None
    pages_fetched = 0

    try:
        while pages_fetched < max_pages:
            pages_fetched += 1

            response = client.get_users_tweets(
                id=author_id,
                max_results=100,
                since_id=since_tweet_id,
                pagination_token=cursor,
                tweet_fields=Params.tweet_fields
            )

            tweets = response.data or []
            if not tweets:
                break

            for t in tweets:
                normalized = normalize_tweet(t, author_id, username)
                all_tweets.append(normalized)

            # Get next page cursor
            meta = getattr(response, 'meta', {}) or {}
            cursor = meta.get('next_token')
            if not cursor:
                break

        if all_tweets:
            logger.info("  Fetched %d tweets across %d pages for @%s",
                       len(all_tweets), pages_fetched, username)

        return all_tweets

    except Exception as e:
        logger.error("Error fetching tweets for @%s: %s", username, str(e))
        return []


def fetch_tweets_with_pagination(
    author_id: int,
    username: str,
    target_date: datetime,
    max_pages: int = 10
) -> List[Dict]:
    """
    Fetch tweets for an author, paginating backwards until we reach target_date.

    This fetches all tweets and keeps paginating until we find tweets older
    than target_date, then returns tweets we don't have yet.

    Args:
        author_id: Twitter user ID
        username: Twitter username
        target_date: Keep fetching until we find tweets older than this
        max_pages: Maximum number of pages to fetch (safety limit)

    Returns:
        List of normalized tweet dicts
    """
    all_tweets = []
    cursor = None
    pages_fetched = 0
    found_older = False

    try:
        while pages_fetched < max_pages and not found_older:
            pages_fetched += 1

            response = client.get_users_tweets(
                id=author_id,
                max_results=100,
                pagination_token=cursor,
                tweet_fields=Params.tweet_fields
            )

            tweets = response.data or []
            if not tweets:
                break

            for t in tweets:
                normalized = normalize_tweet(t, author_id, username)
                all_tweets.append(normalized)

                # Check if we've gone past our target date
                if t.created_at and t.created_at < target_date:
                    found_older = True

            # Get next page cursor
            meta = getattr(response, 'meta', {}) or {}
            cursor = meta.get('next_token')
            if not cursor:
                break

            logger.debug("  Page %d: fetched %d tweets, cursor: %s",
                        pages_fetched, len(tweets), cursor[:20] if cursor else None)

        logger.info("  Fetched %d total tweets across %d pages for @%s",
                   len(all_tweets), pages_fetched, username)

        return all_tweets

    except Exception as e:
        logger.error("Error fetching tweets for @%s: %s", username, str(e))
        return []


def fill_gaps_for_author(
    author_id: int,
    username: str,
    latest_tweet_id: str,
    latest_tweet_date: datetime,
    dry_run: bool = False,
    max_pages: int = 5
) -> int:
    """
    Fill gaps for an author since their last stored tweet.

    Fetches tweets newer than the latest stored tweet to catch any
    that may have been missed in recent fetches.

    Args:
        author_id: Twitter user ID
        username: Twitter username
        latest_tweet_id: ID of the latest tweet in database
        latest_tweet_date: Date of the latest tweet in database
        dry_run: If True, don't save to database
        max_pages: Maximum pages to fetch per author

    Returns:
        Number of tweets filled
    """
    logger.info("Checking @%s (ID: %s), latest in DB: %s",
                username, author_id, latest_tweet_date)

    # Fetch tweets since the last stored tweet
    tweets = fetch_tweets_since(
        author_id, username, latest_tweet_id, max_pages=max_pages
    )

    if not tweets:
        logger.info("  No new tweets for @%s", username)
        return 0

    # Filter out any tweets we already have (shouldn't happen with since_id, but safety check)
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT tweet_id FROM tweets WHERE author_id = :aid"),
            {"aid": author_id}
        ).fetchall()
        existing_ids = {str(row[0]) for row in existing}

    new_tweets = [t for t in tweets if t["tweet_id"] not in existing_ids]

    if not new_tweets:
        logger.info("  All %d tweets already in database for @%s", len(tweets), username)
        return 0

    # Sort by date to show stats
    new_tweets_with_dates = [t for t in new_tweets if t["created_at"]]
    if new_tweets_with_dates:
        oldest_new = min(t["created_at"] for t in new_tweets_with_dates)
        newest_new = max(t["created_at"] for t in new_tweets_with_dates)
    else:
        oldest_new = newest_new = None

    if dry_run:
        logger.info("  [DRY RUN] Would save %d new tweets for @%s",
                   len(new_tweets), username)
        if oldest_new:
            logger.info("  [DRY RUN] Date range: %s to %s", oldest_new, newest_new)
        return len(new_tweets)

    # Save to database
    records = sanitize_rows(new_tweets)
    with engine.begin() as conn:
        conn.execute(INSERT_TWEETS_STMT, records)

    logger.info("  ✅ Saved %d new tweets for @%s", len(records), username)
    if oldest_new:
        logger.info("  ✅ Date range: %s to %s", oldest_new, newest_new)

    return len(records)


def backfill_author(
    author_id: int,
    username: str,
    oldest_tweet_date: datetime,
    dry_run: bool = False,
    max_pages: int = 10
) -> int:
    """
    Backfill tweets for a single author (historical backfill).

    Fetches tweets with pagination until we reach tweets older than our
    oldest stored tweet, ensuring no gaps in the timeline.

    Args:
        author_id: Twitter user ID
        username: Twitter username
        oldest_tweet_date: Date of oldest tweet in database
        dry_run: If True, don't save to database
        max_pages: Maximum pages to fetch per author

    Returns:
        Number of tweets backfilled
    """
    logger.info("Backfilling @%s (ID: %s), oldest in DB: %s",
                username, author_id, oldest_tweet_date)

    # Fetch tweets, paginating until we pass our oldest tweet
    tweets = fetch_tweets_with_pagination(
        author_id, username, oldest_tweet_date, max_pages=max_pages
    )

    if not tweets:
        logger.info("  No tweets returned from API for @%s", username)
        return 0

    # Filter out any tweets we already have
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT tweet_id FROM tweets WHERE author_id = :aid"),
            {"aid": author_id}
        ).fetchall()
        existing_ids = {str(row[0]) for row in existing}

    new_tweets = [t for t in tweets if t["tweet_id"] not in existing_ids]

    if not new_tweets:
        logger.info("  All %d tweets already in database for @%s", len(tweets), username)
        return 0

    # Sort by date to show stats
    new_tweets_with_dates = [t for t in new_tweets if t["created_at"]]
    if new_tweets_with_dates:
        oldest_new = min(t["created_at"] for t in new_tweets_with_dates)
        newest_new = max(t["created_at"] for t in new_tweets_with_dates)
    else:
        oldest_new = newest_new = None

    if dry_run:
        logger.info("  [DRY RUN] Would save %d new tweets for @%s",
                   len(new_tweets), username)
        if oldest_new:
            logger.info("  [DRY RUN] Date range: %s to %s", oldest_new, newest_new)
        return len(new_tweets)

    # Save to database
    records = sanitize_rows(new_tweets)
    with engine.begin() as conn:
        conn.execute(INSERT_TWEETS_STMT, records)

    logger.info("  ✅ Saved %d new tweets for @%s", len(records), username)
    if oldest_new:
        logger.info("  ✅ Date range: %s to %s", oldest_new, newest_new)

    return len(records)


def run_fill_gaps(
    limit: Optional[int] = None,
    since_date: Optional[str] = None,
    dry_run: bool = False,
    author_filter: Optional[str] = None,
    only_active_politicians: bool = True
):
    """
    Fill gaps for authors whose latest tweet is before the last fetch date.

    This ensures no tweets are missed between fetches. Uses last_fetch_date
    from parameters.yml by default.

    Args:
        limit: Max number of authors to process
        since_date: Only process authors whose latest tweet is before this date (YYYY-MM-DD)
                   If not specified, uses last_fetch_date from parameters.yml
        dry_run: If True, don't save to database
        author_filter: Only process this specific username
        only_active_politicians: Only process current politicians
    """
    # Use last_fetch_date from params if since_date not specified
    if since_date is None:
        since_date = Params.last_fetch_date
        if since_date:
            logger.info("Using last_fetch_date from parameters.yml: %s", since_date)
        else:
            logger.warning("No since_date specified and last_fetch_date not set in parameters.yml")
            logger.warning("Will check ALL authors for gaps (this may take a while)")

    logger.info("=" * 80)
    logger.info("FILL GAPS SINCE LAST FETCH")
    logger.info("=" * 80)
    logger.info("Options: limit=%s, since_date=%s, dry_run=%s, author=%s",
               limit or "all", since_date or "all", dry_run, author_filter or "all")

    # Get all authors with their latest tweets
    authors = get_authors_with_latest_tweets()
    logger.info("Found %d authors with tweets in database", len(authors))

    # Filter to active politicians if requested
    if only_active_politicians:
        active_ids = get_active_politicians()
        authors = [a for a in authors if a["author_id"] in active_ids]
        logger.info("Filtered to %d active politicians", len(authors))

    # Filter by username if specified
    if author_filter:
        authors = [a for a in authors if a["username"].lower() == author_filter.lower()]
        if not authors:
            logger.error("Author @%s not found", author_filter)
            return

    # Filter by since_date - only process authors whose latest tweet is BEFORE this date
    if since_date:
        cutoff = datetime.fromisoformat(since_date + "T00:00:00+00:00")
        before_filter = len(authors)
        authors = [a for a in authors if a["latest_tweet_date"] and a["latest_tweet_date"] < cutoff]
        logger.info("Filtered from %d to %d authors (latest tweet before %s)",
                   before_filter, len(authors), since_date)

    # Apply limit
    if limit and limit < len(authors):
        authors = authors[:limit]
        logger.info("Limited to %d authors", limit)

    if not authors:
        logger.info("No authors to process - all authors have tweets after %s", since_date)
        return

    # Process each author
    total_filled = 0
    for i, author in enumerate(authors, 1):
        logger.info("\n[%d/%d] @%s (%d tweets, latest: %s)",
                   i, len(authors), author["username"], author["tweet_count"],
                   author["latest_tweet_date"])

        count = fill_gaps_for_author(
            author_id=author["author_id"],
            username=author["username"],
            latest_tweet_id=author["latest_tweet_id"],
            latest_tweet_date=author["latest_tweet_date"],
            dry_run=dry_run
        )
        total_filled += count

    logger.info("\n" + "=" * 80)
    logger.info("FILL GAPS COMPLETE")
    logger.info("=" * 80)
    logger.info("Processed %d authors", len(authors))
    logger.info("Total tweets filled: %d", total_filled)
    if dry_run:
        logger.info("(DRY RUN - no data was saved)")
    else:
        logger.info("\n💡 Remember to update last_fetch_date in parameters.yml after a successful fetch!")


def run_backfill(
    limit: Optional[int] = None,
    min_gap_days: int = 0,
    dry_run: bool = False,
    author_filter: Optional[str] = None,
    only_active_politicians: bool = True
):
    """
    Run historical backfill for all authors or a subset.

    This fetches older tweets by paginating backwards from each author's
    oldest stored tweet.

    Args:
        limit: Max number of authors to process
        min_gap_days: Only process authors whose oldest tweet is newer than this many days
        dry_run: If True, don't save to database
        author_filter: Only process this specific username
        only_active_politicians: Only process current politicians
    """
    logger.info("=" * 80)
    logger.info("HISTORICAL TWEET BACKFILL")
    logger.info("=" * 80)
    logger.info("Options: limit=%s, min_gap_days=%d, dry_run=%s, author=%s",
               limit or "all", min_gap_days, dry_run, author_filter or "all")

    # Get all authors with their oldest tweets
    authors = get_authors_with_oldest_tweets()
    logger.info("Found %d authors with tweets in database", len(authors))

    # Filter to active politicians if requested
    if only_active_politicians:
        active_ids = get_active_politicians()
        authors = [a for a in authors if a["author_id"] in active_ids]
        logger.info("Filtered to %d active politicians", len(authors))

    # Filter by username if specified
    if author_filter:
        authors = [a for a in authors if a["username"].lower() == author_filter.lower()]
        if not authors:
            logger.error("Author @%s not found", author_filter)
            return

    # Filter by gap days
    if min_gap_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=min_gap_days)
        before_filter = len(authors)
        authors = [a for a in authors if a["oldest_tweet_date"] and a["oldest_tweet_date"] > cutoff]
        logger.info("Filtered from %d to %d authors (oldest tweet newer than %d days)",
                   before_filter, len(authors), min_gap_days)

    # Apply limit
    if limit and limit < len(authors):
        authors = authors[:limit]
        logger.info("Limited to %d authors", limit)

    if not authors:
        logger.info("No authors to process")
        return

    # Process each author
    total_backfilled = 0
    for i, author in enumerate(authors, 1):
        logger.info("\n[%d/%d] Processing @%s (%d tweets, oldest: %s)",
                   i, len(authors), author["username"], author["tweet_count"],
                   author["oldest_tweet_date"])

        count = backfill_author(
            author_id=author["author_id"],
            username=author["username"],
            oldest_tweet_date=author["oldest_tweet_date"],
            dry_run=dry_run
        )
        total_backfilled += count

    logger.info("\n" + "=" * 80)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 80)
    logger.info("Processed %d authors", len(authors))
    logger.info("Total tweets backfilled: %d", total_backfilled)
    if dry_run:
        logger.info("(DRY RUN - no data was saved)")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill tweets - fill gaps or fetch historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fill gaps since last fetch (recommended after each fetch)
  python -m xminer.tasks.backfill_tweets fill-gaps

  # Fill gaps for authors whose latest tweet is before Jan 4
  python -m xminer.tasks.backfill_tweets fill-gaps --since-date 2026-01-04

  # Dry run to see what would be fetched
  python -m xminer.tasks.backfill_tweets fill-gaps --dry-run

  # Historical backfill (fetch older tweets)
  python -m xminer.tasks.backfill_tweets historical --limit 10
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Fill gaps command (recommended)
    fill_parser = subparsers.add_parser(
        "fill-gaps",
        help="Fill gaps since each author's last stored tweet (recommended)"
    )
    fill_parser.add_argument("--limit", type=int, help="Max number of authors to process")
    fill_parser.add_argument("--since-date", type=str,
                            help="Only process authors whose latest tweet is before this date (YYYY-MM-DD)")
    fill_parser.add_argument("--dry-run", action="store_true",
                            help="Only show what would be fetched")
    fill_parser.add_argument("--author", type=str, help="Only process specific username")
    fill_parser.add_argument("--all-authors", action="store_true",
                            help="Process all authors, not just active politicians")

    # Historical backfill command
    hist_parser = subparsers.add_parser(
        "historical",
        help="Fetch older tweets (historical backfill)"
    )
    hist_parser.add_argument("--limit", type=int, help="Max number of authors to process")
    hist_parser.add_argument("--min-gap-days", type=int, default=0,
                            help="Only backfill if oldest tweet is newer than N days")
    hist_parser.add_argument("--dry-run", action="store_true",
                            help="Only show what would be fetched")
    hist_parser.add_argument("--author", type=str, help="Only backfill for specific username")
    hist_parser.add_argument("--all-authors", action="store_true",
                            help="Process all authors, not just active politicians")

    args = parser.parse_args()

    if args.command == "fill-gaps":
        run_fill_gaps(
            limit=args.limit,
            since_date=args.since_date,
            dry_run=args.dry_run,
            author_filter=args.author,
            only_active_politicians=not args.all_authors
        )
    elif args.command == "historical":
        run_backfill(
            limit=args.limit,
            min_gap_days=args.min_gap_days,
            dry_run=args.dry_run,
            author_filter=args.author,
            only_active_politicians=not args.all_authors
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
