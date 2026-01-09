# src/xminer/tasks/fetch_tweets_jan2026_test.py
"""
Fetch new tweets from 2026-01-04 onwards into a test table.
This allows testing before updating the main tweets table.
"""
import os, time, logging, random, json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import tweepy
from sqlalchemy import text

from ..config.params import Params
from ..io.db import engine
from ..io.x_api_dual import client  # Use dual API client
from ..utils.global_helpers import sanitize_rows, politicians_table_name

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/fetch_tweets_jan2026_test.log", mode="w"),
              logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ---------- Test table setup ----------
TEST_TABLE = "tweets_test_jan2026"
START_DATE = datetime(2026, 1, 4, tzinfo=timezone.utc)

def setup_test_table():
    """Create test table if it doesn't exist."""
    with engine.begin() as conn:
        # Check if table exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :tbl
            )
        """), {"tbl": TEST_TABLE})

        if not result.scalar():
            logger.info(f"Creating test table: {TEST_TABLE}")
            conn.execute(text(f"""
                CREATE TABLE {TEST_TABLE} (LIKE tweets INCLUDING ALL)
            """))
            logger.info(f"✅ Test table created: {TEST_TABLE}")
        else:
            logger.info(f"Test table already exists: {TEST_TABLE}")

# ---------- INSERT statement for test table ----------
INSERT_TEST_TWEETS_STMT = text(f"""
    INSERT INTO {TEST_TABLE} (
        tweet_id, author_id, username, created_at, text, lang,
        conversation_id, in_reply_to_user_id, possibly_sensitive,
        like_count, reply_count, retweet_count, quote_count,
        bookmark_count, impression_count,
        source, entities, referenced_tweets, retrieved_at
    ) VALUES (
        :tweet_id, :author_id, :username, :created_at, :text, :lang,
        :conversation_id, :in_reply_to_user_id, :possibly_sensitive,
        :like_count, :reply_count, :retweet_count, :quote_count,
        :bookmark_count, :impression_count,
        :source, :entities, :referenced_tweets, :retrieved_at
    )
    ON CONFLICT (tweet_id)
    DO UPDATE SET
        like_count = EXCLUDED.like_count,
        reply_count = EXCLUDED.reply_count,
        retweet_count = EXCLUDED.retweet_count,
        quote_count = EXCLUDED.quote_count,
        bookmark_count = EXCLUDED.bookmark_count,
        impression_count = EXCLUDED.impression_count,
        retrieved_at = EXCLUDED.retrieved_at
""")

# ---------- db ----------
def get_all_profiles() -> list[dict]:
    tbl = politicians_table_name(Params.month, Params.year)
    logger.info("Filtering x_profiles using table: %s", tbl)

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

    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()
    return [{"author_id": int(r[0]), "username": r[1]} for r in rows]

def get_latest_tweet_id_from_test(author_id: int) -> Optional[str]:
    """Get latest tweet ID from test table."""
    sql = text(f"""
        SELECT tweet_id FROM {TEST_TABLE}
        WHERE author_id = :aid
        ORDER BY created_at DESC
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"aid": author_id}).fetchone()
        return str(row[0]) if row else None

def get_latest_tweet_id_from_main(author_id: int) -> Optional[str]:
    """Get latest tweet ID from main tweets table."""
    sql = text("""
        SELECT tweet_id FROM tweets
        WHERE author_id = :aid
        ORDER BY created_at DESC
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"aid": author_id}).fetchone()
        return str(row[0]) if row else None

# ---------- rate-limit ----------
def sleep_from_headers(response) -> None:
    try:
        hdrs = response.headers if response is not None else {}
        reset = hdrs.get("x-rate-limit-reset")
        now = int(time.time())
        reset_ts = int(reset) if reset and reset.isdigit() else None
        sleep_for = (reset_ts - now + 2) if reset_ts and reset_ts > now else Params.rate_limit_fallback_sleep
        logger.warning("Rate limit hit; sleeping %ds", sleep_for)
        time.sleep(max(1, sleep_for))
    except Exception:
        logger.exception("Rate-limit header parse failed; sleeping default")
        time.sleep(Params.rate_limit_fallback_sleep)

# ---------- tweepy wrappers ----------
def _refs_to_dict_list(refs):
    if not refs:
        return None
    out = []
    for r in refs:
        rid = getattr(r, "id", None) if not isinstance(r, dict) else r.get("id")
        rtype = getattr(r, "type", None) if not isinstance(r, dict) else r.get("type")
        out.append({"id": int(rid) if rid is not None else None, "type": rtype})
    return out

def normalize_tweet(t, author_id: int, username: Optional[str]) -> Dict:
    pm = getattr(t, "public_metrics", {}) or {}
    entities = getattr(t, "entities", None)
    refs = _refs_to_dict_list(getattr(t, "referenced_tweets", None))
    return {
        "tweet_id": str(t.id),
        "author_id": int(author_id),
        "username": username,
        "created_at": t.created_at,  # aware dt
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
        "entities": entities,                # dict (JSONB)
        "referenced_tweets": refs,           # list[dict] (JSONB)
        "retrieved_at": datetime.now(timezone.utc),
    }

def fetch_since_pages(author_id: int, since_id: str, start_time: datetime):
    """Fetch tweets since a given tweet ID and after start_time."""
    return tweepy.Paginator(
        client.get_users_tweets,
        id=author_id,
        since_id=since_id,
        start_time=start_time,
        max_results=100,
        tweet_fields=Params.tweet_fields
    )

# ---------- insert ----------
def upsert_tweets_to_test(rows: List[Dict]) -> int:
    if not rows:
        return 0
    records = sanitize_rows(rows)
    with engine.begin() as conn:
        conn.execute(INSERT_TEST_TWEETS_STMT, records)
    return len(records)

# ---------- main ----------
def main():
    # Setup test table
    setup_test_table()

    profiles = get_all_profiles()
    total_available = len(profiles)

    # optional sampling
    n = int(Params.tweets_sample_limit)
    if n >= 0:
        if Params.sample_seed is not None:
            random.seed(int(Params.sample_seed))
        profiles = random.sample(profiles, min(n, total_available))

    logger.info(
        "Starting tweets fetch from %s into test table %s: selected %d profiles (out of %d).",
        START_DATE.date(), TEST_TABLE, len(profiles), total_available
    )

    total_upserts = 0
    for i, p in enumerate(profiles, start=1):
        aid = p["author_id"]; uname = p["username"]
        logger.info("Profile %d/%d: %s (%s)", i, len(profiles), uname, aid)

        try:
            # Get the latest tweet ID from main table
            last_id = get_latest_tweet_id_from_main(aid)

            if last_id is None:
                logger.warning("No tweets found in main table for %s (%s), skipping", uname, aid)
                continue

            # Fetch tweets since that ID, but only after START_DATE
            new_rows: List[Dict] = []
            for page in fetch_since_pages(aid, since_id=last_id, start_time=START_DATE):
                n = len(page.data) if page.data else 0
                logger.info("Author %s (%s): page with %d tweets", uname, aid, n)
                if page.data:
                    new_rows.extend(normalize_tweet(t, aid, uname) for t in page.data)

            inserted = upsert_tweets_to_test(new_rows)
            logger.info("Fetched %d new tweets for %s (%s)", inserted, uname, aid)
            total_upserts += inserted

        except tweepy.TooManyRequests as e:
            sleep_from_headers(getattr(e, "response", None))
            # retry this author - decrement counter
            continue
        except Exception:
            logger.exception("Unexpected error for author_id=%s", aid)

    logger.info("Done. Total tweets upserted to %s: %d", TEST_TABLE, total_upserts)

    # Print summary
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT
                COUNT(*) as total,
                MIN(created_at) as earliest,
                MAX(created_at) as latest
            FROM {TEST_TABLE}
        """))
        stats = result.fetchone()
        logger.info("=" * 80)
        logger.info("TEST TABLE SUMMARY: %s", TEST_TABLE)
        logger.info("=" * 80)
        logger.info("Total tweets: %d", stats[0])
        logger.info("Date range: %s to %s", stats[1], stats[2])
        logger.info("=" * 80)

if __name__ == "__main__":
    main()
