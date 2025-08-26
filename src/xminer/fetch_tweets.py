# src/xminer/fetch_tweets.py
"""
Tweets fetcher (no media) with explicit 15-min rate-limit sleeps + progress logging.

Run:
    python -m xminer.fetch_tweets

Prereqs:
    - Config.X_BEARER_TOKEN set (like in your profiles script)
    - DB engine available via xminer.db.engine
    - Create table once:

    CREATE TABLE IF NOT EXISTS tweets (
      tweet_id             BIGINT PRIMARY KEY,
      author_id            BIGINT NOT NULL,
      username             TEXT,
      created_at           TIMESTAMPTZ NOT NULL,
      text                 TEXT,
      lang                 TEXT,
      conversation_id      BIGINT,
      in_reply_to_user_id  BIGINT,
      possibly_sensitive   BOOLEAN,
      like_count           INTEGER,
      reply_count          INTEGER,
      retweet_count        INTEGER,
      quote_count          INTEGER,
      bookmark_count       INTEGER,
      impression_count     INTEGER,
      source               TEXT,
      entities             JSONB,
      referenced_tweets    JSONB,
      retrieved_at         TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS tweets_author_created_idx
      ON tweets (author_id, created_at DESC);
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import tweepy
from sqlalchemy import text

from .config.config import Config
from .db import engine

# ---------- Logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", "fetch_tweets.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------- Tweepy client ----------
# We want to manage sleeping ourselves to log it clearly:
client = tweepy.Client(
    bearer_token=Config.X_BEARER_TOKEN,
    wait_on_rate_limit=False  # manual handling so we can log sleeps
)

# Only text/metrics/metadata (NO media)
TWEET_FIELDS = [
    "created_at",
    "lang",
    "public_metrics",
    "conversation_id",
    "in_reply_to_user_id",
    "possibly_sensitive",
    "source",
    "entities",
    "referenced_tweets",
]

# Default sleep if headers aren’t present (approx 15 minutes)
DEFAULT_RATE_LIMIT_SLEEP = 15 * 60 + 1  # 901 sec


# ---------- DB helpers ----------
def get_all_profiles() -> List[Dict]:
    sql = text("""
        SELECT x_user_id, username
        FROM x_profiles
        WHERE x_user_id IS NOT NULL
        ORDER BY x_user_id
    """)
    with engine.begin() as conn:
        return [{"author_id": int(r[0]), "username": r[1]} for r in conn.execute(sql).fetchall()]

def get_latest_tweet_id(author_id: int) -> Optional[int]:
    sql = text("""
        SELECT tweet_id
        FROM tweets
        WHERE author_id = :aid
        ORDER BY created_at DESC
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"aid": author_id}).fetchone()
        return int(row[0]) if row else None

def upsert_tweets(rows: List[Dict]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    if df.empty:
        return 0
    df = df.where(pd.notnull(df), None)

    sql = text("""
        INSERT INTO tweets (
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
        ON CONFLICT (tweet_id) DO UPDATE SET
            author_id            = EXCLUDED.author_id,
            username             = EXCLUDED.username,
            created_at           = EXCLUDED.created_at,
            text                 = EXCLUDED.text,
            lang                 = EXCLUDED.lang,
            conversation_id      = EXCLUDED.conversation_id,
            in_reply_to_user_id  = EXCLUDED.in_reply_to_user_id,
            possibly_sensitive   = EXCLUDED.possibly_sensitive,
            like_count           = EXCLUDED.like_count,
            reply_count          = EXCLUDED.reply_count,
            retweet_count        = EXCLUDED.retweet_count,
            quote_count          = EXCLUDED.quote_count,
            bookmark_count       = EXCLUDED.bookmark_count,
            impression_count     = EXCLUDED.impression_count,
            source               = EXCLUDED.source,
            entities             = EXCLUDED.entities,
            referenced_tweets    = EXCLUDED.referenced_tweets,
            retrieved_at         = EXCLUDED.retrieved_at
    """)
    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))
    return len(df)


# ---------- Rate limit helper ----------
def sleep_from_headers(response) -> None:
    """
    Sleep until the window resets using response headers if available.
    Falls back to DEFAULT_RATE_LIMIT_SLEEP.
    """
    try:
        hdrs = response.headers if response is not None else {}
        limit = hdrs.get("x-rate-limit-limit")
        remaining = hdrs.get("x-rate-limit-remaining")
        reset = hdrs.get("x-rate-limit-reset")

        now = int(time.time())
        reset_ts = int(reset) if reset and reset.isdigit() else None
        sleep_for = (reset_ts - now + 2) if reset_ts and reset_ts > now else DEFAULT_RATE_LIMIT_SLEEP

        logger.warning(
            "Rate limit reached (limit=%s, remaining=%s, reset=%s). Sleeping for %d seconds.",
            limit, remaining, reset, sleep_for
        )
        time.sleep(max(1, sleep_for))
    except Exception:
        logger.exception("Failed to parse rate-limit headers; sleeping default %ds.", DEFAULT_RATE_LIMIT_SLEEP)
        time.sleep(DEFAULT_RATE_LIMIT_SLEEP)


# ---------- Fetch helpers ----------
def normalize_tweet(t, author_id: int, username: Optional[str]) -> Dict:
    pm = getattr(t, "public_metrics", {}) or {}
    return {
        "tweet_id": int(t.id),
        "author_id": int(author_id),
        "username": username,
        "created_at": t.created_at,
        "text": getattr(t, "text", None),
        "lang": getattr(t, "lang", None),
        "conversation_id": getattr(t, "conversation_id", None),
        "in_reply_to_user_id": getattr(t, "in_reply_to_user_id", None),
        "possibly_sensitive": getattr(t, "possibly_sensitive", None),
        "like_count": pm.get("like_count"),
        "reply_count": pm.get("reply_count"),
        "retweet_count": pm.get("retweet_count"),
        "quote_count": pm.get("quote_count"),
        # often absent on Basic for others' tweets:
        "bookmark_count": pm.get("bookmark_count"),
        "impression_count": pm.get("impression_count"),
        "source": getattr(t, "source", None),
        "entities": json.dumps(getattr(t, "entities", None)) if getattr(t, "entities", None) else None,
        "referenced_tweets": json.dumps(getattr(t, "referenced_tweets", None)) if getattr(t, "referenced_tweets", None) else None,
        "retrieved_at": datetime.now(timezone.utc),
    }

def fetch_last_100(author_id: int):
    return client.get_users_tweets(
        id=author_id,
        max_results=100,
        tweet_fields=TWEET_FIELDS
    )

def fetch_since_pages(author_id: int, since_id: int):
    return tweepy.Paginator(
        client.get_users_tweets,
        id=author_id,
        since_id=since_id,
        max_results=100,
        tweet_fields=TWEET_FIELDS
    )


# ---------- Main ----------
def main():
    profiles = get_all_profiles()
    total_profiles = len(profiles)
    logger.info("Starting tweets fetch for %d profiles", total_profiles)

    total_upserts = 0
    processed = 0

    for p in profiles:
        processed += 1
        remaining = total_profiles - processed
        aid = p["author_id"]
        uname = p["username"]

        logger.info("Profile %d/%d (remaining %d): %s (%s)",
                    processed, total_profiles, remaining, uname, aid)

        try:
            last_id = get_latest_tweet_id(aid)

            if last_id is None:
                # Initial: just one call for last 100
                while True:
                    try:
                        resp = fetch_last_100(aid)
                        break
                    except tweepy.TooManyRequests as e:
                        sleep_from_headers(getattr(e, "response", None))
                tweets = resp.data or []
                rows = [normalize_tweet(t, aid, uname) for t in tweets]
                n = upsert_tweets(rows)
                total_upserts += n
                logger.info("Initial fetch: upserted %d tweets for %s", n, uname)

            else:
                # Incremental: paginate since last_id
                new_rows: List[Dict] = []
                paginator = fetch_since_pages(aid, last_id)

                for page in paginator:
                    if page.data:
                        for t in page.data:
                            new_rows.append(normalize_tweet(t, aid, uname))
                    # If rate limited mid-pagination, Tweepy raises on next API call:
                    # we loop/sleep/retry the current author until window resets.
                # Upsert after finishing pages (or if none, n=0)
                n = upsert_tweets(new_rows)
                total_upserts += n
                logger.info("Incremental fetch since_id=%s: upserted %d tweets for %s",
                            last_id, n, uname)

        except tweepy.TooManyRequests as e:
            # If a 429 bubbles up (e.g., first call for an author), sleep and retry this author
            sleep_from_headers(getattr(e, "response", None))
            # decrement processed so the same profile is retried with correct progress numbering
            processed -= 1
            continue
        except Exception:
            logger.exception("Unexpected error for author_id=%s", aid)

    logger.info("Done. Total tweets upserted/updated: %d", total_upserts)


if __name__ == "__main__":
    main()
