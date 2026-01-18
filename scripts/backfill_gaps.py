#!/usr/bin/env python3
"""
Backfill script to fix missing tweets for users who likely hit the max_pages=5 limit.
These are users with 80+ tweets in our database for the period (close to 100 limit).
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
load_dotenv()

import requests
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = os.getenv("DATABASE_URL")
TWITTERAPIIO_API_KEY = os.getenv("TWITTERAPIIO_API_KEY") or os.getenv("twitterapiio_API_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")
if not TWITTERAPIIO_API_KEY:
    raise RuntimeError("TWITTERAPIIO_API_KEY not set")

engine = create_engine(DATABASE_URL)

# Parameters
TWEET_COUNT_THRESHOLD = 80  # Users with 80+ tweets likely hit the 100 limit
MAX_PAGES = 20  # Fetch up to 400 tweets per user
GAP_START = datetime(2026, 1, 9, tzinfo=timezone.utc)
GAP_END = datetime(2026, 1, 18, tzinfo=timezone.utc)


def fetch_tweets_twitterapiio(user_id: str, max_pages: int = 20):
    """Fetch tweets from TwitterAPI.io with pagination."""
    url = "https://api.twitterapi.io/twitter/user/last_tweets"
    headers = {"X-API-Key": TWITTERAPIIO_API_KEY}

    all_tweets = []
    cursor = None

    for page in range(max_pages):
        params = {"userId": user_id, "includeReplies": True}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        data_obj = data.get("data") or {}
        tweets = data_obj.get("tweets", []) if isinstance(data_obj, dict) else []
        all_tweets.extend(tweets)

        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor")
        if not cursor:
            break

    return all_tweets


def parse_tweet_datetime(dt_str):
    """Parse Twitter datetime format."""
    if not dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%a %b %d %H:%M:%S %z %Y")
    except:
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except:
            return None


def main():
    # Find users with 80+ tweets in the Jan 9-18 period (likely hit the 100 limit)
    prolific_query = """
    SELECT author_id, username, COUNT(*) as tweet_count
    FROM tweets
    WHERE created_at BETWEEN '2026-01-09' AND '2026-01-18'
    GROUP BY author_id, username
    HAVING COUNT(*) >= :threshold
    ORDER BY COUNT(*) DESC
    """

    with engine.connect() as conn:
        prolific_users = pd.read_sql(text(prolific_query), conn, params={"threshold": TWEET_COUNT_THRESHOLD})

    print(f"Found {len(prolific_users)} users with {TWEET_COUNT_THRESHOLD}+ tweets (likely hit limit)", flush=True)

    if len(prolific_users) == 0:
        print("No users need backfilling!", flush=True)
        return

    # Get existing tweet IDs to avoid duplicates
    existing_query = """
    SELECT DISTINCT tweet_id FROM tweets
    WHERE created_at BETWEEN '2026-01-09' AND '2026-01-18'
    """
    with engine.connect() as conn:
        existing_ids = set(pd.read_sql(text(existing_query), conn)['tweet_id'].astype(str))

    print(f"Found {len(existing_ids)} existing tweets in date range", flush=True)

    total_saved = 0

    for idx, row in prolific_users.iterrows():
        author_id = str(int(row['author_id']))
        username = row['username']
        current_count = int(row['tweet_count'])

        print(f"\n[{idx+1}/{len(prolific_users)}] Processing @{username} (author_id: {author_id}, current: {current_count} tweets)", flush=True)

        try:
            tweets = fetch_tweets_twitterapiio(author_id, max_pages=MAX_PAGES)
            print(f"  Fetched {len(tweets)} tweets from API", flush=True)

            new_tweets = []
            for t in tweets:
                tweet_id = t.get("id")
                if not tweet_id or str(tweet_id) in existing_ids:
                    continue

                created_at = parse_tweet_datetime(t.get("createdAt"))
                if not created_at:
                    continue

                if GAP_START <= created_at <= GAP_END:
                    new_tweets.append({
                        'tweet_id': tweet_id,
                        'author_id': author_id,
                        'username': username,
                        'text': t.get("text"),
                        'created_at': created_at,
                        'lang': t.get("lang"),
                        'conversation_id': t.get("conversationId"),
                        'in_reply_to_user_id': t.get("inReplyToUserId"),
                        'possibly_sensitive': t.get("possiblySensitive"),
                        'source': t.get("source"),
                        'like_count': t.get("likeCount", 0),
                        'reply_count': t.get("replyCount", 0),
                        'retweet_count': t.get("retweetCount", 0),
                        'quote_count': t.get("quoteCount", 0),
                        'bookmark_count': t.get("bookmarkCount", 0),
                        'impression_count': t.get("viewCount", 0),
                        'retrieved_at': datetime.now(timezone.utc),
                    })

            if new_tweets:
                df = pd.DataFrame(new_tweets)
                with engine.connect() as conn:
                    df.to_sql('tweets', conn, if_exists='append', index=False)
                    conn.commit()
                print(f"  Saved {len(new_tweets)} new tweets (total now: {current_count + len(new_tweets)})", flush=True)
                total_saved += len(new_tweets)
                existing_ids.update(str(t['tweet_id']) for t in new_tweets)
            else:
                print(f"  No new tweets found in gap period", flush=True)

        except Exception as e:
            print(f"  ERROR: {e}", flush=True)
            continue

    print(f"\n{'='*50}", flush=True)
    print(f"Backfill complete! Saved {total_saved} tweets total", flush=True)


if __name__ == "__main__":
    main()
