# src/xminer/fetch_tweets.py
import os, time, logging, random, json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import tweepy
from sqlalchemy import text

from .config.config import Config
from .config.params import Params
from .db import engine
from .utils.tweets_helpers import sanitize_rows, INSERT_TWEETS_STMT

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/fetch_tweets.log", mode="w"),
              logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ---------- client ----------
client = tweepy.Client(bearer_token=Config.X_BEARER_TOKEN, wait_on_rate_limit=False)
TWEET_FIELDS = [
    "created_at","lang","public_metrics","conversation_id","in_reply_to_user_id",
    "possibly_sensitive","source","entities","referenced_tweets",
]
DEFAULT_RATE_LIMIT_SLEEP = 901

def _start_time():
    val = getattr(Params, "tweets_since", None)
    if not val:
        return None
    # assume isoformat with Z/offset provided upstream; tweepy wants aware dt
    return datetime.fromisoformat(val.replace("Z","+00:00"))

# ---------- db ----------
def get_all_profiles() -> List[Dict]:
    sql = text("SELECT x_user_id, username FROM x_profiles WHERE x_user_id IS NOT NULL ORDER BY x_user_id")
    with engine.begin() as conn:
        return [{"author_id": int(r[0]), "username": r[1]} for r in conn.execute(sql).fetchall()]

def get_latest_tweet_id(author_id: int) -> Optional[str]:
    sql = text("""
        SELECT tweet_id FROM tweets
        WHERE author_id = :aid
        ORDER BY created_at DESC
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"aid": author_id}).fetchone()
        return str(row[0]) if row else None

def author_already_fetched_on(author_id: int) -> bool:
    day_start = Params.skip_fetch_date
    if not day_start:
        return False
    day_end = day_start + timedelta(days=1)
    sql = text("""
        SELECT 1 FROM tweets
        WHERE author_id = :aid
          AND retrieved_at >= :start_ts
          AND retrieved_at <  :end_ts
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"aid": author_id, "start_ts": day_start, "end_ts": day_end}).fetchone()
        return row is not None

# ---------- rate-limit ----------
def sleep_from_headers(response) -> None:
    try:
        hdrs = response.headers if response is not None else {}
        reset = hdrs.get("x-rate-limit-reset")
        now = int(time.time())
        reset_ts = int(reset) if reset and reset.isdigit() else None
        sleep_for = (reset_ts - now + 2) if reset_ts and reset_ts > now else DEFAULT_RATE_LIMIT_SLEEP
        logger.warning("Rate limit hit; sleeping %ds", sleep_for)
        time.sleep(max(1, sleep_for))
    except Exception:
        logger.exception("Rate-limit header parse failed; sleeping default")
        time.sleep(DEFAULT_RATE_LIMIT_SLEEP)

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

def fetch_last_100(author_id: int, start_time=None):
    kwargs = {"start_time": start_time} if start_time else {}
    return client.get_users_tweets(id=author_id, max_results=100, tweet_fields=TWEET_FIELDS, **kwargs)

def fetch_since_pages(author_id: int, since_id: str):
    return tweepy.Paginator(
        client.get_users_tweets,
        id=author_id, since_id=since_id, max_results=100, tweet_fields=TWEET_FIELDS
    )

# ---------- insert ----------
def upsert_tweets(rows: List[Dict]) -> int:
    if not rows:
        return 0
    records = sanitize_rows(rows)
    with engine.begin() as conn:
        conn.execute(INSERT_TWEETS_STMT, records)
    return len(records)

# ---------- main ----------
def main():
    profiles = get_all_profiles()
    total_available = len(profiles)

    # optional sampling
    n = int(Params.tweets_sample_limit)
    if n >= 0:
        if Params.sample_seed is not None:
            random.seed(int(Params.sample_seed))
        profiles = random.sample(profiles, min(n, total_available))

    # start time cutoff from params
    start_time = _start_time()

    logger.info(
        "Starting tweets fetch: selected %d profiles (out of %d). sample_limit=%s seed=%s",
        len(profiles), total_available, Params.tweets_sample_limit, Params.sample_seed
    )

    total_upserts = 0
    for i, p in enumerate(profiles, start=1):
        aid = p["author_id"]; uname = p["username"]
        if Params.skip_fetch_date and author_already_fetched_on(aid):
            logger.info("Skipping %s (%s): already fetched on %s.", uname, aid, Params.skip_fetch_date.date())
            continue

        logger.info("Profile %d/%d: %s (%s)", i, len(profiles), uname, aid)

        try:
            last_id = get_latest_tweet_id(aid)

            if last_id is None:
                # initial
                while True:
                    try:
                        resp = fetch_last_100(aid, start_time=start_time)
                        break
                    except tweepy.TooManyRequests as e:
                        sleep_from_headers(getattr(e, "response", None))
                tweets = resp.data or []
                rows = [normalize_tweet(t, aid, uname) for t in tweets]
                inserted = upsert_tweets(rows)
                logger.info("Fetched %d tweets for %s (%s)", inserted, uname, aid)
                total_upserts += inserted
            else:
                # incremental
                new_rows: List[Dict] = []
                for page in fetch_since_pages(aid, since_id=last_id):
                    n = len(page.data) if page.data else 0
                    logger.info("Author %s (%s): page with %d tweets", uname, aid, n)
                    if page.data:
                        new_rows.extend(normalize_tweet(t, aid, uname) for t in page.data)
                inserted = upsert_tweets(rows)
                logger.info("Fetched %d tweets for %s (%s)", inserted, uname, aid)
                total_upserts += inserted

        except tweepy.TooManyRequests as e:
            sleep_from_headers(getattr(e, "response", None))
            # retry this author
            continue
        except Exception:
            logger.exception("Unexpected error for author_id=%s", aid)

    logger.info("Done. Total tweets upserted/updated: %d", total_upserts)

if __name__ == "__main__":
    main()
