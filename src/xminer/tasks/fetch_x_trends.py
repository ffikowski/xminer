# src/xminer/tasks/fetch_x_trends.py
"""
Fetch X/Twitter trends for a location (by WOEID).

Uses DualAPIClient to support both:
- Official Twitter API (X_API_MODE=official)
- TwitterAPI.io (X_API_MODE=twitterapiio)

Usage:
    python -m xminer.tasks.fetch_x_trends
"""
import os, logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import text

from ..config.params import Params
from ..io.db import engine
from ..io.x_api_dual import client  # Use dual API client

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=getattr(logging, Params.logging_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/fetch_x_trends.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------- constants ----------
# Read from parameters.yml via Params (instead of env vars)
GERMANY_WOEID = int(getattr(Params, "trends_woeid", 23424829))
PLACE_NAME    = getattr(Params, "trends_place_name", "Germany")

# ---------- db helpers ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.x_trends (
    woeid           BIGINT       NOT NULL,
    place_name      TEXT         NOT NULL,
    trend_name      TEXT         NOT NULL,
    tweet_count     BIGINT,
    rank            INTEGER,
    retrieved_at    TIMESTAMPTZ  NOT NULL,
    source_version  TEXT         NOT NULL DEFAULT 'v2'
);
-- one row per (woeid, retrieved_at, trend_name) -> snapshot safe
CREATE UNIQUE INDEX IF NOT EXISTS ux_x_trends_woeid_time_name
ON public.x_trends (woeid, retrieved_at, trend_name);
"""

UPSERT_SQL = text("""
INSERT INTO public.x_trends
(woeid, place_name, trend_name, tweet_count, rank, retrieved_at, source_version)
VALUES (:woeid, :place_name, :trend_name, :tweet_count, :rank, :retrieved_at, :source_version)
ON CONFLICT (woeid, retrieved_at, trend_name) DO UPDATE
SET tweet_count = EXCLUDED.tweet_count,
    source_version = EXCLUDED.source_version
""")

def ensure_table():
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))

# ---------- api ----------
def fetch_trends(woeid: int) -> List[Dict[str, Any]]:
    """Fetch trends using the dual API client."""
    return client.get_trends(woeid)


def parse_tweet_count(meta_desc: str) -> int:
    """Parse tweet count from meta_description like '10.2K posts', '1.5M posts', or '2,867 posts'."""
    if not meta_desc:
        return None
    import re
    # Match formats like: "699K posts", "2,867 posts", "45.9K posts", "1.5M posts"
    match = re.search(r'([\d,.]+)([KMB]?)\s*(?:posts?|tweets?)?', meta_desc, re.IGNORECASE)
    if not match:
        return None
    num_str, suffix = match.groups()
    try:
        # Remove commas from numbers like "2,867"
        num_str = num_str.replace(',', '')
        num = float(num_str)
        multipliers = {'': 1, 'K': 1000, 'M': 1000000, 'B': 1000000000}
        return int(num * multipliers.get(suffix.upper(), 1))
    except ValueError:
        return None

# ---------- persistence ----------
def upsert_trends(woeid: int, place_name: str, items: List[Dict[str, Any]], source_version: str = "v2") -> int:
    if not items:
        return 0
    now = datetime.now(timezone.utc)
    rows = []
    for idx, it in enumerate(items, start=1):
        # Handle both official API format (tweet_count) and twitterapiio format (meta_description)
        tweet_count = it.get("tweet_count")
        if tweet_count is None and it.get("meta_description"):
            tweet_count = parse_tweet_count(it.get("meta_description"))

        rows.append({
            "woeid": woeid,
            "place_name": place_name,
            "trend_name": it.get("trend_name"),
            "tweet_count": tweet_count,
            "rank": it.get("rank") or idx,
            "retrieved_at": now,
            "source_version": source_version,
        })
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)
    return len(rows)

# ---------- main ----------
def main():
    from ..config.config import Config

    # Determine source version based on API mode
    source_version = "twitterapiio" if Config.X_API_MODE == "twitterapiio" else "v2"

    ensure_table()

    logger.info("=" * 60)
    logger.info("FETCH X TRENDS")
    logger.info("=" * 60)
    logger.info("WOEID: %s (%s)", GERMANY_WOEID, PLACE_NAME)
    logger.info("API Mode: %s", Config.X_API_MODE)

    try:
        items = fetch_trends(GERMANY_WOEID)
        logger.info("Fetched %d trends from API", len(items))

        # Log top 5 trends
        for t in items[:5]:
            count_str = t.get("tweet_count") or t.get("meta_description") or "N/A"
            logger.info("  #%s: %s (%s)", t.get("rank", "?"), t.get("trend_name"), count_str)

        n = upsert_trends(GERMANY_WOEID, PLACE_NAME, items, source_version)
        logger.info("Upserted %d trend rows into public.x_trends", n)
    except Exception:
        logger.exception("Trend fetch failed")
        raise

    logger.info("=" * 60)
    logger.info("TRENDS FETCH COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
