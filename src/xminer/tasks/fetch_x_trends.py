# src/xminer/tasks/fetch_x_trends.py
import os, logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
import pandas as pd
from sqlalchemy import text

from ..config.params import Params                 # keep consistency with other tasks
from ..io.db import engine                         # shared SQLAlchemy engine (Neon)
from ..config.config import Config                 # env: DATABASE_URL, X_BEARER_TOKEN

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
# X API v2 Trends by WOEID (see docs)
# https://docs.x.com/x-api/trends/trends-by-woeid/introduction
TRENDS_URL_TMPL = "https://api.x.com/2/trends/by/woeid/{woeid}"

# Germany WOEID (country)
# Example list (also shown on v1.1 page): Germany: 23424829
GERMANY_WOEID = int(os.getenv("TRENDS_WOEID", "23424829"))
PLACE_NAME = os.getenv("TRENDS_PLACE_NAME", "Germany")

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
def fetch_trends_v2(woeid: int, bearer_token: str) -> List[Dict[str, Any]]:
    url = TRENDS_URL_TMPL.format(woeid=woeid)
    headers = {"Authorization": f"Bearer {bearer_token}"}
    r = requests.get(url, headers=headers, timeout=30)
    try:
        r.raise_for_status()
    except Exception:
        logger.error("Trends call failed (%s): %s", r.status_code, r.text)
        raise
    payload = r.json() or {}
    data = payload.get("data") or []   # [{"trend_name": "...", "tweet_count": 1234}, ...]
    if not isinstance(data, list):
        logger.warning("Unexpected payload structure: %s", payload)
        return []
    return data

# ---------- persistence ----------
def upsert_trends(woeid: int, place_name: str, items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    now = datetime.now(timezone.utc)
    rows = []
    for idx, it in enumerate(items, start=1):
        rows.append({
            "woeid": woeid,
            "place_name": place_name,
            "trend_name": it.get("trend_name"),
            "tweet_count": it.get("tweet_count"),
            "rank": idx,
            "retrieved_at": now,
            "source_version": "v2",
        })
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)
    return len(rows)

# ---------- main ----------
def main():
    # Validate token once (same pattern as other tasks using Config)
    token = Config.X_BEARER_TOKEN
    if not token:
        raise SystemExit("Missing X_BEARER_TOKEN in environment.")

    ensure_table()

    logger.info("Fetching Trends v2 for WOEID=%s (%s)", GERMANY_WOEID, PLACE_NAME)
    try:
        items = fetch_trends_v2(GERMANY_WOEID, token)
        n = upsert_trends(GERMANY_WOEID, PLACE_NAME, items)
        logger.info("Upserted %d trend rows into public.x_trends", n)
    except Exception:
        logger.exception("Trend fetch failed")

if __name__ == "__main__":
    main()
