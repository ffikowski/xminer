import os, logging
from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
import tweepy
from sqlalchemy import text

from .config.config import Config          # secrets: bearer, env
from .config.params import Params          # non-secrets: log file, sample_limit
from .db import engine              # shared engine

# logging setup from parameters.yml
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=getattr(logging, Params.logging_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", Params.logging_file)),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------- Tweepy client ----------
client = tweepy.Client(bearer_token=Config.BEARER, wait_on_rate_limit=True)


def read_usernames(limit: int):
    q = text("""SELECT username FROM politicians
                WHERE username IS NOT NULL AND username <> '' LIMIT :lim""")
    with engine.begin() as conn:
        return [r[0].lstrip("@") for r in conn.execute(q, {"lim": limit}).fetchall()]

def chunk(lst, n): 
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def fetch_batch(usernames):
    out = []
    try:
        resp = client.get_users(
            usernames=usernames,
            user_fields=["created_at","description","location","public_metrics","protected","verified"]
        )
        for u in resp.data or []:
            m = u.public_metrics or {}
            out.append({
                "username": u.username, "x_user_id": int(u.id), "name": u.name,
                "created_at": u.created_at, "verified": bool(getattr(u,"verified", False)),
                "protected": bool(getattr(u,"protected", False)),
                "followers_count": m.get("followers_count", 0),
                "following_count": m.get("following_count", 0),
                "tweet_count": m.get("tweet_count", 0),
                "listed_count": m.get("listed_count", 0),
                "location": getattr(u, "location", None),
                "description": getattr(u, "description", None),
                "retrieved_at": datetime.now(timezone.utc),
            })
        found = {u.username.lower() for u in (resp.data or [])}
        missing = [n for n in usernames if n.lower() not in found]
        if missing: logger.warning("Not found/suspended: %s", missing)
    except Exception:
        logger.exception("Batch failed for %s", usernames)
    return out

def main():
    names = read_usernames(Params.sample_limit)
    rows = []
    for group in chunk(names, min(Params.chunk_size, 100)):
        rows.extend(fetch_batch(group))
        logger.info("Fetched group=%d rows_total=%d", len(group), len(rows))

    df = pd.DataFrame(rows)
    os.makedirs("outputs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_csv = f"outputs/x_profiles_{ts}.csv"
    df.to_csv(out_csv, index=False)
    logger.info("Saved %s (rows=%d)", out_csv, len(df))

if __name__ == "__main__":
    main()
