import os, logging, re
from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
import tweepy
from sqlalchemy import text

from ..config.params import Params          # non-secrets: log file, sample_limit, etc.
from ..io.db import engine                     # shared engine
from ..io.x_api import client 
# ---------- Logging (from parameters.yml) ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=getattr(logging, Params.logging_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", Params.logging_file), mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------- Main logic ----------

import re

def _politicians_table_name(month: int, year: int) -> str:
    # Coerce and zero-pad month, e.g. 8 -> "08"
    mm = f"{int(month):02d}"
    yyyy = f"{int(year):04d}"
    tbl = f'politicians_{mm}_{yyyy}'
    # extra safety: only allow names like politicians_08_2025
    if not re.fullmatch(r"politicians_\d{2}_\d{4}", tbl):
        raise ValueError(f"Invalid table name: {tbl}")
    return tbl


def read_usernames(limit: int | None):
    tbl = _politicians_table_name(Params.month, Params.year)  # e.g., "politicians_08_2025"

    if limit is None or limit < 0:
        q = text(f"""
            SELECT username
            FROM public."{tbl}"
            WHERE username IS NOT NULL AND username <> '' AND username <> 'gelöscht'
        """)
        params = {}
    else:
        q = text(f"""
            SELECT username
            FROM public."{tbl}"
            WHERE username IS NOT NULL AND username <> '' AND username <> 'gelöscht'
            LIMIT :lim
        """)
        params = {"lim": limit}

    with engine.begin() as conn:
        return [str(r[0]).lstrip("@") for r in conn.execute(q, params).fetchall()]

def chunk(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def fetch_batch(usernames):
    out: List[Dict] = []
    try:
        resp = client.get_users(
            usernames=usernames,
            user_fields=["created_at","description","location","public_metrics","protected","verified"]
        )
        for u in resp.data or []:
            m = u.public_metrics or {}
            out.append({
                "username": u.username,
                "x_user_id": int(u.id),
                "name": u.name,
                "created_at": u.created_at,
                "verified": bool(getattr(u,"verified", False)),
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
        if missing:
            logger.warning("Not found/suspended: %s", missing)
    except Exception:
        logger.exception("Batch failed for %s", usernames)
    return out

def upsert_x_profiles(df: pd.DataFrame) -> int:
    """UPSERT rows into x_profiles on (x_user_id). Returns rows written."""
    if df.empty:
        return 0
    # Drop rows without key
    df = df[df["x_user_id"].notna()].copy()
    if df.empty:
        return 0

    # Ensure None instead of NaN for SQL, and proper dtypes
    df = df.where(pd.notnull(df), None)

    rows = df.to_dict(orient="records")
    sql = text("""
        INSERT INTO x_profiles (
            x_user_id, username, name, created_at, verified, protected,
            followers_count, following_count, tweet_count, listed_count,
            location, description, retrieved_at
        ) VALUES (
            :x_user_id, :username, :name, :created_at, :verified, :protected,
            :followers_count, :following_count, :tweet_count, :listed_count,
            :location, :description, :retrieved_at
        )
        ON CONFLICT (x_user_id, retrieved_at) DO NOTHING;
    """)


    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)

def main():
    names = read_usernames(None if Params.sample_limit == -1 else Params.sample_limit)

    rows: List[Dict] = []
    for group in chunk(names, min(Params.chunk_size, 100)):
        rows.extend(fetch_batch(group))
        logger.info("Fetched group=%d rows_total=%d", len(group), len(rows))

    df = pd.DataFrame(rows, columns=[
        "username","x_user_id","name","created_at","verified","protected",
        "followers_count","following_count","tweet_count","listed_count",
        "location","description","retrieved_at"
    ])

    # (A) Optional CSV on VPS
    if Params.store_csv:
        os.makedirs("outputs", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_csv = f"outputs/x_profiles_{ts}.csv"
        df.to_csv(out_csv, index=False)
        logger.info("Saved %s (rows=%d)", out_csv, len(df))
    else:
        logger.info("CSV saving disabled (store_csv=false). Rows=%d", len(df))

    # (B) Optional upsert to Neon
    if Params.load_to_db:
        n = upsert_x_profiles(df)
        logger.info("Upserted %d rows into x_profiles", n)
    else:
        logger.info("DB loading disabled (load_to_db=false).")

if __name__ == "__main__":
    main()
