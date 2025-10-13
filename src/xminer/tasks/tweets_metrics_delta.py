# src/xminer/tasks/tweets_metrics_delta.py
from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# --- Project-style imports (align with your other tasks) ---
from ..io.db import engine                               # shared SQLAlchemy engine
from ..config.params import Params                       # parameters.yml access
from ..utils.global_helpers import (
    politicians_table_name,
    normalize_party,
    month_bounds,
    prev_year_month,
    _safe_div,
    build_outdir,
)

# Reuse metric builders from your tweets monthly task
from ..utils.metrics_helpers import (
    enrich_with_profiles,
    metric_individual_month,
    metric_party_month,
)

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/tweets_metrics_delta.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# -------------------------------
# SQL templates (same as tweets_metrics_monthly)
# -------------------------------
POSTGRES_LATEST_PROFILES_TMPL = r"""
WITH joined AS (
  SELECT
    xp.username,
    xp.x_user_id,
    xp.name,
    xp.created_at,
    xp.verified,
    xp.protected,
    xp.followers_count,
    xp.following_count,
    xp.tweet_count,
    xp.listed_count,
    xp.location,
    xp.description,
    xp.retrieved_at,
    p.partei_kurz,
    p.geschlecht,
    p.geburtsdatum,
    ROW_NUMBER() OVER (PARTITION BY lower(xp.username) ORDER BY xp.retrieved_at DESC) AS rn
  FROM {schema}.{x_profiles} xp
  JOIN {schema}.{politicians} p
    ON lower(xp.username) = lower(p.username)
)
SELECT *
FROM joined
WHERE rn = 1
"""

POSTGRES_TWEETS_MONTH_TMPL = r"""
SELECT
  t.tweet_id,
  t.author_id,
  t.username,
  t.created_at,
  t.text,
  t.lang,
  t.conversation_id,
  t.in_reply_to_user_id,
  t.possibly_sensitive,
  t.like_count,
  t.reply_count,
  t.retweet_count,
  t.quote_count,
  t.bookmark_count,
  t.impression_count,
  t.source,
  t.entities,
  t.referenced_tweets,
  t.retrieved_at,
  p.partei_kurz
FROM {schema}.{tweets} t
JOIN {schema}.{politicians} p
  ON lower(t.username) = lower(p.username)
WHERE t.created_at >= :start_ts
  AND t.created_at < :end_ts
"""

# -------------------------------
# Data loaders (mirroring tweets_metrics_monthly)
# -------------------------------
def load_latest_profiles(schema: str, x_profiles: str, month: int, year: int) -> pd.DataFrame:
    politicians = politicians_table_name(month, year)
    sql = POSTGRES_LATEST_PROFILES_TMPL.format(schema=schema, x_profiles=x_profiles, politicians=politicians)
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    # dtypes/cleanup
    if "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    if "retrieved_at" in df:
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], utc=True, errors="coerce")
    if "geburtsdatum" in df:
        df["geburtsdatum"] = pd.to_datetime(df["geburtsdatum"], utc=True, errors="coerce").dt.date
    if "username" in df:
        df["username"] = df["username"].astype(str).str.strip()
    return normalize_party(df)


def load_tweets_month(schema: str, tweets: str, month: int, year: int) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    start_ts, end_ts = month_bounds(year, month)
    politicians = politicians_table_name(month, year)
    sql = POSTGRES_TWEETS_MONTH_TMPL.format(schema=schema, tweets=tweets, politicians=politicians)
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params={"start_ts": start_ts, "end_ts": end_ts})
    # dtypes / cleanup
    if "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    if "retrieved_at" in df:
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], utc=True, errors="coerce")
    for c in ["like_count", "reply_count", "retweet_count", "quote_count", "bookmark_count", "impression_count"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "username" in df:
        df["username"] = df["username"].astype(str).str.strip()
    df = normalize_party(df)
    return df, start_ts, end_ts

# -------------------------------
# Delta helpers
# -------------------------------
def _build_monthly_author_table(schema: str, tweets_tbl: str, x_profiles_tbl: str, month: int, year: int) -> pd.DataFrame:
    """Return the per-author monthly table (metric_individual_month) for given year-month."""
    tweets, _, _ = load_tweets_month(schema, tweets_tbl, month, year)
    if tweets.empty:
        logger.warning("No tweets found for %04d-%02d.", year, month)
    profiles_latest = load_latest_profiles(schema, x_profiles_tbl, month, year)
    enriched = enrich_with_profiles(tweets, profiles_latest)  # follower-normalized fields, engagement rate, etc.
    return metric_individual_month(enriched)  # includes sums/means & followers_latest


def _build_monthly_party_table(schema: str, tweets_tbl: str, x_profiles_tbl: str, month: int, year: int) -> pd.DataFrame:
    """Return the party-level monthly aggregates (metric_party_month) for given year-month."""
    tweets, _, _ = load_tweets_month(schema, tweets_tbl, month, year)
    profiles_latest = load_latest_profiles(schema, x_profiles_tbl, month, year)
    enriched = enrich_with_profiles(tweets, profiles_latest)
    return metric_party_month(enriched)


def _join_and_delta(prev_df: pd.DataFrame, curr_df: pd.DataFrame, on: List[str], id_cols_keep: List[str]) -> pd.DataFrame:
    """
    Generic prev/curr join with automatic delta/pct across numeric columns.
    - inner-joins on `on`
    - carries over current 'partei_kurz' when available (author table)
    """
    merged = curr_df.merge(prev_df, on=on, how="inner", suffixes=("_curr", "_prev"))

    # If joining authors by 'username', keep current party tag when available
    if "partei_kurz_curr" in merged:
        merged["partei_kurz"] = merged["partei_kurz_curr"]
    elif "partei_kurz_prev" in merged:
        merged["partei_kurz"] = merged["partei_kurz_prev"]

    # Detect numeric pairs present in both prev/curr
    numeric_curr = merged.select_dtypes(include=[np.number]).columns
    candidates = [c[:-5] for c in numeric_curr if c.endswith("_curr")]  # base col names
    for base in candidates:
        c_prev, c_curr = f"{base}_prev", f"{base}_curr"
        if c_prev in merged.columns and c_curr in merged.columns:
            merged[f"delta_{base}"] = merged[c_curr] - merged[c_prev]
            merged[f"pct_{base}"] = _safe_div(merged[f"delta_{base}"], merged[c_prev].replace(0, np.nan))

    # Column ordering: ids + (all prev/curr pairs) + deltas/pcts
    id_cols = [c for c in id_cols_keep if c in merged.columns]
    # Bring over all useful columns
    delta_cols = sorted([c for c in merged.columns if c.startswith("delta_")])
    pct_cols = sorted([c for c in merged.columns if c.startswith("pct_")])
    # Keep prev/curr numeric plus key descriptors
    keep_pairs = []
    for base in candidates:
        for suffix in ("_prev", "_curr"):
            col = f"{base}{suffix}"
            if col in merged.columns:
                keep_pairs.append(col)

    ordered = id_cols + keep_pairs + delta_cols + pct_cols
    existing = [c for c in ordered if c in merged.columns]
    return merged[existing]


# -------------------------------
# Orchestration
# -------------------------------
def run(year: int, month: int, outdir: str, schema: str, tweets_tbl: str, x_profiles_tbl: str):
    """
    Compute month-over-month deltas for tweet metrics:
      - per-politician (username)
      - per-party
    and write CSVs tagged with the *current* month (YYYYMM).
    """
    outdir_tweets = build_outdir(outdir, year, month, "tweets")   # e.g., output/202509/tweets
    ym = f"{year:04d}{month:02d}"
    prev_y, prev_m = prev_year_month(year, month)

    logger.info("Building monthly tables: prev=%04d-%02d, curr=%04d-%02d", prev_y, prev_m, year, month)

    # Build monthly aggregates (prev & curr)
    prev_auth = _build_monthly_author_table(schema, tweets_tbl, x_profiles_tbl, prev_m, prev_y)
    curr_auth = _build_monthly_author_table(schema, tweets_tbl, x_profiles_tbl, month, year)

    prev_party = _build_monthly_party_table(schema, tweets_tbl, x_profiles_tbl, prev_m, prev_y)
    curr_party = _build_monthly_party_table(schema, tweets_tbl, x_profiles_tbl, month, year)

    # Guard rails
    if prev_auth.empty or curr_auth.empty:
        logger.warning("Author tables: prev=%d rows, curr=%d rows.", len(prev_auth), len(curr_auth))
    if prev_party.empty or curr_party.empty:
        logger.warning("Party tables: prev=%d rows, curr=%d rows.", len(prev_party), len(curr_party))

    # Join & compute deltas
    # Author-level join key: username
    author_delta = _join_and_delta(
        prev_df=prev_auth,
        curr_df=curr_auth,
        on=["username"],
        id_cols_keep=["username", "partei_kurz"],  # keep current party attribution if present
    )
    # Party-level join key: partei_kurz
    party_delta = _join_and_delta(
        prev_df=prev_party,
        curr_df=curr_party,
        on=["partei_kurz"],
        id_cols_keep=["partei_kurz"],
    )

    # Sort for presentation
    if "delta_engagement_sum" in author_delta.columns:
        author_delta = author_delta.sort_values("delta_engagement_sum", ascending=False, na_position="last")
    elif "delta_likes_sum" in author_delta.columns:
        author_delta = author_delta.sort_values("delta_likes_sum", ascending=False, na_position="last")

    if "delta_engagement_sum" in party_delta.columns:
        party_delta = party_delta.sort_values("delta_engagement_sum", ascending=False, na_position="last")
    elif "delta_tweets" in party_delta.columns:
        party_delta = party_delta.sort_values("delta_tweets", ascending=False, na_position="last")

    # Write
    out_auth = os.path.join(outdir_tweets, f"tweets_individual_delta_{ym}.csv")
    out_party = os.path.join(outdir_tweets, f"tweets_party_delta_{ym}.csv")
    author_delta.to_csv(out_auth, index=False)
    party_delta.to_csv(out_party, index=False)
    logger.info("Wrote author deltas -> %s (rows=%d)", out_auth, len(author_delta))
    logger.info("Wrote party deltas  -> %s (rows=%d)", out_party, len(party_delta))


# -------------------------------
# Entrypoint (parameters.yml only)
# -------------------------------
if __name__ == "__main__":
    # Align with other tasks: read from Params (year/month/outdir)
    year = int(getattr(Params, "year", datetime.now().year))
    month = int(getattr(Params, "month", datetime.now().month))
    outdir = getattr(Params, "outdir", "output")
    if not (1 <= month <= 12):
        raise SystemExit("Month must be in 1..12")

    # Hard-coded table identifiers to match your other tasks
    schema = "public"
    tweets_tbl = "tweets"
    x_profiles_tbl = "x_profiles"

    run(year, month, outdir, schema, tweets_tbl, x_profiles_tbl)
