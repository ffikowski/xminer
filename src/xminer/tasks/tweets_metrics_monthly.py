from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# --- Project-style imports (match your existing tasks) ---
from ..io.db import engine  # central engine from Config.DATABASE_URL
from ..config.params import Params  # parameters class used in production

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/tweets_metrics_monthly.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# -------------------------------
# Helpers
# -------------------------------
@dataclass
class MetricSpec:
    name: str  # slug used in filename
    description: str
    compute: callable  # function(df) -> DataFrame

def _safe_div(a, b):
    with np.errstate(divide="ignore", invalid="ignore"):
        res = np.divide(a, b)
    return np.where(~np.isfinite(res), np.nan, res)

def _month_bounds(year: int, month: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    if month == 12:
        end = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
    else:
        end = pd.Timestamp(year=year, month=month + 1, day=1, tz="UTC")
    return start, end

# -------------------------------
# Data access
# -------------------------------
# latest x_profile per username joined with politician attributes (for followers etc.)
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

# tweets for a given month joined to politicians (party) by username
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

def load_latest_profiles(schema: str, x_profiles: str, politicians: str) -> pd.DataFrame:
    sql = POSTGRES_LATEST_PROFILES_TMPL.format(schema=schema, x_profiles=x_profiles, politicians=politicians)
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    if "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    if "retrieved_at" in df:
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], utc=True, errors="coerce")
    if "geburtsdatum" in df:
        df["geburtsdatum"] = pd.to_datetime(df["geburtsdatum"], utc=True, errors="coerce").dt.date
    if "username" in df:
        df["username"] = df["username"].astype(str).str.strip()
    return df

def load_tweets_month(schema: str, tweets: str, politicians: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
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
    return df

# -------------------------------
# Metric computation
# -------------------------------
def enrich_with_profiles(tweets_df: pd.DataFrame, prof_df: pd.DataFrame) -> pd.DataFrame:
    """Attach latest profile fields used for follower-based ratios to each tweet."""
    use_cols = [c for c in ["username", "name", "followers_count", "following_count", "tweet_count", "listed_count", "verified", "protected"] if c in prof_df.columns]
    prof_small = prof_df[use_cols].drop_duplicates("username") if "username" in prof_df else prof_df
    out = tweets_df.merge(prof_small, on="username", how="left", suffixes=("", "_profile"))
    # precompute per-tweet engagement components
    for c in ["like_count", "reply_count", "retweet_count", "quote_count", "bookmark_count", "impression_count"]:
        if c not in out:
            out[c] = np.nan
    out["engagement_total"] = out[["like_count", "reply_count", "retweet_count", "quote_count", "bookmark_count"]].sum(axis=1, min_count=1)
    out["engagement_rate"] = _safe_div(out["engagement_total"], out["impression_count"])
    out["like_to_reply"] = _safe_div(out["like_count"], out["reply_count"])
    out["retweet_to_like"] = _safe_div(out["retweet_count"], out["like_count"])
    # follower-normalized per tweet
    followers_k = _safe_div(out["followers_count"], 1000.0)
    out["likes_per_1k_followers"] = _safe_div(out["like_count"], followers_k)
    out["engagement_per_1k_followers"] = _safe_div(out["engagement_total"], followers_k)
    return out

def metric_individual_month(out: pd.DataFrame) -> pd.DataFrame:
    """Per-politician metrics for the month (averages per post, ratios, follower-normalized)."""
    if "username" not in out.columns:
        logger.warning("metric_individual_month skipped: 'username' column missing")
        return pd.DataFrame()

    g = out.groupby(["partei_kurz", "username"], dropna=False)

    agg = g.agg(
        n_tweets=("tweet_id", "count"),
        likes_sum=("like_count", "sum"),
        likes_mean=("like_count", "mean"),
        replies_sum=("reply_count", "sum"),
        replies_mean=("reply_count", "mean"),
        retweets_sum=("retweet_count", "sum"),
        retweets_mean=("retweet_count", "mean"),
        quotes_sum=("quote_count", "sum"),
        quotes_mean=("quote_count", "mean"),
        bookmarks_sum=("bookmark_count", "sum"),
        bookmarks_mean=("bookmark_count", "mean"),
        impressions_sum=("impression_count", "sum"),
        impressions_mean=("impression_count", "mean"),
        engagement_sum=("engagement_total", "sum"),
        engagement_mean=("engagement_total", "mean"),
        engagement_rate_mean=("engagement_rate", "mean"),
        like_to_reply_mean=("like_to_reply", "mean"),
        retweet_to_like_mean=("retweet_to_like", "mean"),
        likes_per_1k_followers_mean=("likes_per_1k_followers", "mean"),
        engagement_per_1k_followers_mean=("engagement_per_1k_followers", "mean"),
        verified_share=("verified", "mean"),
        protected_share=("protected", "mean"),
        followers_latest=("followers_count", "max"),
    ).reset_index()

    # Derived stable ratios (across totals)
    agg["like_to_reply_total_ratio"] = _safe_div(agg["likes_sum"], agg["replies_sum"])
    agg["retweet_to_like_total_ratio"] = _safe_div(agg["retweets_sum"], agg["likes_sum"])
    agg["engagement_rate_total"] = _safe_div(agg["engagement_sum"], agg["impressions_sum"])

    # presentation order
    cols = [
        "partei_kurz", "username", "n_tweets",
        "likes_mean", "replies_mean", "retweets_mean", "quotes_mean", "bookmarks_mean", "impressions_mean",
        "engagement_mean", "engagement_rate_mean",
        "like_to_reply_mean", "retweet_to_like_mean",
        "likes_per_1k_followers_mean", "engagement_per_1k_followers_mean",
        "likes_sum", "replies_sum", "retweets_sum", "quotes_sum", "bookmarks_sum", "impressions_sum", "engagement_sum",
        "like_to_reply_total_ratio", "retweet_to_like_total_ratio", "engagement_rate_total",
        "followers_latest", "verified_share", "protected_share",
    ]
    cols = [c for c in cols if c in agg.columns]
    result = agg[cols].sort_values(["partei_kurz", "n_tweets"], ascending=[True, False])
    logger.info("Computed metric_individual_month with %d rows", len(result))
    return result

def metric_party_month(out: pd.DataFrame) -> pd.DataFrame:
    """Party-level monthly aggregates across all tweets in the month."""
    if "partei_kurz" not in out.columns:
        logger.warning("metric_party_month skipped: 'partei_kurz' column missing")
        return pd.DataFrame()

    g = out.groupby("partei_kurz", dropna=False)

    summary = g.agg(
        tweets=("tweet_id", "count"),
        likes_sum=("like_count", "sum"),
        replies_sum=("reply_count", "sum"),
        retweets_sum=("retweet_count", "sum"),
        quotes_sum=("quote_count", "sum"),
        bookmarks_sum=("bookmark_count", "sum"),
        impressions_sum=("impression_count", "sum"),
        engagement_sum=("engagement_total", "sum"),
        engagement_rate_mean=("engagement_rate", "mean"),
        like_to_reply_mean=("like_to_reply", "mean"),
        retweet_to_like_mean=("retweet_to_like", "mean"),
        likes_per_1k_followers_mean=("likes_per_1k_followers", "mean"),
        engagement_per_1k_followers_mean=("engagement_per_1k_followers", "mean"),
        verified_share=("verified", "mean"),
        protected_share=("protected", "mean"),
    )

    # Totals-based engagement rate (robust vs mean of per-tweet rates)
    summary["engagement_rate_total"] = _safe_div(summary["engagement_sum"], summary["impressions_sum"])
    result = summary.reset_index().sort_values("engagement_sum", ascending=False)
    logger.info("Computed metric_party_month with %d rows", len(result))
    return result

def metric_top_tweets(out: pd.DataFrame, top_n: int = 50) -> pd.DataFrame:
    """Top tweets of the month by engagement rate, then by absolute engagement."""
    keep = [
        "tweet_id", "username", "partei_kurz", "created_at", "text", "lang",
        "like_count", "reply_count", "retweet_count", "quote_count", "bookmark_count",
        "impression_count", "engagement_total", "engagement_rate",
        "likes_per_1k_followers", "engagement_per_1k_followers",
    ]
    keep = [c for c in keep if c in out.columns]
    df = out[keep].copy()
    df = df.sort_values(["engagement_rate", "engagement_total"], ascending=[False, False]).head(top_n).reset_index(drop=True)
    logger.info("Computed metric_top_tweets with %d rows (top_n=%d)", len(df), top_n)
    return df

# -------------------------------
# Orchestration
# -------------------------------
def build_metrics(top_n: int) -> List[MetricSpec]:
    return [
        MetricSpec(
            name="tweets_individual_month",
            description="Per-politician monthly tweet metrics (averages, ratios, follower-normalized)",
            compute=metric_individual_month,
        ),
        MetricSpec(
            name="tweets_party_month",
            description="Party-level monthly tweet aggregates and rates",
            compute=metric_party_month,
        ),
        MetricSpec(
            name="tweets_top_tweets",
            description=f"Top {top_n} tweets by engagement rate in the month",
            compute=lambda df: metric_top_tweets(df, top_n=top_n),
        ),
    ]

def run(year: int, month: int, outdir: str, schema: str, tweets_tbl: str, politicians_tbl: str, x_profiles_tbl: str, top_n: int):
    os.makedirs(outdir, exist_ok=True)
    ym = f"{year:04d}{month:02d}"

    # bounds for month (UTC)
    start_ts, end_ts = _month_bounds(year, month)
    logger.info("Computing metrics for %s to %s (UTC)", start_ts.isoformat(), end_ts.isoformat())

    # load
    prof_latest = load_latest_profiles(schema=schema, x_profiles=x_profiles_tbl, politicians=politicians_tbl)
    tweets_month = load_tweets_month(schema=schema, tweets=tweets_tbl, politicians=politicians_tbl, start_ts=start_ts, end_ts=end_ts)

    if tweets_month.empty:
        logger.warning("No tweets found for %04d-%02d. Outputs will be empty.", year, month)

    # enrich tweets with latest followers etc. for follower-normalized metrics
    dataset = enrich_with_profiles(tweets_month, prof_latest)

    # compute & write
    for spec in build_metrics(top_n=top_n):
        df_metric = spec.compute(dataset)
        out_path = os.path.join(outdir, f"{spec.name}_{ym}.csv")
        df_metric.to_csv(out_path, index=False)
        logger.info("Wrote %s -> %s", spec.description, out_path)

# -------------------------------
# Entrypoint (parameters.yml only)
# -------------------------------
if __name__ == "__main__":
    # Read all parameters from parameters.yml via Params
    year = int(getattr(Params, "year", datetime.now().year))
    month = int(getattr(Params, "month", datetime.now().month))
    outdir = getattr(Params, "outdir", "output")
    top_n = int(getattr(Params, "top_n", 50))
    if not (1 <= month <= 12):
        raise SystemExit("Month must be in 1..12")

    # Hard-coded table identifiers per request
    schema = "public"
    tweets_tbl = "tweets"
    politicians_tbl = "politicians"
    x_profiles_tbl = "x_profiles"

    run(year, month, outdir, schema, tweets_tbl, politicians_tbl, x_profiles_tbl, top_n)
