from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import List

import pandas as pd
from sqlalchemy import text

# --- Project-style imports (match your existing tasks) ---
from ..io.db import engine  # central engine from Config.DATABASE_URL
from ..config.params import Params  # parameters class used in production

from ..utils.global_helpers import politicians_table_name, normalize_party, UNION_MAP, month_bounds, _safe_div, build_outdir
# --- add/replace this import block near the top ---
from ..utils.metrics_helpers import (
    MetricSpec,
    enrich_with_profiles,
    # summaries
    metric_individual_month,
    metric_party_month,
    # leaderboards (absolute)
    metric_top_tweets,  # engagement_rate (canonical top)
    metric_bottom_tweets_by_engagement_rate,
    metric_top_tweets_by_likes,
    metric_top_tweets_by_retweets,
    metric_top_tweets_by_replies,
    metric_top_tweets_by_quotes,
    metric_top_tweets_by_bookmarks,
    metric_top_tweets_by_impressions,
    # follower-normalized
    metric_top_tweets_by_likes_per_1k,
    metric_top_tweets_by_engagement_per_1k,
    metric_bottom_tweets_by_engagement_per_1k,
    # controversy / “shitstorm”
    metric_most_controversial,
    metric_most_reply_heavy,
    metric_most_quote_heavy,
    metric_most_amplified_debate,
    metric_most_controversial_by_like_to_reply,
    # conversion patterns
    metric_low_conversion_high_reach,
    metric_silent_hits,
    # author-level
    metric_top_authors_by_avg_engagement_rate,
    metric_most_active_authors,
)

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

def load_latest_profiles(schema: str, x_profiles: str, month: int, year: int) -> pd.DataFrame:
    politicians = politicians_table_name(month, year)
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

    # NEW: normalize CDU/CSU union
    df = normalize_party(df)
    return df

def load_tweets_month(schema: str, tweets: str, month: int, year: int, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
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
    return df


# -------------------------------
# Orchestration
# -------------------------------
# --- replace your build_metrics(...) with this ---
def build_metrics(top_n: int) -> List[MetricSpec]:
    return [
        # Account / party summaries
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

        # Core engagement-rate boards
        MetricSpec(
            name="tweets_top_by_engagement_rate",
            description=f"Top {top_n} tweets by engagement rate in the month",
            compute=lambda df: metric_top_tweets(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_bottom_by_engagement_rate",
            description=f"Bottom {top_n} tweets by engagement rate (min reach guard)",
            compute=lambda df: metric_bottom_tweets_by_engagement_rate(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_likes",
            description=f"Top {top_n} tweets by likes",
            compute=lambda df: metric_top_tweets_by_likes(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_retweets",
            description=f"Top {top_n} tweets by retweets",
            compute=lambda df: metric_top_tweets_by_retweets(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_replies",
            description=f"Top {top_n} tweets by replies",
            compute=lambda df: metric_top_tweets_by_replies(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_quotes",
            description=f"Top {top_n} tweets by quotes",
            compute=lambda df: metric_top_tweets_by_quotes(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_bookmarks",
            description=f"Top {top_n} tweets by bookmarks",
            compute=lambda df: metric_top_tweets_by_bookmarks(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_impressions",
            description=f"Top {top_n} tweets by impressions",
            compute=lambda df: metric_top_tweets_by_impressions(df, top_n=top_n),
        ),

        # Follower-normalized leaderboards
        MetricSpec(
            name="tweets_top_by_likes_per_1k_followers",
            description=f"Top {top_n} tweets by likes per 1k followers",
            compute=lambda df: metric_top_tweets_by_likes_per_1k(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_top_by_engagement_per_1k_followers",
            description=f"Top {top_n} tweets by engagement per 1k followers",
            compute=lambda df: metric_top_tweets_by_engagement_per_1k(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_bottom_by_engagement_per_1k_followers",
            description=f"Bottom {top_n} tweets by engagement per 1k followers (min reach guard)",
            compute=lambda df: metric_bottom_tweets_by_engagement_per_1k(df, top_n=top_n),
        ),

        # Controversy / “shitstorm” indicators
        MetricSpec(
            name="tweets_most_controversial",
            description=f"Top {top_n} most controversial tweets ((replies+quotes) / likes)",
            compute=lambda df: metric_most_controversial(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_most_reply_heavy",
            description=f"Top {top_n} by reply share of engagement (replies / engagement_total)",
            compute=lambda df: metric_most_reply_heavy(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_most_quote_heavy",
            description=f"Top {top_n} by quote share of engagement (quotes / engagement_total)",
            compute=lambda df: metric_most_quote_heavy(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_most_amplified_debate",
            description=f"Top {top_n} by amplification rate ((retweets+quotes) / impressions)",
            compute=lambda df: metric_most_amplified_debate(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_most_controversial_by_like_to_reply",
            description=f"Top {top_n} most controversial (lowest like-to-reply ratio)",
            compute=lambda df: metric_most_controversial_by_like_to_reply(df, top_n=top_n),
        ),

        # Conversion patterns
        MetricSpec(
            name="tweets_low_conversion_high_reach",
            description=f"Top {top_n} low-conversion tweets (high impressions, low engagement rate)",
            compute=lambda df: metric_low_conversion_high_reach(df, top_n=top_n),
        ),
        MetricSpec(
            name="tweets_silent_hits",
            description=f"Top {top_n} silent hits (high engagement rate at low reach)",
            compute=lambda df: metric_silent_hits(df, top_n=top_n),
        ),

        # Author-level
        MetricSpec(
            name="authors_top_avg_engagement_rate",
            description=f"Top {top_n} authors by avg engagement rate (min tweets threshold inside)",
            compute=lambda df: metric_top_authors_by_avg_engagement_rate(df, top_n=top_n),
        ),
        MetricSpec(
            name="authors_most_active",
            description=f"Top {top_n} most active authors (tweets this month)",
            compute=lambda df: metric_most_active_authors(df, top_n=top_n),
        ),
    ]


def run(year: int, month: int, outdir: str, schema: str, tweets_tbl: str, x_profiles_tbl: str, top_n: int):
    outdir_tweets = build_outdir(outdir, year, month, "tweets")
    ym = f"{year:04d}{month:02d}"

    # bounds for month (UTC)
    start_ts, end_ts = month_bounds(year, month)
    logger.info("Computing metrics for %s to %s (UTC)", start_ts.isoformat(), end_ts.isoformat())

    # load
    prof_latest = load_latest_profiles(schema=schema, x_profiles=x_profiles_tbl, month=month, year=year)
    tweets_month = load_tweets_month(schema=schema, tweets=tweets_tbl, month=month, year=year, start_ts=start_ts, end_ts=end_ts)
    logger.info("Partei_kurz values in latest profiles: %s", prof_latest["partei_kurz"].dropna().unique())
    logger.info("Partei_kurz values in tweets_month: %s", tweets_month["partei_kurz"].dropna().unique())
    if tweets_month.empty:
        logger.warning("No tweets found for %04d-%02d. Outputs will be empty.", year, month)
        

    # enrich tweets with latest followers etc. for follower-normalized metrics
    dataset = enrich_with_profiles(tweets_month, prof_latest)
    logger.info("Partei_kurz values in dataset: %s", dataset["partei_kurz"].dropna().unique())

    # compute & write
    for spec in build_metrics(top_n=top_n):
        df_metric = spec.compute(dataset)
        out_path = os.path.join(outdir_tweets, f"{spec.name}_{ym}.csv")
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
    x_profiles_tbl = "x_profiles"

    run(year, month, outdir, schema, tweets_tbl, x_profiles_tbl, top_n)
