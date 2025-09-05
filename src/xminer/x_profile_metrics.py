from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import text

# --- Project-style imports (match fetch_tweets) ---
from .db import engine  # central engine built from Config.DATABASE_URL
from .config.params import Params  # parameters class already used in production

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/x_profile_metrics.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# -------------------------------
# Data access
# -------------------------------
POSTGRES_LATEST_SQL_TMPL = r"""
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


def load_latest_profiles(schema: str, x_profiles: str, politicians: str) -> pd.DataFrame:
    """Return one latest row per username joined with politician attributes."""
    sql = POSTGRES_LATEST_SQL_TMPL.format(schema=schema, x_profiles=x_profiles, politicians=politicians)
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    # Ensure expected dtypes
    if "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    if "retrieved_at" in df:
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], utc=True, errors="coerce")
    if "geburtsdatum" in df:
        df["geburtsdatum"] = pd.to_datetime(df["geburtsdatum"], utc=True, errors="coerce").dt.date
    # Normalize username case
    if "username" in df:
        df["username"] = df["username"].astype(str).str.strip()
    return df


# -------------------------------
# Metric computation
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


def metric_individual_base(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Account age in days (relative to now)
    now = pd.Timestamp.now(tz=timezone.utc)
    if "created_at" in out:
        out["account_age_days"] = (now - out["created_at"]).dt.days
        out.loc[out["account_age_days"] < 0, "account_age_days"] = np.nan
    else:
        out["account_age_days"] = np.nan

    # Ratios
    denom_following = out["following_count"].replace(0, np.nan) if "following_count" in out else np.nan
    denom_tweets = out["tweet_count"].replace(0, np.nan) if "tweet_count" in out else np.nan
    denom_age = out["account_age_days"].replace(0, np.nan)

    out["follow_ratio"] = _safe_div(out.get("followers_count"), denom_following)
    out["followers_per_tweet"] = _safe_div(out.get("followers_count"), denom_tweets)
    out["followers_per_day"] = _safe_div(out.get("followers_count"), denom_age)

    cols = [
        "username", "name", "partei_kurz", "verified", "protected",
        "followers_count", "following_count", "tweet_count", "listed_count",
        "account_age_days", "followers_per_day", "follow_ratio", "followers_per_tweet",
        "created_at", "retrieved_at"
    ]
    existing_cols = [c for c in cols if c in out.columns]
    sort_cols = [c for c in ["partei_kurz", "followers_count"] if c in out.columns]
    result = out[existing_cols].sort_values(sort_cols, ascending=[True, False] if len(sort_cols)==2 else False)
    logger.info("Computed metric_individual_base with %d rows", len(result))
    return result

def metric_party_summary(df: pd.DataFrame) -> pd.DataFrame:
    if "party" not in df.columns:
        logger.warning("metric_party_summary skipped: 'party' column missing")
        return pd.DataFrame()

    g = df.groupby("party", dropna=False)

    # start with a simple members count
    summary = g.size().rename("members").to_frame()

    # add aggregations only if those columns exist
    if "followers_count" in df.columns:
        summary["followers_sum"] = g["followers_count"].sum()
        summary["followers_mean"] = g["followers_count"].mean()
        summary["followers_median"] = g["followers_count"].median()

    if "following_count" in df.columns:
        summary["following_mean"] = g["following_count"].mean()

    if "tweet_count" in df.columns:
        summary["tweet_mean"] = g["tweet_count"].mean()

    if "listed_count" in df.columns:
        summary["listed_mean"] = g["listed_count"].mean()

    # boolean shares (mean over 0/1)
    if "verified" in df.columns:
        summary["verified_share"] = g["verified"].mean()
    if "protected" in df.columns:
        summary["protected_share"] = g["protected"].mean()

    # derived metric if inputs present
    if {"followers_sum", "members"}.issubset(summary.columns):
        summary["followers_per_member"] = summary["followers_sum"] / summary["members"]

    # order by what's available
    sort_col = "followers_sum" if "followers_sum" in summary.columns else "members"
    result = summary.reset_index().sort_values(sort_col, ascending=False)

    logger.info("Computed metric_party_summary with %d rows", len(result))
    return result


def metric_top_accounts_by_party(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    needed = {"partei_kurz", "followers_count"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()
    df = df.copy()
    df["rank_in_party"] = df.groupby("partei_kurz")["followers_count"].rank(ascending=False, method="first")
    cols = [c for c in ["partei_kurz", "rank_in_party", "username", "name", "followers_count", "verified"] if c in df.columns]
    result = (
        df.loc[df["rank_in_party"] <= top_n, cols]
        .sort_values(["partei_kurz", "rank_in_party"])
    )
    logger.info("Computed metric_top_accounts_by_party with %d rows (top_n=%d)", len(result), top_n)
    return result

def metric_top_accounts_global(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Top accounts overall by followers (across all parties)."""
    if "followers_count" not in df.columns:
        logger.warning("metric_top_accounts_global skipped: 'followers_count' column missing")
        return pd.DataFrame()
    df = df.copy()
    df["rank_global"] = df["followers_count"].rank(ascending=False, method="first")
    cols = [c for c in ["rank_global", "username", "name", "partei_kurz", "followers_count", "verified"] if c in df.columns]
    result = (
        df.sort_values("followers_count", ascending=False)
          .head(top_n)[cols]
          .reset_index(drop=True)
    )
    logger.info("Computed metric_top_accounts_global with %d rows (top_n=%d)", len(result), top_n)
    return result


# -------------------------------
# Orchestration
# -------------------------------

def build_metrics(top_n: int) -> List[MetricSpec]:
    return [
        MetricSpec(
            name="individual_base",
            description="Per-account basics and ratios (latest profile per username)",
            compute=metric_individual_base,
        ),
        MetricSpec(
            name="party_summary",
            description="Aggregated metrics by party",
            compute=metric_party_summary,
        ),
        MetricSpec(
            name="top_accounts_by_party",
            description=f"Top {top_n} accounts within each party by followers",
            compute=lambda df: metric_top_accounts_by_party(df, top_n=top_n),
        ),
        MetricSpec(
            name="top_accounts_global",
            description=f"Top {top_n} accounts overall by followers",
            compute=lambda df: metric_top_accounts_global(df, top_n=top_n),
        )
    ]


def run(year: int, month: int, outdir: str, schema: str, x_profiles: str, politicians: str, top_n: int):
    os.makedirs(outdir, exist_ok=True)
    ym = f"{year:04d}{month:02d}"

    latest = load_latest_profiles(schema=schema, x_profiles=x_profiles, politicians=politicians)

    required_cols = {
        "username", "partei_kurz", "created_at", "verified", "protected",
        "followers_count", "following_count", "tweet_count", "listed_count", "retrieved_at"
    }
    missing = required_cols - set(latest.columns)
    if missing:
        logger.warning("Missing columns in joined dataset: %s. Some metrics may be partial.", sorted(missing))

    for spec in build_metrics(top_n=top_n):
        df_metric = spec.compute(latest)
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
    top_n = int(getattr(Params, "top_n", 10))
    if not (1 <= month <= 12):
        raise SystemExit("Month must be in 1..12")

    # Hard-coded table identifiers per request
    schema = "public"
    x_profiles_tbl = "x_profiles"
    politicians_tbl = "politicians"
    run(year, month, outdir, schema, x_profiles_tbl, politicians_tbl, top_n)
