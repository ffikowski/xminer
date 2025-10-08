import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import timezone
import logging

logger = logging.getLogger(__name__)

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
    if "partei_kurz" not in df.columns:
        logger.warning("metric_party_summary skipped: 'party' column missing")
        return pd.DataFrame()

    g = df.groupby("partei_kurz", dropna=False)

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

def metric_individual_deltas(delta_df: pd.DataFrame) -> pd.DataFrame:
    """Per-account month-over-month changes."""
    cols = [
        # identity
        "username", "name_curr", "partei_kurz",
        # raw counts (prev/curr)
        "followers_count_prev", "followers_count_curr",
        "following_count_prev", "following_count_curr",
        "tweet_count_prev", "tweet_count_curr",
        "listed_count_prev", "listed_count_curr",
        # deltas
        "delta_followers_count", "delta_following_count",
        "delta_tweet_count", "delta_listed_count",
        # pct deltas
        "pct_followers_count", "pct_following_count",
        "pct_tweet_count", "pct_listed_count",
        # bookkeeping
        "retrieved_at_prev", "retrieved_at_curr", "snapshot_span_days",
    ]
    existing = [c for c in cols if c in delta_df.columns]
    result = delta_df[existing].sort_values("delta_followers_count", ascending=False, na_position="last")
    logger.info("Computed metric_individual_deltas with %d rows", len(result))
    return result


def metric_party_delta_summary(delta_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregated month-over-month changes by party."""
    if "partei_kurz" not in delta_df:
        logger.warning("metric_party_delta_summary skipped: 'partei_kurz' missing")
        return pd.DataFrame()

    g = delta_df.groupby("partei_kurz", dropna=False)

    out = g.size().rename("members_in_both").to_frame()

    for col in ["followers_count", "following_count", "tweet_count", "listed_count"]:
        dcol = f"delta_{col}"
        if dcol in delta_df:
            out[f"{dcol}_sum"] = g[dcol].sum()
            out[f"{dcol}_mean"] = g[dcol].mean()
            out[f"{dcol}_median"] = g[dcol].median()

    # Order by total follower delta if available, else by members
    sort_col = "delta_followers_count_sum" if "delta_followers_count_sum" in out.columns else "members_in_both"
    result = out.reset_index().sort_values(sort_col, ascending=False)
    logger.info("Computed metric_party_delta_summary with %d rows", len(result))
    return result


def metric_top_gainers_by_party(delta_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Top accounts per party by follower gains."""
    needed = {"partei_kurz", "delta_followers_count"}
    if not needed.issubset(delta_df.columns):
        return pd.DataFrame()
    df = delta_df.copy()
    df["rank_in_party_gain"] = df.groupby("partei_kurz")["delta_followers_count"].rank(ascending=False, method="first")
    cols = [
        "partei_kurz", "rank_in_party_gain",
        "username", "name_curr",
        "followers_count_prev", "followers_count_curr", "delta_followers_count",
        "retrieved_at_prev", "retrieved_at_curr"
    ]
    cols = [c for c in cols if c in df.columns]
    result = df.loc[df["rank_in_party_gain"] <= top_n, cols].sort_values(["partei_kurz", "rank_in_party_gain"])
    logger.info("Computed metric_top_gainers_by_party with %d rows (top_n=%d)", len(result), top_n)
    return result


def metric_top_gainers_global(delta_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Top overall follower gainers."""
    if "delta_followers_count" not in delta_df.columns:
        logger.warning("metric_top_gainers_global skipped: delta column missing")
        return pd.DataFrame()
    df = delta_df.sort_values("delta_followers_count", ascending=False).head(top_n).copy()
    df["rank_gain_global"] = range(1, len(df) + 1)
    cols = [
        "rank_gain_global", "username", "name_curr", "partei_kurz",
        "followers_count_prev", "followers_count_curr", "delta_followers_count",
        "retrieved_at_prev", "retrieved_at_curr"
    ]
    cols = [c for c in cols if c in df.columns]
    result = df[cols].reset_index(drop=True)
    logger.info("Computed metric_top_gainers_global with %d rows (top_n=%d)", len(result), top_n)
    return result