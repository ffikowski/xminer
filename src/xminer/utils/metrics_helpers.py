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

def metric_top_tweets_by(out: pd.DataFrame, metric: str, top_n: int = 10, ascending: bool = False) -> pd.DataFrame:
    """Generic helper: top or bottom tweets by a given metric."""
    if metric not in out.columns:
        logger.warning("Column '%s' not found; skipping metric_top_tweets_by.", metric)
        return pd.DataFrame()
    cols = [
        "tweet_id", "username", "partei_kurz", "created_at", "text",
        "like_count", "reply_count", "retweet_count", "quote_count",
        "impression_count", "engagement_total", "engagement_rate"
    ]
    cols = [c for c in cols if c in out.columns]
    if metric in out.columns and metric not in cols:
        cols.append(metric)
    df = out[cols].copy()

    df = df.sort_values(metric, ascending=ascending).head(top_n).reset_index(drop=True)
    logger.info("Computed top tweets by %s (%s)", metric, "ascending" if ascending else "descending")
    return df

def metric_top_tweets_by_flex(
    out: pd.DataFrame,
    metric: str,
    top_n: int = 10,
    ascending: bool = False,
    min_impressions: int | None = None,
    dropna: bool = True,
) -> pd.DataFrame:
    if metric not in out.columns:
        logger.warning("Column '%s' not found; skipping.", metric)
        return pd.DataFrame()
    df = out.copy()
    if min_impressions is not None and "impression_count" in df.columns:
        df = df[df["impression_count"] >= min_impressions]
    if dropna:
        df = df[np.isfinite(df[metric])]
    cols = [
        "tweet_id","username","partei_kurz","created_at","text","lang",
        "like_count","reply_count","retweet_count","quote_count","bookmark_count",
        "impression_count","engagement_total","engagement_rate", metric
    ]
    cols = [c for c in cols if c in df.columns]
    df = df.sort_values([metric, "engagement_total"], ascending=[ascending, False]).head(top_n)
    logger.info("Leaderboard by %s (asc=%s, min_impr=%s) -> %d rows",
                metric, ascending, min_impressions, len(df))
    return df[cols].reset_index(drop=True)


def metric_bottom_tweets_by_engagement_rate(out: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    return metric_top_tweets_by(out, "engagement_rate", top_n, ascending=True)

def metric_top_tweets_by_likes(out: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    return metric_top_tweets_by(out, "like_count", top_n)

def metric_top_tweets_by_reply_ratio(out: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if "like_to_reply" not in out.columns:
        logger.warning("Missing like_to_reply ratio.")
        return pd.DataFrame()
    return metric_top_tweets_by(out, "like_to_reply", top_n, ascending=True)

def metric_top_tweets_by_retweets(out, top_n=10):
    return metric_top_tweets_by(out, "retweet_count", top_n)

def metric_top_tweets_by_replies(out, top_n=10):
    return metric_top_tweets_by(out, "reply_count", top_n)

def metric_top_tweets_by_quotes(out, top_n=10):
    return metric_top_tweets_by(out, "quote_count", top_n)

def metric_top_tweets_by_bookmarks(out, top_n=10):
    return metric_top_tweets_by(out, "bookmark_count", top_n)

def metric_top_tweets_by_impressions(out, top_n=10):
    return metric_top_tweets_by(out, "impression_count", top_n)

def metric_top_tweets_by_likes_per_1k(out, top_n=10):
    return metric_top_tweets_by_flex(out, "likes_per_1k_followers", top_n)

def metric_top_tweets_by_engagement_per_1k(out, top_n=10):
    return metric_top_tweets_by_flex(out, "engagement_per_1k_followers", top_n)

def metric_bottom_tweets_by_engagement_per_1k(out, top_n=10, min_impressions=1000):
    return metric_top_tweets_by_flex(out, "engagement_per_1k_followers", top_n, ascending=True, min_impressions=min_impressions)

def metric_most_controversial(out, top_n=10, min_impressions=1000):
    # (replies + quotes) / max(likes, 1)
    df = out.copy()
    likes = df["like_count"].replace(0, np.nan) if "like_count" in df else np.nan
    num = df.get("reply_count", np.nan) + df.get("quote_count", np.nan)
    df["controversy_score"] = _safe_div(num, likes)
    return metric_top_tweets_by_flex(df, "controversy_score", top_n, min_impressions=min_impressions)

def metric_most_reply_heavy(out, top_n=10, min_impressions=1000):
    # replies / engagement_total
    df = out.copy()
    df["reply_share"] = _safe_div(df.get("reply_count", np.nan), df["engagement_total"].replace(0, np.nan))
    return metric_top_tweets_by_flex(df, "reply_share", top_n, min_impressions=min_impressions)

def metric_most_quote_heavy(out, top_n=10, min_impressions=1000):
    df = out.copy()
    df["quote_share"] = _safe_div(df.get("quote_count", np.nan), df["engagement_total"].replace(0, np.nan))
    return metric_top_tweets_by_flex(df, "quote_share", top_n, min_impressions=min_impressions)

def metric_most_amplified_debate(out, top_n=10, min_impressions=1000):
    # (retweets + quotes) / impressions
    df = out.copy()
    df["amplification_rate"] = _rate(df, numer=None) if False else _safe_div(df.get("retweet_count", np.nan) + df.get("quote_count", np.nan), df.get("impression_count", np.nan))
    return metric_top_tweets_by_flex(df, "amplification_rate", top_n, min_impressions=min_impressions)

def metric_most_controversial_by_like_to_reply(out, top_n=10, min_impressions=1000):
    # Smallest like_to_reply = most controversial
    return metric_top_tweets_by_flex(out, "like_to_reply", top_n, ascending=True, min_impressions=min_impressions)

def metric_low_conversion_high_reach(out, top_n=10, min_impressions=10000):
    # lowest engagement rate among tweets with large reach
    return metric_top_tweets_by_flex(out, "engagement_rate", top_n, ascending=True, min_impressions=min_impressions)

def metric_silent_hits(out, top_n=10, max_impressions=5000):
    # very good conversion with small reach
    df = out.copy()
    if "impression_count" in df:
        df = df[df["impression_count"] <= max_impressions]
    return metric_top_tweets_by_flex(df, "engagement_rate", top_n)

def metric_top_authors_by_avg_engagement_rate(out: pd.DataFrame, top_n: int = 10, min_tweets: int = 5) -> pd.DataFrame:
    if "username" not in out or "engagement_rate" not in out:
        return pd.DataFrame()
    g = out.groupby(["partei_kurz", "username"], dropna=False)
    agg = g.agg(n_tweets=("tweet_id", "count"), avg_engagement_rate=("engagement_rate", "mean"),
    impressions_sum=("impression_count", "sum"), engagement_sum=("engagement_total", "sum"))
    agg = agg[agg["n_tweets"] >= min_tweets]
    agg = agg.sort_values(["avg_engagement_rate", "engagement_sum"], ascending=[False, False]).head(top_n)
    return agg.reset_index()


def metric_most_active_authors(out: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if "username" not in out:
        return pd.DataFrame()
    g = out.groupby(["partei_kurz", "username"], dropna=False).size().rename("n_tweets").reset_index()
    return g.sort_values(["n_tweets"], ascending=False).head(top_n)

