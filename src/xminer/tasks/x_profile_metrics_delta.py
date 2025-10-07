from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# --- Project-style imports (match your existing script) ---
from ..io.db import engine                   # central engine built from Config.DATABASE_URL
from ..config.params import Params           # parameters class already used in production
from ..utils.tweets_helpers import politicians_table_name

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/x_profile_metrics_delta.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# -------------------------------
# Helpers
# -------------------------------

UNION_MAP = {"CDU": "CDU/CSU", "CSU": "CDU/CSU"}

def normalize_party(df: pd.DataFrame) -> pd.DataFrame:
    if "partei_kurz" in df.columns:
        df["partei_kurz"] = (
            df["partei_kurz"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace(UNION_MAP)
        )
    return df


def month_bounds(year: int, month: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Return (month_start_utc, next_month_start_utc)."""
    start = pd.Timestamp(year=year, month=month, day=1, tz=timezone.utc)
    # next month: if December, roll to Jan of next year
    if month == 12:
        nxt = pd.Timestamp(year=year + 1, month=1, day=2, tz=timezone.utc)
    else:
        nxt = pd.Timestamp(year=year, month=month + 1, day=2, tz=timezone.utc)
    return start, nxt


def prev_year_month(year: int, month: int) -> Tuple[int, int]:
    """Return (prev_year, prev_month)."""
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _safe_div(a, b):
    with np.errstate(divide="ignore", invalid="ignore"):
        res = np.divide(a, b)
    return np.where(~np.isfinite(res), np.nan, res)


# -------------------------------
# Data access
# -------------------------------
POSTGRES_SNAPSHOT_SQL_TMPL = r"""
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
  WHERE xp.retrieved_at < TIMESTAMPTZ '{ub_iso}'
)
SELECT *
FROM joined
WHERE rn = 1
"""

def load_month_snapshot(schema: str, x_profiles: str, politicians: str, year: int, month: int) -> pd.DataFrame:
    """
    Return the latest profile per username taken at/before the start of the next month.
    This effectively gives you a month-end snapshot (or the latest available before that).
    """
    politicians = politicians_table_name(month, year)
    _, ub = month_bounds(year, month)  # use next month start as upper bound
    ub_iso = ub.strftime("%Y-%m-%d %H:%M:%S%z")  # e.g., '2025-10-01 00:00:00+0000'
    sql = POSTGRES_SNAPSHOT_SQL_TMPL.format(
        schema=schema, x_profiles=x_profiles, politicians=politicians, ub_iso=ub_iso
    )
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)

    # Ensure expected dtypes
    if "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    if "retrieved_at" in df:
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], utc=True, errors="coerce")
    if "geburtsdatum" in df:
        df["geburtsdatum"] = pd.to_datetime(df["geburtsdatum"], utc=True, errors="coerce").dt.date
    if "username" in df:
        df["username"] = df["username"].astype(str).str.strip()

    df = normalize_party(df)

    logger.info("Loaded snapshot for %04d-%02d: %d rows", year, month, len(df))
    return df


def join_prev_curr(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Inner-join prev and curr snapshots on username and add delta columns.
    Only accounts present in both months are included (prevents spurious spikes).
    """
    on = ["username"]
    merged = curr_df.merge(prev_df, on=on, how="outer", suffixes=("_curr", "_prev"))

    # Numeric deltas (only compute if both columns exist)
    for col in ["followers_count", "following_count", "tweet_count", "listed_count"]:
        c_cur, c_prev = f"{col}_curr", f"{col}_prev"
        if c_cur in merged and c_prev in merged:
            merged[f"delta_{col}"] = merged[c_cur].astype("float64") - merged[c_prev].astype("float64")
            # % change relative to prev
            merged[f"pct_{col}"] = _safe_div(merged[f"delta_{col}"], merged[c_prev].replace(0, np.nan))

    # Keep a simple 'party' column (prefer current month attribution if present)
    if "partei_kurz_curr" in merged:
        merged["partei_kurz"] = merged["partei_kurz_curr"]
    elif "partei_kurz_prev" in merged:
        merged["partei_kurz"] = merged["partei_kurz_prev"]

    # Convenience: last retrieval timestamps
    if "retrieved_at_curr" in merged and "retrieved_at_prev" in merged:
        merged["snapshot_span_days"] = (merged["retrieved_at_curr"] - merged["retrieved_at_prev"]).dt.days

    return merged


# -------------------------------
# Metric computation (on the joined delta frame)
# -------------------------------
@dataclass
class MetricSpec:
    name: str
    description: str
    compute: callable  # function(delta_df) -> DataFrame


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


# -------------------------------
# Orchestration
# -------------------------------
def build_delta_metrics(top_n: int) -> List[MetricSpec]:
    return [
        MetricSpec(
            name="individual_deltas",
            description="Per-account MoM deltas (followers, following, tweets, listed)",
            compute=metric_individual_deltas,
        ),
        MetricSpec(
            name="party_delta_summary",
            description="Aggregated MoM deltas by party",
            compute=metric_party_delta_summary,
        ),
        MetricSpec(
            name="top_gainers_by_party",
            description=f"Top {top_n} follower gainers within each party",
            compute=lambda df: metric_top_gainers_by_party(df, top_n=top_n),
        ),
        MetricSpec(
            name="top_gainers_global",
            description=f"Top {top_n} follower gainers overall",
            compute=lambda df: metric_top_gainers_global(df, top_n=top_n),
        ),
    ]


def run(year: int, month: int, outdir: str, schema: str, x_profiles: str, politicians: str, top_n: int):
    """
    Compute month-over-month metrics for the target year-month vs its previous month.
    Writes one CSV per metric into outdir with the suffix YYYYMM (the *current* month).
    """
    os.makedirs(outdir, exist_ok=True)
    ym = f"{year:04d}{month:02d}"
    prev_y, prev_m = prev_year_month(year, month)

    prev_snap = load_month_snapshot(schema=schema, x_profiles=x_profiles, politicians=politicians,
                                    year=prev_y, month=prev_m)
    curr_snap = load_month_snapshot(schema=schema, x_profiles=x_profiles, politicians=politicians,
                                    year=year, month=month)

    # Guard rails
    if prev_snap.empty or curr_snap.empty:
        logger.warning("One of the snapshots is empty (prev=%d rows, curr=%d rows). Outputs may be empty.",
                       len(prev_snap), len(curr_snap))

    delta_df = join_prev_curr(prev_snap, curr_snap)

    required_cols = {
        "username", "partei_kurz",
        "followers_count_prev", "followers_count_curr",
        "following_count_prev", "following_count_curr",
        "tweet_count_prev", "tweet_count_curr",
        "listed_count_prev", "listed_count_curr",
        "retrieved_at_prev", "retrieved_at_curr",
    }
    missing = required_cols - set(delta_df.columns)
    if missing:
        logger.warning("Missing expected columns after join: %s. Some metrics may be partial.", sorted(missing))

    for spec in build_delta_metrics(top_n=top_n):
        out = spec.compute(delta_df)
        out_path = os.path.join(outdir, f"{spec.name}_{ym}.csv")
        out.to_csv(out_path, index=False)
        logger.info("Wrote %s -> %s", spec.description, out_path)


# -------------------------------
# Entrypoint (parameters.yml only)
# -------------------------------
if __name__ == "__main__":
    # pull parameters the same way as your monthly script
    year = int(getattr(Params, "year", datetime.now().year))
    month = int(getattr(Params, "month", datetime.now().month))
    outdir = getattr(Params, "outdir", "output")
    top_n = int(getattr(Params, "top_n", 10))
    if not (1 <= month <= 12):
        raise SystemExit("Month must be in 1..12")

    # Hard-coded table identifiers per request (same as your other script)
    schema = "public"
    x_profiles_tbl = "x_profiles"
    politicians_tbl = "politicians"
    run(year, month, outdir, schema, x_profiles_tbl, politicians_tbl, top_n)
