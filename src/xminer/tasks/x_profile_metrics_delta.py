from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import text

# --- Project-style imports (match your existing script) ---
from ..io.db import engine                   # central engine built from Config.DATABASE_URL
from ..config.params import Params           # parameters class already used in production
from ..utils.global_helpers import politicians_table_name, normalize_party, UNION_MAP, month_bounds, prev_year_month, _safe_div, build_outdir
from ..utils.metrics_helpers import MetricSpec, metric_individual_deltas, metric_party_delta_summary, metric_top_gainers_by_party, metric_top_gainers_global


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
    outdir_profiles = build_outdir(outdir, year, month, "profiles")
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
        out_path = os.path.join(outdir_profiles, f"{spec.name}_{ym}.csv")
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
