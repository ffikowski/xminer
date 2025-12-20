from __future__ import annotations

import argparse
import csv
import os
from typing import Iterable, Tuple

import pandas as pd
from sqlalchemy import text

from ..config.params import Params
from ..io.db import engine
from ..utils.global_helpers import build_outdir, month_bounds

DEFAULT_SCHEMA = "public"
TRENDS_TABLE = "x_trends"
TWEETS_TABLE = "tweets"
TREND_COLUMNS = [
    "woeid",
    "place_name",
    "trend_name",
    "tweet_count",
    "rank",
    "retrieved_at",
    "source_version",
]
TWEET_COLUMNS = [
    "tweet_id",
    "author_id",
    "username",
    "created_at",
    "text",
    "lang",
    "conversation_id",
    "in_reply_to_user_id",
    "possibly_sensitive",
    "like_count",
    "reply_count",
    "retweet_count",
    "quote_count",
    "bookmark_count",
    "impression_count",
    "source",
    "entities",
    "referenced_tweets",
    "retrieved_at",
]


def month_window(year: int, month: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Return lower bound and exclusive upper bound for the configured month."""
    return month_bounds(year, month)


def default_trends_out_path(base_outdir: str, year: int, month: int) -> str:
    ym = f"{year:04d}{month:02d}"
    trends_dir = build_outdir(base_outdir, year, month, "trends")
    return os.path.join(trends_dir, f"{TRENDS_TABLE}_{ym}.csv")


def default_tweets_out_path(base_outdir: str, year: int, month: int) -> str:
    ym = f"{year:04d}{month:02d}"
    tweets_dir = build_outdir(base_outdir, year, month, "tweets")
    return os.path.join(tweets_dir, f"{TWEETS_TABLE}_{ym}.csv")


def _stream_to_csv(sql, params: dict, out_path: str, columns: Iterable[str], chunksize: int) -> int:
    total = 0
    first = True
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with engine.connect() as conn, open(out_path, "w", encoding="utf-8", newline="") as fh:
        for chunk in pd.read_sql(sql, conn, params=params, chunksize=chunksize):
            total += len(chunk)
            chunk.to_csv(fh, index=False, header=first, quoting=csv.QUOTE_ALL)
            first = False

        # If no rows matched, still write a header for downstream tools.
        if total == 0 and first:
            writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
    return total


def export_trends_month(
    schema: str,
    table: str,
    start,
    end,
    out_path: str,
    chunksize: int = 50_000,
) -> int:
    sql = text(
        f"""
        SELECT
            woeid,
            place_name,
            trend_name,
            tweet_count,
            rank,
            retrieved_at,
            source_version
        FROM {schema}.{table}
        WHERE retrieved_at >= :start AND retrieved_at < :end
        ORDER BY retrieved_at, rank
        """
    )
    return _stream_to_csv(sql, {"start": start, "end": end}, out_path, TREND_COLUMNS, chunksize)


def export_tweets_month(
    schema: str,
    table: str,
    start,
    end,
    out_path: str,
    chunksize: int = 50_000,
) -> int:
    sql = text(
        f"""
        SELECT
            tweet_id,
            author_id,
            username,
            created_at,
            text,
            lang,
            conversation_id,
            in_reply_to_user_id,
            possibly_sensitive,
            like_count,
            reply_count,
            retweet_count,
            quote_count,
            bookmark_count,
            impression_count,
            source,
            entities,
            referenced_tweets,
            retrieved_at
        FROM {schema}.{table}
        WHERE created_at >= :start AND created_at < :end
        ORDER BY created_at
        """
    )
    return _stream_to_csv(sql, {"start": start, "end": end}, out_path, TWEET_COLUMNS, chunksize)


def main(argv=None) -> int:
    P = Params()

    parser = argparse.ArgumentParser(description="Export monthly snapshots from Neon/Postgres.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema for both tables.")
    parser.add_argument("--trends-table", default=TRENDS_TABLE, help="Table name to export for trends.")
    parser.add_argument("--tweets-table", default=TWEETS_TABLE, help="Table name to export for tweets.")
    parser.add_argument("--chunksize", type=int, default=50_000, help="Rows per chunk when streaming from Postgres.")
    parser.add_argument("--outdir", help="Override base output directory (default: outputs/...).")
    parser.add_argument("--year", type=int, help="Optional override for year (defaults to parameters.yml).")
    parser.add_argument("--month", type=int, help="Optional override for month (defaults to parameters.yml).")
    parser.add_argument("--skip-trends", action="store_true", help="Only export tweets.")
    parser.add_argument("--skip-tweets", action="store_true", help="Only export trends.")
    args = parser.parse_args(argv)

    year = args.year or getattr(P, "year", 2025)
    month = args.month or getattr(P, "month", 1)
    # Default to "outputs" per requested layout; allow explicit override.
    base_outdir = args.outdir or getattr(P, "outdir", None) or "outputs"
    if str(base_outdir).rstrip("/\\") == "output":
        base_outdir = "outputs"

    start, end = month_window(year, month)
    totals = []
    if not args.skip_trends:
        out_trends = default_trends_out_path(base_outdir, year, month)
        total_trends = export_trends_month(
            args.schema, args.trends_table, start, end, out_trends, args.chunksize
        )
        totals.append(f"trends={total_trends} -> {out_trends}")

    if not args.skip_tweets:
        out_tweets = default_tweets_out_path(base_outdir, year, month)
        total_tweets = export_tweets_month(
            args.schema, args.tweets_table, start, end, out_tweets, args.chunksize
        )
        totals.append(f"tweets={total_tweets} -> {out_tweets}")

    print("Export complete:", "; ".join(totals) if totals else "nothing to do")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
