"""
Export data from Neon (Postgres) to local files.

Usage examples:
  python -m xminer.export_neon --table public.x_profiles --out exports/x_profiles.csv
  python -m xminer.export_neon --query "SELECT * FROM public.x_profiles WHERE verified" --out exports/verified.csv.gz
  python -m xminer.export_neon --table public.x_profiles --out exports/x_profiles.parquet
"""

import argparse
import os
import sys
import gzip
import csv
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env file if present (so DATABASE_URL is available)
load_dotenv()

# Try to reuse xminer.db.engine if available
try:
    from .db import engine as default_engine
except Exception:
    default_engine = None


def get_engine(dsn: Optional[str]):
    """Return a SQLAlchemy engine, preferring explicit DSN > DATABASE_URL > default engine."""
    if dsn:
        return create_engine(dsn)
    env_dsn = os.getenv("DATABASE_URL")
    if env_dsn:
        return create_engine(env_dsn)
    if default_engine is not None:
        return default_engine
    sys.exit(
        "❌ No database connection found.\n"
        "Provide --dsn postgresql://USER:PASS@HOST/DB?sslmode=require\n"
        "or set DATABASE_URL in .env, or configure xminer.db.engine."
    )


def build_sql(args) -> str:
    if args.query:
        return args.query
    if not args.table:
        sys.exit("Provide --table or --query.")
    sql = f"SELECT * FROM {args.table}"
    if args.where:
        sql += f" WHERE {args.where}"
    if args.limit is not None:
        sql += f" LIMIT {int(args.limit)}"
    return sql


def write_csv_iter(conn, sql: str, out_path: str, chunksize: int, header: bool, gzip_enabled: bool):
    opener = gzip.open if gzip_enabled else open
    mode = "wt" if gzip_enabled else "w"
    first = True
    with opener(out_path, mode, encoding="utf-8", newline="") as f:
        for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
            chunk.to_csv(
                f,
                index=False,
                header=(header and first),
                quoting=csv.QUOTE_ALL  # 👈 ensures tweets with \n are wrapped in quotes
            )
            first = False


def write_parquet(conn, sql: str, out_path: str, chunksize: int):
    frames = []
    for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
        frames.append(chunk)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    df.to_parquet(out_path, index=False)


def main():
    ap = argparse.ArgumentParser(description="Export from Neon/Postgres to CSV or Parquet.")
    ap.add_argument("--dsn", help="Optional: Postgres DSN (overrides DATABASE_URL and default engine).")
    ap.add_argument("--table", help="Table to export, e.g. public.x_profiles")
    ap.add_argument("--query", help="Custom SELECT; if set, overrides --table/--where/--limit")
    ap.add_argument("--where", help="Optional WHERE clause, e.g. verified = true")
    ap.add_argument("--limit", type=int, help="Optional LIMIT")
    ap.add_argument("--out", required=True, help="Output file (.csv, .csv.gz, or .parquet)")
    ap.add_argument("--chunksize", type=int, default=100_000, help="Rows per chunk (for streaming)")
    ap.add_argument("--no-header", action="store_true", help="CSV only: omit header row")
    args = ap.parse_args()

    sql = build_sql(args)
    out_dir = os.path.dirname(args.out) or "."
    os.makedirs(out_dir, exist_ok=True)

    engine = get_engine(args.dsn)

    lower = args.out.lower()
    is_parquet = lower.endswith(".parquet")
    is_gzip = lower.endswith(".gz")
    header = not args.no_header

    with engine.connect() as conn:
        if is_parquet:
            write_parquet(conn, sql, args.out, args.chunksize)
        else:
            write_csv_iter(conn, sql, args.out, args.chunksize, header, gzip_enabled=is_gzip)

    print(f"✅ Exported to {args.out}")


if __name__ == "__main__":
    main()
