# src/xminer/ingest_helpers.py
from __future__ import annotations
import json, math, os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple
from sqlalchemy import text, bindparam, BigInteger, Text, JSON
from psycopg2.extras import Json

# ---- tiny coercers ----
def to_int_or_none(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, float):
        try:
            if math.isnan(v) or math.isinf(v):
                return None
        except Exception:
            return None
    try:
        return int(v)
    except Exception:
        return None

def to_json_obj(v: Any):
    if v in (None, "", "null"):
        return None
    if isinstance(v, (dict, list)):
        return Json(v)  # Wrap with psycopg2.extras.Json for JSONB compatibility
    if isinstance(v, str):
        try:
            return Json(json.loads(v))
        except Exception:
            return None
    return None

def to_aware_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        # ensure tz-aware
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    # last resort: now()
    return datetime.now(timezone.utc)

# ---- row sanitizer ----
def sanitize_rows(rows: Iterable[Dict]) -> List[Dict]:
    out: List[Dict] = []
    for r in rows:
        out.append({
            # TEXT ids (you converted these columns to TEXT)
            "tweet_id":        str(r.get("tweet_id")) if r.get("tweet_id") is not None else None,
            "conversation_id": str(r.get("conversation_id")) if r.get("conversation_id") is not None else None,

            # BIGINT ids
            "author_id":           to_int_or_none(r.get("author_id")),
            "in_reply_to_user_id": to_int_or_none(r.get("in_reply_to_user_id")),

            # other
            "username":            r.get("username"),
            "created_at":          to_aware_dt(r.get("created_at")),
            "text":                r.get("text"),
            "lang":                r.get("lang"),
            "possibly_sensitive":  r.get("possibly_sensitive"),

            # BIGINT counters
            "like_count":        to_int_or_none(r.get("like_count")),
            "reply_count":       to_int_or_none(r.get("reply_count")),
            "retweet_count":     to_int_or_none(r.get("retweet_count")),
            "quote_count":       to_int_or_none(r.get("quote_count")),
            "bookmark_count":    to_int_or_none(r.get("bookmark_count")),
            "impression_count":  to_int_or_none(r.get("impression_count")),

            "source":             r.get("source"),
            "entities":           to_json_obj(r.get("entities")),
            "referenced_tweets":  to_json_obj(r.get("referenced_tweets")),
            "retrieved_at":       to_aware_dt(r.get("retrieved_at")),
        })
    return out

def politicians_table_name(month: int, year: int) -> str:
    mm = f"{int(month):02d}"
    yyyy = f"{int(year):04d}"
    return f"politicians_{mm}_{yyyy}"

# ---- single prepared statement (types bound) ----
INSERT_TWEETS_STMT = text("""
    INSERT INTO tweets (
        tweet_id, author_id, username, created_at, text, lang,
        conversation_id, in_reply_to_user_id, possibly_sensitive,
        like_count, reply_count, retweet_count, quote_count,
        bookmark_count, impression_count,
        source, entities, referenced_tweets, retrieved_at
    ) VALUES (
        :tweet_id, :author_id, :username, :created_at, :text, :lang,
        :conversation_id, :in_reply_to_user_id, :possibly_sensitive,
        :like_count, :reply_count, :retweet_count, :quote_count,
        :bookmark_count, :impression_count,
        :source, :entities, :referenced_tweets, :retrieved_at
    )
    ON CONFLICT (tweet_id) DO UPDATE SET
        author_id            = EXCLUDED.author_id,
        username             = EXCLUDED.username,
        created_at           = EXCLUDED.created_at,
        text                 = EXCLUDED.text,
        lang                 = EXCLUDED.lang,
        conversation_id      = EXCLUDED.conversation_id,
        in_reply_to_user_id  = EXCLUDED.in_reply_to_user_id,
        possibly_sensitive   = EXCLUDED.possibly_sensitive,
        like_count           = EXCLUDED.like_count,
        reply_count          = EXCLUDED.reply_count,
        retweet_count        = EXCLUDED.retweet_count,
        quote_count          = EXCLUDED.quote_count,
        bookmark_count       = EXCLUDED.bookmark_count,
        impression_count     = EXCLUDED.impression_count,
        source               = EXCLUDED.source,
        entities             = EXCLUDED.entities,
        referenced_tweets    = EXCLUDED.referenced_tweets,
        retrieved_at         = EXCLUDED.retrieved_at
""").bindparams(
    bindparam("tweet_id",            type_=Text()),
    bindparam("author_id",           type_=BigInteger()),
    bindparam("username",            type_=Text()),
    bindparam("created_at"),
    bindparam("text",                type_=Text()),
    bindparam("lang",                type_=Text()),
    bindparam("conversation_id",     type_=Text()),
    bindparam("in_reply_to_user_id", type_=BigInteger()),
    bindparam("possibly_sensitive"),
    bindparam("like_count",          type_=BigInteger()),
    bindparam("reply_count",         type_=BigInteger()),
    bindparam("retweet_count",       type_=BigInteger()),
    bindparam("quote_count",         type_=BigInteger()),
    bindparam("bookmark_count",      type_=BigInteger()),
    bindparam("impression_count",    type_=BigInteger()),
    bindparam("source",              type_=Text()),
    bindparam("entities",            type_=JSON()),
    bindparam("referenced_tweets",   type_=JSON()),
    bindparam("retrieved_at"),
)

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

def build_outdir(base_outdir: str, year: int, month: int, channel: str) -> str:
    ym = f"{year:04d}{month:02d}"
    path = os.path.join(base_outdir, ym, channel)
    os.makedirs(path, exist_ok=True)
    return path