import os
import json
from datetime import datetime, timezone
from sqlalchemy import create_engine, text, BigInteger, Integer, Text, JSON, bindparam

from .config.config import Config
from .config.params import Params
from .db import engine


# 2) the exact row you logged
row = {
    "tweet_id": "1961685456451461169",             # TEXT in your DB
    "author_id": 809895794,                         # BIGINT
    "username": "GtzFrmming",
    "created_at": datetime(2025, 8, 30, 7, 0, 33, tzinfo=timezone.utc),
    "text": "RT @shellenberger: America spends trillions on NATO to protect democracies including Germany. But it is has just prevented an opposition ca…",
    "lang": "en",
    "conversation_id": "1961685456451461169",       # TEXT in your DB
    "in_reply_to_user_id": None,                    # BIGINT (NULL ok)
    "possibly_sensitive": False,
    "like_count": 0,                                # BIGINT
    "reply_count": 0,                               # BIGINT
    "retweet_count": 1616,                          # BIGINT
    "quote_count": 0,                               # BIGINT
    "bookmark_count": 0,                            # BIGINT
    "impression_count": 0,                          # BIGINT
    "source": None,                                 # TEXT
    "entities": json.dumps({
        "mentions": [{"start":3,"end":17,"username":"shellenberger","id":"2474749586"}],
        "annotations": [
            {"start":19,"end":25,"probability":0.9276,"type":"Place","normalized_text":"America"},
            {"start":47,"end":50,"probability":0.9738,"type":"Organization","normalized_text":"NATO"},
            {"start":85,"end":91,"probability":0.9565,"type":"Place","normalized_text":"Germany"},
        ],
    }),                                             # JSONB column; driver can cast text->jsonb
    "referenced_tweets": json.dumps([{"id": 1961468514507919813, "type": "retweeted"}]),
    "retrieved_at": datetime(2025, 8, 30, 7, 49, 38, tzinfo=timezone.utc),
}

# 3) same SQL as in fetch_tweets.py
SQL = text("""
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
""")

def dump_types(d):
    keys = ["tweet_id","author_id","conversation_id","in_reply_to_user_id",
            "like_count","reply_count","retweet_count","quote_count",
            "bookmark_count","impression_count"]
    print("BOUND TYPES:")
    for k in keys:
        if k in d:
            print(f"  {k:<20} -> {type(d[k]).__name__}   value={d[k]!r}")
    print("")

def try_insert(record):
    print("== Direct insert (no explicit binding) ==")
    dump_types(record)
    try:
        with engine.begin() as conn:
            conn.execute(SQL, [record])
        print("Direct insert: SUCCESS")
    except Exception as e:
        print("Direct insert: FAILED:", repr(e))
        # isolate which column breaks
        print("\n== Binary search by adding columns stepwise ==")
        core_cols = ["tweet_id","author_id","username","created_at","text","lang",
                     "conversation_id","possibly_sensitive","retrieved_at"]
        core = {k: record.get(k) for k in core_cols}
        try:
            with engine.begin() as conn:
                conn.execute(SQL, [core])
            print("Core insert OK.")
        except Exception as ee:
            print("Core insert FAILED:", repr(ee))
            return
        extras = ["in_reply_to_user_id","like_count","reply_count","retweet_count",
                  "quote_count","bookmark_count","impression_count",
                  "source","entities","referenced_tweets"]
        for col in extras:
            core[col] = record.get(col)
            try:
                with engine.begin() as conn:
                    conn.execute(SQL, [core])
                print(f" + {col} OK")
            except Exception as e3:
                print(f" ** Adding {col} caused FAILURE ** -> {repr(e3)}")
                break

def try_insert_with_bound_types(record):
    print("\n== Insert with explicit bind types ==")
    stmt = text(SQL.text).bindparams(
        bindparam("tweet_id",           type_=Text()),
        bindparam("author_id",          type_=BigInteger()),
        bindparam("username",           type_=Text()),
        bindparam("created_at"),
        bindparam("text",               type_=Text()),
        bindparam("lang",               type_=Text()),
        bindparam("conversation_id",    type_=Text()),
        bindparam("in_reply_to_user_id",type_=BigInteger()),
        bindparam("possibly_sensitive"),
        bindparam("like_count",         type_=BigInteger()),
        bindparam("reply_count",        type_=BigInteger()),
        bindparam("retweet_count",      type_=BigInteger()),
        bindparam("quote_count",        type_=BigInteger()),
        bindparam("bookmark_count",     type_=BigInteger()),
        bindparam("impression_count",   type_=BigInteger()),
        bindparam("source",             type_=Text()),
        bindparam("entities",           type_=JSON()),
        bindparam("referenced_tweets",  type_=JSON()),
        bindparam("retrieved_at"),
    )
    dump_types(record)
    try:
        with engine.begin() as conn:
            conn.execute(stmt, [record])
        print("Bound insert: SUCCESS")
    except Exception as e:
        print("Bound insert: FAILED:", repr(e))

if __name__ == "__main__":
    try_insert(row)
    try_insert_with_bound_types(row)
