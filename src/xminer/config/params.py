# src/xminer/config/params.py
import os, yaml
from pathlib import Path
from datetime import datetime, timezone

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

HERE = Path(__file__).resolve().parent
PARAMS_FILE = HERE / "parameters.yml"
if not PARAMS_FILE.exists():
    raise RuntimeError("No parameters file found. Looked for: parameters.yml")

_loaded = _load_yaml(PARAMS_FILE)

def _dig(d: dict, dotted: str):
    """Traverse dict by dotted path; return (found, value)."""
    cur = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur

def _get(*candidates, default=None):
    """
    Return the first existing key among dotted-path candidates.
    Examples: _get('fetch_tweets.tweets_since','tweets_since', default=None)
    """
    for key in candidates:
        ok, val = _dig(_loaded, key)
        if ok:
            return val
    return default

def _get_int(*candidates, default=0):
    v = _get(*candidates, default=default)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _get_bool(*candidates, default=False):
    v = _get(*candidates, default=default)
    return bool(v)

def _get_list(*candidates, default=None):
    v = _get(*candidates, default=default if default is not None else [])
    return list(v) if isinstance(v, (list, tuple)) else (default or [])

def _get_dt_utc(*candidates, default=None):
    s = _get(*candidates, default=None)
    if not s:
        return default
    try:
        # Allow 'Z' suffix
        s = str(s).replace("Z", "+00:00")
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc) if "Z" in s or "+" in s else datetime.fromisoformat(s)
    except Exception:
        return default

class Params:
    # ----- logging -----
    logging_file  = _get("common.logging.file", "file", default="app.log")
    logging_level = _get("common.logging.level", "level", default="INFO")

    # ----- common date/output -----
    year   = _get_int("common.year", "year", default=2025)
    month  = _get_int("common.month", "month", default=9)
    outdir = _get("common.outdir", "outdir", default="output")
    top_n  = _get_int("common.top_n", "top_n", default=10)

    # ----- fetch_x_profiles -----
    sample_limit = _get_int("fetch_x_profiles.sample_limit", "sample_limit", default=50)
    chunk_size   = _get_int("fetch_x_profiles.chunk_size", "chunk_size", default=100)
    load_to_db   = _get_bool("fetch_x_profiles.load_to_db", "load_to_db", default=False)
    store_csv    = _get_bool("fetch_x_profiles.store_csv", "store_csv", default=False)

    # ----- fetch_tweets -----
    tweets_sample_limit = _get_int("fetch_tweets.tweets_sample_limit", "tweets_sample_limit", "sample_limit", default=-1)
    sample_seed         = _get("fetch_tweets.sample_seed", "sample_seed", default=None)
    tweets_since        = _get("fetch_tweets.tweets_since", "tweets_since", default=None)
    tweet_fields        = _get_list("fetch_tweets.tweet_fields", "tweet_fields",
                                    default=["created_at","lang","public_metrics","conversation_id",
                                             "in_reply_to_user_id","possibly_sensitive","source",
                                             "entities","referenced_tweets"])
    rate_limit_fallback_sleep = _get_int("fetch_tweets.rate_limit_fallback_sleep", "rate_limit_fallback_sleep", default=901)
    skip_fetch_date     = _get_dt_utc("fetch_tweets.skip_fetch_date", "skip_fetch_date", default=None)
    last_fetch_date     = _get("fetch_tweets.last_fetch_date", "last_fetch_date", default=None)

    # ----- trends -----
    trends_woeid      = _get_int("fetch_x_trends.trends_woeid", "trends_woeid", default=23424829)
    trends_place_name = _get("fetch_x_trends.trends_place_name", "trends_place_name", default="Germany")

    # ----- bundestag votes -----
    bundestag_votes_excel_dir = _get("fetch_bundestag_votes.excel_dir", "excel_dir", default="data/bundestag_votes")
    bundestag_votes_load_to_db = _get_bool("fetch_bundestag_votes.load_to_db", default=True)
    bundestag_votes_store_csv = _get_bool("fetch_bundestag_votes.store_csv", default=False)

    # ----- export outputs -----
    EXPORT_SSH_HOST          = _get("export_outputs.ssh_host", "ssh_host", default=None)
    EXPORT_SSH_USER          = _get("export_outputs.ssh_user", "ssh_user", default=None)
    EXPORT_SSH_PORT          = _get_int("export_outputs.ssh_port", "ssh_port", default=22)
    EXPORT_SSH_IDENTITY_FILE = _get("export_outputs.ssh_identity_file", "ssh_identity_file", default=None)
    EXPORT_REMOTE_BASE_DIR   = _get("export_outputs.remote_base_dir", "remote_base_dir", default=None)
    EXPORT_PATTERNS          = _get_list("export_outputs.export_patterns", "export_patterns", default=[])
    EXPORT_LOCAL_DEST_DIR    = _get("export_outputs.local_dest_dir", "local_dest_dir", default=None)
    EXPORT_GRAPHICS_BASE_DIR = _get("export_outputs.graphics_base_dir", "graphics_base_dir", default=None)
