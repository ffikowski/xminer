# src/xminer/params.py
import os, yaml
from pathlib import Path
from datetime import datetime, timezone

def _load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

# choose parameters file; allow ENV-specific override
_loaded = {}

HERE = Path(__file__).resolve().parent
PARAMS_FILE = HERE / "parameters.yml"

if os.path.exists(PARAMS_FILE):
    _loaded = _load_yaml(PARAMS_FILE)
if not _loaded:
    raise RuntimeError(f"No parameters file found. Looked for: parameters.yml")

class Params:
    logging_file = _loaded.get("file", "app.log")
    logging_level = _loaded.get("level", "INFO")
    sample_limit = int(_loaded.get("sample_limit", 50))
    chunk_size = int(_loaded.get("chunk_size", 100))
    load_to_db = bool(_loaded.get("load_to_db", False))
    store_csv    = bool(_loaded.get("store_csv", False))
    # NEW:
    tweets_sample_limit = int(_loaded.get("tweets_sample_limit", _loaded.get("sample_limit", -1)))
    sample_seed = _loaded.get("sample_seed", None)
    tweets_since = _loaded.get("tweets_since", None)

    id_cols = _loaded.get("id_cols", [
        "tweet_id","author_id","conversation_id","in_reply_to_user_id"
    ])

    count_cols = _loaded.get("count_cols", [
        "like_count","reply_count","retweet_count",
        "quote_count","bookmark_count","impression_count"
    ])

    tweet_fields = _loaded.get("tweet_fields", [
        "created_at","lang","public_metrics","conversation_id","in_reply_to_user_id",
        "possibly_sensitive","source","entities","referenced_tweets",
    ])
    rate_limit_fallback_sleep = int(_loaded.get("rate_limit_fallback_sleep", 901))

    skip_fetch_date = datetime.fromisoformat(_loaded.get("skip_fetch_date")).replace(tzinfo=timezone.utc)

    # New metrics parameters (top-level keys in parameters.yml)
    year   = int(_loaded.get("year", 2025))
    month  = int(_loaded.get("month", 9))
    outdir = _loaded.get("outdir", "output")
    top_n  = int(_loaded.get("top_n", 10))

    # X Trends params
    trends_woeid = int(_loaded.get("trends_woeid", 23424829))
    trends_place_name = _loaded.get("trends_place_name", "Germany")

    # --- export settings ---
    EXPORT_SSH_HOST = _loaded.get("ssh_host")
    EXPORT_SSH_USER = _loaded.get("ssh_user")
    EXPORT_SSH_PORT = int(_loaded.get("ssh_port", 22))
    EXPORT_SSH_IDENTITY_FILE = _loaded.get("ssh_identity_file")

    EXPORT_REMOTE_BASE_DIR = _loaded.get("remote_base_dir")
    EXPORT_PATTERNS = _loaded.get("export_patterns", [])
    EXPORT_LOCAL_DEST_DIR = _loaded.get("local_dest_dir")

