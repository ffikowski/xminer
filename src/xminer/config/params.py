# src/xminer/params.py
import os, yaml
from pathlib import Path

def _load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

# choose parameters file; allow ENV-specific override
_loaded = {}

HERE = Path(__file__).resolve().parent
PARAMS_FILE = HERE / "parameters.yml"

if os.path.exists(p):
    _loaded = _load_yaml(p)
if not _loaded:
    raise RuntimeError(f"No parameters file found. Looked for: parameters.yml")

class Params:
    logging_file = _loaded.get("file", "app.log")
    logging_level = _loaded.get("level", "INFO")
    sample_limit = int(_loaded.get("sample_limit", 50))
    chunk_size = int(_loaded.get("chunk_size", 100))
