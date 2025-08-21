# src/xminer/config.py
import os
from dotenv import load_dotenv

# load .env next to repo root when process starts
load_dotenv()

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
    ENV = os.getenv("ENV", "dev")

    @staticmethod
    def validate():
        missing = [k for k in ("DATABASE_URL", "X_BEARER_TOKEN") if not getattr(Config, k)]
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

Config.validate()
