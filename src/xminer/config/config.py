# src/xminer/config.py
import os
from dotenv import load_dotenv
from .params import Params

# load .env next to repo root when process starts
load_dotenv()

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Official Twitter API
    X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")

    # TwitterAPI.io credentials (check both uppercase and legacy lowercase names)
    TWITTERAPIIO_API_KEY = os.getenv("TWITTERAPIIO_API_KEY") or os.getenv("twitterapiio_API_KEY")
    TWITTERAPIIO_USER_ID = os.getenv("TWITTERAPIIO_USER_ID") or os.getenv("twitterapiio_User_ID")

    # API mode: "official" or "twitterapiio"
    X_API_MODE = Params.x_api_mode

    ENV = os.getenv("ENV", "dev")

    @staticmethod
    def validate():
        # Always require DATABASE_URL
        if not Config.DATABASE_URL:
            raise RuntimeError("Missing required env var: DATABASE_URL")

        # Validate based on API mode
        if Config.X_API_MODE == "twitterapiio":
            if not Config.TWITTERAPIIO_API_KEY:
                raise RuntimeError("twitterapiio_API_KEY required when X_API_MODE=twitterapiio")
        else:
            if not Config.X_BEARER_TOKEN:
                raise RuntimeError("X_BEARER_TOKEN required when X_API_MODE=official")

Config.validate()
