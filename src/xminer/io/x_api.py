from __future__ import annotations
import tweepy
from ..config.config import Config   # import your Config class

client = tweepy.Client(
    bearer_token=Config.X_BEARER_TOKEN,
    wait_on_rate_limit=True  # or False, but True is safer
)
