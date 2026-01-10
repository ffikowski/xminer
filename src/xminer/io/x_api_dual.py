# src/xminer/io/x_api_dual.py
"""
Dual Twitter API client supporting both:
1. Official Twitter API (via tweepy)
2. TwitterAPI.io (third-party service)
"""
from __future__ import annotations
import requests
import tweepy
from typing import Optional, List, Dict, Any
from datetime import datetime
from ..config.config import Config

class TwitterAPIIOClient:
    """Client for twitterapi.io service"""

    BASE_URL = "https://api.twitterapi.io"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-API-Key": api_key}

    def get_users_tweets(
        self,
        id: int,
        max_results: int = 100,
        since_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        tweet_fields: Optional[List[str]] = None,
        pagination_token: Optional[str] = None,  # Added for tweepy Paginator
        **kwargs  # Catch any other kwargs
    ):
        """Fetch tweets for a user - compatible with tweepy interface"""
        url = f"{self.BASE_URL}/twitter/user/last_tweets"

        params = {
            "userId": str(id),
            "includeReplies": True  # Include all tweets
        }

        # Note: TwitterAPI.io uses cursor-based pagination, not since_id/start_time
        # We'll fetch what we can and filter client-side if needed
        if pagination_token:
            params["cursor"] = pagination_token

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()

        return TwitterAPIIOResponse(response.json(), since_id=since_id, start_time=start_time)

    def get_trends(self, woeid: int, count: int = 30) -> List[Dict[str, Any]]:
        """
        Fetch trends by WOEID using TwitterAPI.io.

        Args:
            woeid: The WOEID of the location (e.g., 23424829 for Germany)
            count: Number of trends to return (min 30)

        Returns:
            List of trend dicts with keys: trend_name, query, rank, meta_description
        """
        url = f"{self.BASE_URL}/twitter/trends"
        params = {"woeid": woeid}
        if count > 30:
            params["count"] = count

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()

        data = response.json()
        if data.get("status") != "success":
            raise Exception(f"Trends API error: {data.get('msg', 'Unknown error')}")

        trends = data.get("trends", [])
        # Normalize to consistent format
        # Response format: {"trends": [{"trend": {"name": "...", "rank": 1, "meta_description": "684K posts"}}]}
        result = []
        for item in trends:
            t = item.get("trend", item)  # Handle nested 'trend' object
            result.append({
                "trend_name": t.get("name"),
                "query": t.get("target", {}).get("query"),
                "rank": t.get("rank"),
                "meta_description": t.get("meta_description"),  # Usually contains tweet count like "684K posts"
            })
        return result

class TwitterAPIIOResponse(tweepy.Response):
    """Wrapper to make twitterapi.io response compatible with tweepy"""

    def __new__(cls, json_data: Dict[str, Any], since_id: Optional[str] = None, start_time: Optional[datetime] = None):
        # TwitterAPI.io returns tweets in data['tweets'] array
        data_obj = json_data.get("data", {})
        tweets = data_obj.get("tweets", []) if isinstance(data_obj, dict) else []

        tweet_list = []
        should_stop_pagination = False

        if tweets:
            # Convert to TwitterAPIIOTweet objects
            all_tweets = [TwitterAPIIOTweet(t) for t in tweets]

            # Check if any tweet is older than start_time - if so, we've gone past our target date
            if start_time and start_time.tzinfo:
                oldest_tweet = min(all_tweets, key=lambda t: t.created_at or datetime.max.replace(tzinfo=start_time.tzinfo))
                if oldest_tweet.created_at and oldest_tweet.created_at < start_time:
                    should_stop_pagination = True

            # Filter by since_id if provided (client-side filtering)
            if since_id:
                all_tweets = [t for t in all_tweets if t.id and int(t.id) > int(since_id)]

            # Filter by start_time if provided (client-side filtering)
            if start_time and start_time.tzinfo:
                all_tweets = [t for t in all_tweets if t.created_at and t.created_at > start_time]

            tweet_list = all_tweets

        # Build meta object with pagination info
        # Stop pagination if we've reached tweets older than start_time OR if no results after filtering
        has_next = json_data.get("has_next_page", False) and not should_stop_pagination
        meta = {
            "next_token": json_data.get("next_cursor") if has_next else None,
            "result_count": len(tweet_list)
        }

        # Create tweepy.Response namedtuple
        return super().__new__(
            cls,
            data=tweet_list,
            includes={},
            errors=[],
            meta=meta
        )

class TwitterAPIIOTweet:
    """Wrapper to make twitterapi.io tweet compatible with tweepy Tweet"""

    def __init__(self, tweet_dict: Dict[str, Any]):
        self._dict = tweet_dict

        # Map TwitterAPI.io fields to match tweepy's Tweet object
        self.id = tweet_dict.get("id")
        self.text = tweet_dict.get("text")
        self.created_at = self._parse_datetime(tweet_dict.get("createdAt"))  # Note: createdAt not created_at
        self.lang = tweet_dict.get("lang")
        self.conversation_id = tweet_dict.get("conversationId")
        self.in_reply_to_user_id = tweet_dict.get("inReplyToUserId")
        self.possibly_sensitive = tweet_dict.get("possiblySensitive")
        self.source = tweet_dict.get("source")
        self.entities = tweet_dict.get("entities")

        # Build referenced_tweets from retweeted_tweet and quoted_tweet
        self.referenced_tweets = self._build_referenced_tweets(tweet_dict)

        # Public metrics - TwitterAPI.io uses individual fields not nested object
        self.public_metrics = {
            "like_count": tweet_dict.get("likeCount", 0),
            "reply_count": tweet_dict.get("replyCount", 0),
            "retweet_count": tweet_dict.get("retweetCount", 0),
            "quote_count": tweet_dict.get("quoteCount", 0),
            "bookmark_count": tweet_dict.get("bookmarkCount", 0),
            "impression_count": tweet_dict.get("viewCount", 0),  # viewCount maps to impression_count
        }

    @staticmethod
    def _build_referenced_tweets(tweet_dict: Dict[str, Any]):
        """Build referenced_tweets array from retweeted_tweet and quoted_tweet"""
        refs = []

        if tweet_dict.get("retweeted_tweet"):
            refs.append({"type": "retweeted", "id": tweet_dict["retweeted_tweet"].get("id")})

        if tweet_dict.get("quoted_tweet"):
            refs.append({"type": "quoted", "id": tweet_dict["quoted_tweet"].get("id")})

        if tweet_dict.get("isReply") and tweet_dict.get("inReplyToId"):
            refs.append({"type": "replied_to", "id": tweet_dict.get("inReplyToId")})

        return refs if refs else None

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str:
            return None
        try:
            # TwitterAPI.io uses Twitter's format: "Sat Sep 27 09:05:04 +0000 2025"
            from datetime import datetime as dt
            return dt.strptime(dt_str, "%a %b %d %H:%M:%S %z %Y")
        except:
            try:
                # Fallback: Try ISO format with Z
                return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            except:
                return None

class DualAPIClient:
    """
    Unified client that can use either official Twitter API or twitterapi.io
    """

    def __init__(self):
        self.mode = Config.X_API_MODE

        if self.mode == "twitterapiio":
            print(f"🔧 Using TwitterAPI.io mode")
            self.client = TwitterAPIIOClient(Config.TWITTERAPIIO_API_KEY)
        else:
            print(f"🔧 Using Official Twitter API mode")
            self.client = tweepy.Client(
                bearer_token=Config.X_BEARER_TOKEN,
                wait_on_rate_limit=True
            )

    def get_users_tweets(self, **kwargs):
        """Get user tweets - works with both APIs"""
        return self.client.get_users_tweets(**kwargs)

    def get_me(self):
        """Get authenticated user - only works with official API"""
        if self.mode == "twitterapiio":
            # twitterapi.io doesn't have a /me endpoint in the same way
            # Return a mock response
            class MockUser:
                class MockData:
                    username = "twitterapiio_user"
                data = MockData()
            return MockUser()
        else:
            return self.client.get_me()

    def get_trends(self, woeid: int, count: int = 30, bearer_token: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get trends by WOEID - works with both APIs.

        Args:
            woeid: The WOEID of the location
            count: Number of trends to return
            bearer_token: Bearer token for official API (ignored for twitterapiio)

        Returns:
            List of trend dicts with keys: trend_name, tweet_count, rank
        """
        if self.mode == "twitterapiio":
            return self.client.get_trends(woeid, count)
        else:
            # Official Twitter API v2 trends endpoint
            import requests
            url = f"https://api.x.com/2/trends/by/woeid/{woeid}"
            token = bearer_token or Config.X_BEARER_TOKEN
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            payload = response.json() or {}
            data = payload.get("data") or []
            # Normalize to same format as twitterapiio
            result = []
            for idx, t in enumerate(data, start=1):
                result.append({
                    "trend_name": t.get("trend_name"),
                    "tweet_count": t.get("tweet_count"),
                    "rank": idx,
                    "meta_description": None,
                })
            return result

# Create the global client instance
client = DualAPIClient()
