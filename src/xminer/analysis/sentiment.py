# src/xminer/analysis/sentiment.py
"""
Sentiment analysis module using Google Gemini API.
Analyzes tweets for sentiment on various topics:
- Political parties (SPD, CDU, AfD, Grüne, FDP, Linke, BSW)
- Policy areas (economy, immigration, climate, security, etc.)
- Politicians (Merz, Scholz, Habeck, Weidel, etc.)
- Custom topics
"""
import os
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
from dataclasses import dataclass

import google.generativeai as genai
from sqlalchemy import text

from ..io.db import engine
from ..config.config import Config

logger = logging.getLogger(__name__)

# Configure Gemini
GEMINI_API_KEY = os.getenv("Gemini_API_Key")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


@dataclass
class SentimentResult:
    """Result of sentiment analysis for a single topic"""
    topic: str
    topic_type: str  # 'party', 'policy', 'politician', 'custom'
    sentiment: str   # 'positive', 'negative', 'neutral', 'mixed'
    score: float     # -1.0 to 1.0
    confidence: float  # 0.0 to 1.0
    reasoning: str


# Predefined topics (defaults - can be overridden)
# Only parties currently in the Bundestag (21. Wahlperiode)
DEFAULT_POLITICAL_PARTIES = [
    "SPD", "CDU", "CSU", "CDU/CSU", "AfD", "Grüne", "Linke"
]

DEFAULT_POLICY_AREAS = [
    "Wirtschaft", "Migration", "Klima", "Sicherheit", "Bildung",
    "Gesundheit", "Rente", "Außenpolitik", "Verteidigung", "Digitalisierung"
]

DEFAULT_POLITICIANS = [
    "Friedrich Merz", "Olaf Scholz", "Robert Habeck", "Christian Lindner",
    "Alice Weidel", "Sahra Wagenknecht", "Markus Söder", "Annalena Baerbock"
]

# Aliases for backwards compatibility
POLITICAL_PARTIES = DEFAULT_POLITICAL_PARTIES
POLICY_AREAS = DEFAULT_POLICY_AREAS
POLITICIANS = DEFAULT_POLITICIANS


def get_gemini_model(model_name: str = "gemini-3-flash-preview"):
    """Get Gemini model instance"""
    return genai.GenerativeModel(model_name)


def analyze_tweet_sentiment(
    tweet_text: str,
    topics: Optional[List[str]] = None,
    topic_type: str = "auto",
    model_name: str = "gemini-3-flash-preview",
    custom_topic_type: str = "custom"
) -> List[SentimentResult]:
    """
    Analyze sentiment of a tweet for specified topics.

    Args:
        tweet_text: The tweet text to analyze
        topics: List of topics to analyze sentiment for. If None, use defaults based on topic_type.
                Can be any list of strings (trends, hashtags, user-defined topics, etc.)
        topic_type: 'party', 'policy', 'politician', 'custom', or 'auto'
                   When topics is provided with topic_type='custom', uses custom_topic_type for labeling.
        model_name: Gemini model to use
        custom_topic_type: The topic_type label to use when analyzing custom topics (default: 'custom')
                          Examples: 'trend', 'hashtag', 'event', 'person', etc.

    Returns:
        List of SentimentResult objects

    Examples:
        # Analyze for default parties
        analyze_tweet_sentiment(text, topic_type='party')

        # Analyze for custom topics (e.g., from trends)
        analyze_tweet_sentiment(text, topics=['#BTW25', 'Koalition', 'Neuwahlen'],
                               topic_type='custom', custom_topic_type='trend')

        # Analyze for specific politicians
        analyze_tweet_sentiment(text, topics=['Merz', 'Scholz'], topic_type='politician')
    """
    model = get_gemini_model(model_name)

    # Build topic list based on type
    if topics is None:
        if topic_type == "party":
            topics = DEFAULT_POLITICAL_PARTIES
        elif topic_type == "policy":
            topics = DEFAULT_POLICY_AREAS
        elif topic_type == "politician":
            topics = DEFAULT_POLITICIANS
        elif topic_type == "auto":
            topics = DEFAULT_POLITICAL_PARTIES + DEFAULT_POLICY_AREAS + DEFAULT_POLITICIANS
        elif topic_type == "custom":
            raise ValueError("topics must be provided when topic_type='custom'")
        else:
            raise ValueError(f"Unknown topic_type: {topic_type}")

    # Determine valid topic types for the prompt
    if topic_type == "custom":
        valid_topic_types = custom_topic_type
    elif topic_type == "auto":
        valid_topic_types = "party|policy|politician"
    else:
        valid_topic_types = topic_type

    prompt = f"""Analyze the sentiment of this German political tweet towards the specified topics.

Tweet: "{tweet_text}"

Topics to analyze: {', '.join(topics)}

For EACH topic that is mentioned or relevant to this tweet, provide:
1. topic: The topic name
2. topic_type: Use '{valid_topic_types}' as the type
3. sentiment: 'positive', 'negative', 'neutral', or 'mixed'
4. score: A number from -1.0 (very negative) to 1.0 (very positive)
5. confidence: A number from 0.0 to 1.0 indicating how confident you are
6. reasoning: Brief explanation (1-2 sentences in German)

Only include topics that are actually mentioned or clearly relevant to the tweet.
If a topic is not mentioned or relevant, do not include it.

Respond ONLY with valid JSON in this format:
{{
  "results": [
    {{
      "topic": "topic name",
      "topic_type": "{valid_topic_types}",
      "sentiment": "positive|negative|neutral|mixed",
      "score": 0.0,
      "confidence": 0.0,
      "reasoning": "explanation"
    }}
  ]
}}

If no topics are relevant, return: {{"results": []}}
"""

    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()

        # Clean up response (remove markdown code blocks if present)
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
        response_text = response_text.strip()

        data = json.loads(response_text)

        results = []
        for item in data.get("results", []):
            results.append(SentimentResult(
                topic=item["topic"],
                topic_type=item["topic_type"],
                sentiment=item["sentiment"],
                score=float(item["score"]),
                confidence=float(item["confidence"]),
                reasoning=item["reasoning"]
            ))

        return results

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Gemini response: {e}")
        logger.error(f"Response was: {response_text[:500]}")
        return []
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {e}")
        return []


def analyze_tweets_batch(
    tweets: List[Dict[str, Any]],
    topics: Optional[List[str]] = None,
    topic_type: str = "auto",
    model_name: str = "gemini-3-flash-preview"
) -> Dict[str, List[SentimentResult]]:
    """
    Analyze sentiment for multiple tweets.

    Args:
        tweets: List of tweet dicts with 'tweet_id' and 'text' keys
        topics: Topics to analyze
        topic_type: Type of topics
        model_name: Gemini model

    Returns:
        Dict mapping tweet_id to list of SentimentResult
    """
    results = {}

    for i, tweet in enumerate(tweets):
        tweet_id = tweet.get("tweet_id")
        text = tweet.get("text", "")

        if not text:
            continue

        logger.info(f"Analyzing tweet {i+1}/{len(tweets)}: {tweet_id}")

        sentiment_results = analyze_tweet_sentiment(
            text, topics=topics, topic_type=topic_type, model_name=model_name
        )

        results[tweet_id] = sentiment_results

    return results


def save_sentiment_results(
    tweet_id: str,
    results: List[SentimentResult],
    author_id: Optional[int] = None
) -> int:
    """
    Save sentiment analysis results to database.

    Returns number of rows inserted.
    """
    if not results:
        return 0

    insert_stmt = text("""
        INSERT INTO tweet_sentiments (
            tweet_id, author_id, topic, topic_type,
            sentiment, score, confidence, reasoning, analyzed_at
        ) VALUES (
            :tweet_id, :author_id, :topic, :topic_type,
            :sentiment, :score, :confidence, :reasoning, :analyzed_at
        )
        ON CONFLICT (tweet_id, topic) DO UPDATE SET
            sentiment = EXCLUDED.sentiment,
            score = EXCLUDED.score,
            confidence = EXCLUDED.confidence,
            reasoning = EXCLUDED.reasoning,
            analyzed_at = EXCLUDED.analyzed_at
    """)

    rows = []
    for r in results:
        rows.append({
            "tweet_id": tweet_id,
            "author_id": author_id,
            "topic": r.topic,
            "topic_type": r.topic_type,
            "sentiment": r.sentiment,
            "score": r.score,
            "confidence": r.confidence,
            "reasoning": r.reasoning,
            "analyzed_at": datetime.now(timezone.utc)
        })

    with engine.begin() as conn:
        conn.execute(insert_stmt, rows)

    return len(rows)


def get_tweets_for_analysis(
    limit: int = 100,
    since_date: Optional[datetime] = None,
    exclude_analyzed: bool = True
) -> List[Dict]:
    """
    Get tweets from database for sentiment analysis.

    Args:
        limit: Maximum number of tweets to return
        since_date: Only get tweets after this date
        exclude_analyzed: Exclude tweets already analyzed

    Returns:
        List of tweet dicts
    """
    conditions = ["1=1"]
    params = {"limit": limit}

    if since_date:
        conditions.append("t.created_at >= :since_date")
        params["since_date"] = since_date

    if exclude_analyzed:
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM tweet_sentiments ts
                WHERE ts.tweet_id = t.tweet_id
            )
        """)

    sql = text(f"""
        SELECT t.tweet_id, t.author_id, t.username, t.text, t.created_at
        FROM tweets t
        WHERE {' AND '.join(conditions)}
        ORDER BY t.created_at DESC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        {
            "tweet_id": r[0],
            "author_id": r[1],
            "username": r[2],
            "text": r[3],
            "created_at": r[4]
        }
        for r in rows
    ]
