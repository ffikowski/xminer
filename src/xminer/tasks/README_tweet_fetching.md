# Tweet Fetching System

This document describes the tweet fetching workflow and how to ensure no tweets are missed between fetches.

## Overview

The system uses `last_fetch_date` in `parameters.yml` to track the last successful fetch date. This enables gap detection and filling for authors who may have been missed.

## Configuration

In `src/xminer/config/parameters.yml`:

```yaml
fetch_tweets:
  # Last successful fetch date - used by backfill to check for gaps
  # Update this after each successful fetch run
  last_fetch_date: "2026-01-09"
```

## Workflow

### 1. Regular Tweet Fetch

Run the main tweet fetch script:

```bash
python -m xminer.tasks.fetch_tweets
```

Options:
- `--limit N` - Limit to N profiles
- `--dry-run` - Preview without saving to database
- `--author USERNAME` - Fetch only for specific username

### 2. Fill Gaps

After fetching, run the gap-fill command to catch any missed tweets:

```bash
python -m xminer.tasks.backfill_tweets fill-gaps
```

This command:
- Reads `last_fetch_date` from `parameters.yml`
- Finds authors whose latest stored tweet is before that date
- Fetches any newer tweets from the API using `since_id`
- Saves new tweets to the database

### 3. Update last_fetch_date

After a successful fetch, update `last_fetch_date` in `parameters.yml` to today's date.

## Backfill Commands

### Fill Gaps (Recommended)

```bash
# Fill gaps since last_fetch_date (from parameters.yml)
python -m xminer.tasks.backfill_tweets fill-gaps

# Fill gaps for a specific date
python -m xminer.tasks.backfill_tweets fill-gaps --since-date 2026-01-04

# Dry run (see what would be fetched without saving)
python -m xminer.tasks.backfill_tweets fill-gaps --dry-run

# Only process specific author
python -m xminer.tasks.backfill_tweets fill-gaps --author hubertus_heil

# Limit number of authors to process
python -m xminer.tasks.backfill_tweets fill-gaps --limit 10
```

### Historical Backfill

Fetch older tweets (before the oldest stored tweet):

```bash
# Backfill historical tweets for all authors
python -m xminer.tasks.backfill_tweets historical

# Only backfill if oldest tweet is newer than 30 days
python -m xminer.tasks.backfill_tweets historical --min-gap-days 30

# Dry run
python -m xminer.tasks.backfill_tweets historical --dry-run
```

## How Gap Detection Works

1. The system queries all authors and their latest tweet date
2. Filters to authors whose latest tweet is BEFORE `last_fetch_date`
3. For each author, fetches tweets using `since_id` (the ID of their latest stored tweet)
4. New tweets are saved to the database

## Files

| File | Description |
|------|-------------|
| `fetch_tweets.py` | Main script for fetching new tweets (uses dual API client) |
| `backfill_tweets.py` | Backfill script with `fill-gaps` and `historical` commands |
| `fetch_missing_authors.py` | Script to fetch tweets for potentially missing authors |
| `verify_tweet_completeness.py` | Script to verify tweet data completeness |

## API Client

The system uses `DualAPIClient` from `xminer.io.x_api_dual` which supports two backends:

### 1. Official Twitter API (via tweepy)

- **Mode**: `X_API_MODE=official`
- **Authentication**: Bearer token (`X_BEARER_TOKEN`)
- **Rate limits**: Strict Twitter API limits apply
- **Features**: Full Twitter API v2 support

### 2. TwitterAPI.io (Third-party service)

- **Mode**: `X_API_MODE=twitterapiio`
- **Authentication**: API key (`TWITTERAPIIO_API_KEY`)
- **Rate limits**: More generous than official API
- **Endpoint**: `https://api.twitterapi.io`

### API Differences

| Feature | Official Twitter API | TwitterAPI.io |
|---------|---------------------|---------------|
| Authentication | Bearer token | API key header |
| Rate limits | Strict (15-900 req/15min) | More generous |
| Pagination | `pagination_token` | `cursor` |
| Date field | `created_at` (ISO format) | `createdAt` (Twitter format) |
| Metrics | `public_metrics` object | Individual fields (`likeCount`, etc.) |
| Referenced tweets | `referenced_tweets` array | `retweeted_tweet`, `quoted_tweet` objects |

### Field Mapping (TwitterAPI.io → Standard)

The `TwitterAPIIOTweet` wrapper class handles field mapping:

```python
# TwitterAPI.io response fields → Standard fields
createdAt        → created_at      # "Sat Sep 27 09:05:04 +0000 2025" → datetime
conversationId   → conversation_id
inReplyToUserId  → in_reply_to_user_id
possiblySensitive → possibly_sensitive
likeCount        → public_metrics.like_count
replyCount       → public_metrics.reply_count
retweetCount     → public_metrics.retweet_count
quoteCount       → public_metrics.quote_count
bookmarkCount    → public_metrics.bookmark_count
viewCount        → public_metrics.impression_count
```

### Configuration

Set the API mode in your environment or `.env` file:

```bash
# Use TwitterAPI.io (recommended for higher rate limits)
X_API_MODE=twitterapiio
TWITTERAPIIO_API_KEY=your_api_key_here

# Or use Official Twitter API
X_API_MODE=official
X_BEARER_TOKEN=your_bearer_token_here
```

### Pagination Behavior

**Official API**: Uses `since_id` and `pagination_token` natively.

**TwitterAPI.io**:
- Does NOT support `since_id` or `start_time` server-side
- Client-side filtering is applied after fetching
- Pagination stops automatically when tweets older than `start_time` are found
- The `TwitterAPIIOResponse` wrapper handles this transparently

## Database

Tweets are stored in the `tweets` table with upsert behavior (ON CONFLICT DO UPDATE).

Key columns:
- `tweet_id` (PRIMARY KEY)
- `author_id`
- `username`
- `created_at`
- `text`
- `public_metrics` (like_count, reply_count, etc.)
- `retrieved_at`

## Automated Fetching (Cron Job)

### Buffer for Engagement Metrics

The script supports a **buffer period** (default: 24 hours) to allow engagement metrics (likes, impressions, retweets) to settle before fetching. This gives tweets time to accumulate their full engagement counts.

```bash
# Fetch tweets that are at least 24 hours old
python -m xminer.tasks.fetch_tweets --buffer-hours 24

# Fetch tweets that are at least 48 hours old (more accurate metrics)
python -m xminer.tasks.fetch_tweets --buffer-hours 48
```

### Setup on VPS

1. **Copy the cron script** to the VPS:
   ```bash
   scp scripts/fetch_tweets_cron.sh app@145.223.101.94:/home/app/apps/xminer/scripts/
   ```

2. **Make it executable**:
   ```bash
   chmod +x /home/app/apps/xminer/scripts/fetch_tweets_cron.sh
   ```

3. **Add to crontab**:
   ```bash
   crontab -e
   ```

4. **Add one of these schedules**:
   ```bash
   # RECOMMENDED: Weekly on Sunday at 3 AM (with 24h buffer)
   0 3 * * 0 /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1

   # Daily at 2 AM (with 24h buffer)
   0 2 * * * /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1

   # Custom buffer (e.g., 48 hours) - set BUFFER_HOURS environment variable
   0 3 * * 0 BUFFER_HOURS=48 /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1
   ```

### Cron Schedule Reference

| Schedule | Cron Expression | Description |
|----------|-----------------|-------------|
| Weekly (Sunday 3 AM) | `0 3 * * 0` | Recommended for weekly analysis |
| Weekly (Monday 6 AM) | `0 6 * * 1` | Start of week |
| Daily (2 AM) | `0 2 * * *` | For more frequent updates |
| Twice weekly (Sun/Wed) | `0 3 * * 0,3` | Mid-week refresh |

### What the Cron Script Does

1. Activates the virtual environment
2. Runs the main tweet fetch with buffer (`fetch_tweets --buffer-hours 24`)
3. Fills any gaps (`backfill_tweets fill-gaps`)
4. Updates `last_fetch_date` in `parameters.yml` (to buffer date)
5. Logs a summary of total tweets

### Monitoring

View cron logs:
```bash
tail -f /home/app/apps/xminer/logs/cron.log
```

Check cron job is running:
```bash
crontab -l
```

### Manual Run

Test the script manually:
```bash
/home/app/apps/xminer/scripts/fetch_tweets_cron.sh
```

## Deployment Guide

### Step 1: Copy Files to VPS

From your local machine (in the xminer project directory):

```bash
# Copy the cron script
scp scripts/fetch_tweets_cron.sh app@145.223.101.94:/home/app/apps/xminer/scripts/

# Copy the updated fetch_tweets.py (with --buffer-hours support)
scp src/xminer/tasks/fetch_tweets.py app@145.223.101.94:/home/app/apps/xminer/src/xminer/tasks/
```

### Step 2: SSH into VPS

```bash
ssh app@145.223.101.94
```

### Step 3: Make Script Executable

```bash
chmod +x /home/app/apps/xminer/scripts/fetch_tweets_cron.sh
```

### Step 4: Test the Script

```bash
# First, test with a dry-run on 1 profile
cd /home/app/apps/xminer
source .venv/bin/activate
python -m xminer.tasks.fetch_tweets --buffer-hours 24 --limit 1 --dry-run

# If that works, run the full cron script
/home/app/apps/xminer/scripts/fetch_tweets_cron.sh
```

### Step 5: Add to Crontab

```bash
crontab -e
```

Add the weekly schedule (Sunday 3 AM with 24h buffer):
```
0 3 * * 0 /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1
```

### Step 6: Verify Setup

```bash
# Check cron is configured
crontab -l

# Watch logs (after next run)
tail -f /home/app/apps/xminer/logs/cron.log
```

### Quick One-Liner Deployment

Run this from your local machine to deploy and set up everything:

```bash
ssh app@145.223.101.94 << 'EOF'
cd /home/app/apps/xminer
chmod +x scripts/fetch_tweets_cron.sh
mkdir -p logs

# Test the script
echo "Testing script..."
./scripts/fetch_tweets_cron.sh

# Add to crontab (weekly Sunday 3 AM)
(crontab -l 2>/dev/null | grep -v "fetch_tweets_cron"; echo "0 3 * * 0 /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1") | crontab -

echo "Cron job installed:"
crontab -l
EOF
```

### Customizing the Buffer

To use a different buffer (e.g., 48 hours for more accurate metrics):

```bash
# Option 1: Set in crontab
0 3 * * 0 BUFFER_HOURS=48 /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1

# Option 2: Edit the script default
# Change BUFFER_HOURS=${BUFFER_HOURS:-24} to BUFFER_HOURS=${BUFFER_HOURS:-48}
```

## Troubleshooting

### No tweets found for an author
- The author may genuinely have no new tweets
- Check if the author's account is still active
- Verify the author_id is correct in x_profiles

### JSON serialization errors
- Ensure `to_json_obj()` in `global_helpers.py` returns plain dict/list (not `psycopg2.extras.Json` wrapper)
- SQLAlchemy's JSON type handles serialization automatically

### Rate limiting
- The TwitterAPI.io client handles pagination automatically
- Rate limits are handled with exponential backoff

## Trends Visualization (Daily Job)

Generate visualizations showing how political parties engage with trending topics.

### Manual Run

```bash
# Generate visualizations for top 5 trends
python -m xminer.tasks.generate_trends_viz

# Limit to top 3 trends
python -m xminer.tasks.generate_trends_viz --limit 3

# Analyze a specific trend
python -m xminer.tasks.generate_trends_viz --trend "#Schnee"

# Dry run (preview what would be generated)
python -m xminer.tasks.generate_trends_viz --dry-run
```

### Output

Visualizations are saved to `outputs/{YYYYMM}/graphics/trends/`:
- `{trend_name}_tweets_de.png` / `{trend_name}_tweets_en.png` - Tweet count by party
- `{trend_name}_impressions_de.png` / `{trend_name}_impressions_en.png` - Impressions by party
- `trends_overview_de.png` / `trends_overview_en.png` - Multi-trend comparison

### Daily Cron Job Setup

1. **Copy the cron script** to the VPS:
   ```bash
   scp scripts/generate_trends_viz_cron.sh app@145.223.101.94:/home/app/apps/xminer/scripts/
   ```

2. **Make it executable**:
   ```bash
   chmod +x /home/app/apps/xminer/scripts/generate_trends_viz_cron.sh
   ```

3. **Add to crontab** (daily at 8 AM):
   ```bash
   crontab -e
   ```

   Add:
   ```
   0 8 * * * /home/app/apps/xminer/scripts/generate_trends_viz_cron.sh >> /home/app/apps/xminer/logs/trends_viz_cron.log 2>&1
   ```

### Cron Script Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TRENDS_LIMIT` | 5 | Number of top trends to analyze |

Example with custom limit:
```bash
0 8 * * * TRENDS_LIMIT=10 /home/app/apps/xminer/scripts/generate_trends_viz_cron.sh >> /home/app/apps/xminer/logs/trends_viz_cron.log 2>&1
```

### What the Cron Script Does

1. Fetches current trends from TwitterAPI.io
2. Saves trends to `x_trends` table
3. Generates bilingual visualizations (DE + EN) for top N trends
4. Logs summary of generated files

### Monitoring

View cron logs:
```bash
tail -f /home/app/apps/xminer/logs/trends_viz_cron.log
```
