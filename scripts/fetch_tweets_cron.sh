#!/bin/bash
# fetch_tweets_cron.sh
# Automated tweet fetching script for cron job
#
# Usage: Add to crontab with:
#   Weekly (Sunday at 3 AM): 0 3 * * 0 /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1
#   Daily at 2 AM:           0 2 * * * /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1
#
# Configuration:
#   BUFFER_HOURS: Hours to wait before fetching tweets (for engagement metrics to settle)
#                 Default: 24 hours (tweets must be at least 24h old to be fetched)

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
PARAMS_FILE="$PROJECT_DIR/src/xminer/config/parameters.yml"

# Buffer hours - fetch tweets that are at least this many hours old
# This allows engagement metrics (likes, impressions, etc.) to settle
BUFFER_HOURS=${BUFFER_HOURS:-24}

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Timestamp for logging
timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

echo "========================================"
echo "$(timestamp) Starting tweet fetch job"
echo "========================================"
echo "$(timestamp) Buffer: $BUFFER_HOURS hours (tweets older than this will be fetched)"

# Change to project directory
cd "$PROJECT_DIR"

# Load environment variables if .env exists
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

# Check if uv is available (prefer uv run)
if command -v uv &> /dev/null; then
    RUN_CMD="uv run python"
else
    # Fallback to venv activation
    VENV_DIR="$PROJECT_DIR/.venv"
    if [ -f "$VENV_DIR/bin/activate" ]; then
        source "$VENV_DIR/bin/activate"
        RUN_CMD="python"
    else
        echo "$(timestamp) ERROR: Neither uv nor virtual environment found"
        exit 1
    fi
fi

echo "$(timestamp) Using: $RUN_CMD"

# Step 1: Run the main tweet fetch with buffer
echo "$(timestamp) Step 1: Fetching tweets (with ${BUFFER_HOURS}h buffer)..."
$RUN_CMD -m xminer.tasks.fetch_tweets --buffer-hours $BUFFER_HOURS 2>&1 || {
    echo "$(timestamp) WARNING: Tweet fetch encountered errors (continuing with gap fill)"
}

# Step 2: Fill any gaps (also with buffer)
echo "$(timestamp) Step 2: Filling gaps since last fetch..."
$RUN_CMD -m xminer.tasks.backfill_tweets fill-gaps 2>&1 || {
    echo "$(timestamp) WARNING: Gap fill encountered errors"
}

# Step 3: Update last_fetch_date in parameters.yml
# Use yesterday's date since we're fetching with a buffer
FETCH_DATE=$(date -d "-${BUFFER_HOURS} hours" "+%Y-%m-%d" 2>/dev/null || date -v-${BUFFER_HOURS}H "+%Y-%m-%d")
echo "$(timestamp) Step 3: Updating last_fetch_date to $FETCH_DATE..."

# Use sed to update the date (works on both Linux and macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/last_fetch_date: \"[0-9-]*\"/last_fetch_date: \"$FETCH_DATE\"/" "$PARAMS_FILE"
else
    # Linux
    sed -i "s/last_fetch_date: \"[0-9-]*\"/last_fetch_date: \"$FETCH_DATE\"/" "$PARAMS_FILE"
fi

# Step 4: Log summary
echo "$(timestamp) Fetching database stats..."
$RUN_CMD -c "
from xminer.io.db import engine
from sqlalchemy import text
from datetime import datetime, timedelta

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM tweets'))
    total = result.scalar()

    result = conn.execute(text('''
        SELECT COUNT(*) FROM tweets
        WHERE created_at >= NOW() - INTERVAL '7 days'
    '''))
    last_week = result.scalar()

    result = conn.execute(text('''
        SELECT MIN(created_at), MAX(created_at) FROM tweets
    '''))
    row = result.fetchone()
    min_date, max_date = row[0], row[1]

print(f'Total tweets: {total:,}')
print(f'Tweets from last 7 days: {last_week:,}')
print(f'Date range: {min_date} to {max_date}')
"

echo "========================================"
echo "$(timestamp) Tweet fetch job completed"
echo "========================================"
