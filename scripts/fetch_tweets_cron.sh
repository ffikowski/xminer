#!/bin/bash
# fetch_tweets_cron.sh
# Automated tweet fetching script for cron job
#
# Usage: Add to crontab with:
#   0 */6 * * * /home/app/apps/xminer/scripts/fetch_tweets_cron.sh >> /home/app/apps/xminer/logs/cron.log 2>&1

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
PARAMS_FILE="$PROJECT_DIR/src/xminer/config/parameters.yml"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Timestamp for logging
timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

echo "========================================"
echo "$(timestamp) Starting tweet fetch job"
echo "========================================"

# Activate virtual environment
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo "$(timestamp) ERROR: Virtual environment not found at $VENV_DIR"
    exit 1
fi

# Change to project directory
cd "$PROJECT_DIR"

# Load environment variables if .env exists
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Step 1: Run the main tweet fetch
echo "$(timestamp) Step 1: Fetching new tweets..."
python -m xminer.tasks.fetch_tweets_jan2026_test 2>&1 || {
    echo "$(timestamp) WARNING: Tweet fetch encountered errors (continuing with gap fill)"
}

# Step 2: Fill any gaps
echo "$(timestamp) Step 2: Filling gaps since last fetch..."
python -m xminer.tasks.backfill_tweets fill-gaps 2>&1

# Step 3: Update last_fetch_date in parameters.yml
TODAY=$(date "+%Y-%m-%d")
echo "$(timestamp) Step 3: Updating last_fetch_date to $TODAY..."

# Use sed to update the date (works on both Linux and macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/last_fetch_date: \"[0-9-]*\"/last_fetch_date: \"$TODAY\"/" "$PARAMS_FILE"
else
    # Linux
    sed -i "s/last_fetch_date: \"[0-9-]*\"/last_fetch_date: \"$TODAY\"/" "$PARAMS_FILE"
fi

# Step 4: Log summary
echo "$(timestamp) Fetching database stats..."
python -c "
from xminer.io.db import engine
from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM tweets'))
    total = result.scalar()

    result = conn.execute(text('''
        SELECT COUNT(*) FROM tweets
        WHERE DATE(created_at) = CURRENT_DATE
    '''))
    today = result.scalar()

print(f'Total tweets: {total}')
print(f'Tweets from today: {today}')
"

echo "========================================"
echo "$(timestamp) Tweet fetch job completed"
echo "========================================"
