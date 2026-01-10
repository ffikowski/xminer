#!/bin/bash
# generate_trends_viz_cron.sh
# Automated trends visualization script for daily cron job
#
# Usage: Add to crontab with:
#   Daily at 8 AM: 0 8 * * * /home/app/apps/xminer/scripts/generate_trends_viz_cron.sh >> /home/app/apps/xminer/logs/trends_viz_cron.log 2>&1
#
# Configuration:
#   TRENDS_LIMIT: Number of top trends to analyze (default: 5)

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"

# Number of trends to analyze
TRENDS_LIMIT=${TRENDS_LIMIT:-5}

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Timestamp for logging
timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

echo "========================================"
echo "$(timestamp) Starting trends visualization job"
echo "========================================"
echo "$(timestamp) Analyzing top $TRENDS_LIMIT trends"

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

# Step 1: Fetch current trends and save to database (optional)
# Skip if SKIP_FETCH is set
if [ -z "$SKIP_FETCH" ]; then
    echo "$(timestamp) Step 1: Fetching current trends..."
    $RUN_CMD -m xminer.tasks.fetch_x_trends 2>&1 || {
        echo "$(timestamp) WARNING: Trend fetch encountered errors (continuing with visualization)"
    }
else
    echo "$(timestamp) Step 1: Skipping trend fetch (SKIP_FETCH is set)"
fi

# Step 2: Generate visualizations for top trends
echo "$(timestamp) Step 2: Generating visualizations for top $TRENDS_LIMIT trends..."
$RUN_CMD -m xminer.tasks.generate_trends_viz --limit $TRENDS_LIMIT 2>&1 || {
    echo "$(timestamp) WARNING: Visualization generation encountered errors"
}

# Step 3: Log summary
echo "$(timestamp) Step 3: Listing generated files..."
OUTPUT_DIR="$PROJECT_DIR/outputs/$(date +%Y%m)/graphics/trends"
if [ -d "$OUTPUT_DIR" ]; then
    echo "$(timestamp) Files in $OUTPUT_DIR:"
    ls -la "$OUTPUT_DIR" 2>/dev/null | tail -20
    FILE_COUNT=$(ls -1 "$OUTPUT_DIR"/*.png 2>/dev/null | wc -l)
    echo "$(timestamp) Total PNG files: $FILE_COUNT"
else
    echo "$(timestamp) Output directory not found: $OUTPUT_DIR"
fi

echo "========================================"
echo "$(timestamp) Trends visualization job completed"
echo "========================================"
